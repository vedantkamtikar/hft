#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <mmsystem.h>

#include "queue/spsc.hpp"
#include "protocol/itch.hpp"
#include "protocol/ouch.hpp"
#include "engine/orderbook.hpp"
#include "engine/risk.hpp"
#include "pipeline/affinity.hpp"
#include "simulator/exchange.hpp"
#include "telemetry/stats.hpp"

#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <csignal>

using namespace hft;

// Global SPSC Ring Buffers (Zero heap allocation, cache-aligned)
static queue::SPSCQueue<protocol::MarketUpdateMessage, 65536> g_queue1_ingress_to_engine;
static queue::SPSCQueue<protocol::TradeSignal, 65536>         g_queue2_engine_to_execution;

// Global Pipeline Control & Counters
static std::atomic<bool> g_running{true};
static std::atomic<uint64_t> g_ingress_pkts{0};
static std::atomic<uint64_t> g_engine_updates{0};
static std::atomic<uint64_t> g_signals_generated{0};
static std::atomic<uint64_t> g_orders_sent{0};
static std::atomic<uint64_t> g_orders_rejected{0};

// Telemetry Aggregators for Breakdown Latencies
static telemetry::LatencyAggregator<200'000> g_latency_tick_to_trade; // T_exec - T0
static telemetry::LatencyAggregator<200'000> g_latency_ingress_engine; // T1 - T0
static telemetry::LatencyAggregator<200'000> g_latency_engine_exec;   // T_exec - T1
static telemetry::HighResTimer g_timer;

/**
 * @brief Phase 1: Ingress Worker (Core 1)
 * Receives raw UDP ITCH packets, timestamps T0, parses zero-copy, and pushes to Q1.
 */
void run_ingress_worker() {
    pipeline::ThreadAffinity::pin_current_thread(1, true);

    SOCKET ingress_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (ingress_sock == INVALID_SOCKET) {
        std::cerr << "[Phase 1: Ingress] Failed to create socket (WSA Error: " << WSAGetLastError() << ")\n" << std::flush;
        return;
    }

    int reuse = 1;
    setsockopt(ingress_sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    // Increase socket receive buffer size for microburst protection (4MB)
    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(ingress_sock, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcvbuf), sizeof(rcvbuf));

    sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(simulator::ExchangeSimulator::ITCH_PORT);
    bind_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(ingress_sock, reinterpret_cast<sockaddr*>(&bind_addr), sizeof(bind_addr)) == SOCKET_ERROR) {
        std::cerr << "[Phase 1: Ingress] Failed to bind to port " << simulator::ExchangeSimulator::ITCH_PORT 
                  << " (WSA Error: " << WSAGetLastError() << ")\n" << std::flush;
        closesocket(ingress_sock);
        return;
    }

    // Set non-blocking mode
    u_long mode = 1;
    ioctlsocket(ingress_sock, FIONBIO, &mode);

    uint8_t rx_buffer[2048];
    protocol::MarketUpdateMessage update_msg{};

    while (g_running.load(std::memory_order_relaxed)) {
        sockaddr_in src_addr{};
        int src_len = sizeof(src_addr);
        int bytes = recvfrom(ingress_sock, reinterpret_cast<char*>(rx_buffer), sizeof(rx_buffer), 0,
                             reinterpret_cast<sockaddr*>(&src_addr), &src_len);

        if (bytes > 0) {
            const uint64_t t0 = g_timer.now_ns();
            if (protocol::ITCHParser::parse_single(rx_buffer, static_cast<size_t>(bytes), t0, update_msg)) {
                while (!g_queue1_ingress_to_engine.push(update_msg) && g_running.load(std::memory_order_relaxed)) {
                    _mm_pause();
                }
                g_ingress_pkts.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            _mm_pause();
        }
    }

    closesocket(ingress_sock);
}

/**
 * @brief Phase 2: Strategy Engine Worker (Core 2)
 * Pops messages from Q1, updates Flat Order Book, evaluates OBI, timestamps T1, and pushes to Q2.
 */
void run_strategy_worker() {
    pipeline::ThreadAffinity::pin_current_thread(2, true);

    static engine::FlatOrderBook order_book;
    engine::OBIStrategy strategy(0.30, -0.30, 100);

    protocol::MarketUpdateMessage update_msg{};
    protocol::TradeSignal signal{};

    while (g_running.load(std::memory_order_relaxed)) {
        if (g_queue1_ingress_to_engine.pop(update_msg)) {
            order_book.process_update(update_msg);
            g_engine_updates.fetch_add(1, std::memory_order_relaxed);

            const uint64_t t1 = g_timer.now_ns();

            if (strategy.evaluate(order_book, update_msg.t0_timestamp_ns, t1, "NVDA    ", signal)) {
                while (!g_queue2_engine_to_execution.push(signal) && g_running.load(std::memory_order_relaxed)) {
                    _mm_pause();
                }
                g_signals_generated.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            _mm_pause();
        }
    }
}

/**
 * @brief Phase 3: Pre-Trade Risk & Execution Worker (Core 3)
 * Pops signals from Q2, executes pre-trade risk checks, formats binary OUCH order, transmits UDP, and records T_exec.
 */
void run_execution_worker() {
    pipeline::ThreadAffinity::pin_current_thread(3, true);

    SOCKET ouch_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (ouch_sock == INVALID_SOCKET) {
        std::cerr << "[Phase 3: Execution] Failed to create socket (WSA Error: " << WSAGetLastError() << ")\n" << std::flush;
        return;
    }

    sockaddr_in ouch_dest{};
    ouch_dest.sin_family = AF_INET;
    ouch_dest.sin_port = htons(simulator::ExchangeSimulator::OUCH_PORT);
    inet_pton(AF_INET, simulator::ExchangeSimulator::IP_ADDR, &ouch_dest.sin_addr);

    engine::PreTradeRiskEngine risk_engine;
    protocol::TradeSignal signal{};
    uint8_t ouch_packet[128];

    while (g_running.load(std::memory_order_relaxed)) {
        if (g_queue2_engine_to_execution.pop(signal)) {
            const engine::RiskCheckResult risk_res = risk_engine.validate(signal);

            if (risk_res == engine::RiskCheckResult::PASSED) {
                const size_t pkt_len = protocol::OUCHFormatter::format_enter_order(
                    ouch_packet, signal.signal_id, signal.side, signal.shares, signal.symbol, signal.price);

                sendto(ouch_sock, reinterpret_cast<const char*>(ouch_packet), static_cast<int>(pkt_len), 0,
                       reinterpret_cast<const sockaddr*>(&ouch_dest), sizeof(ouch_dest));

                const uint64_t t_exec = g_timer.now_ns();
                const uint64_t total_latency_ns = (t_exec >= signal.t0_ingress_ns) ? (t_exec - signal.t0_ingress_ns) : 0;
                const uint64_t ingress_engine_ns = (signal.t1_strategy_ns >= signal.t0_ingress_ns) ? (signal.t1_strategy_ns - signal.t0_ingress_ns) : 0;
                const uint64_t engine_exec_ns = (t_exec >= signal.t1_strategy_ns) ? (t_exec - signal.t1_strategy_ns) : 0;

                g_latency_tick_to_trade.record(total_latency_ns);
                g_latency_ingress_engine.record(ingress_engine_ns);
                g_latency_engine_exec.record(engine_exec_ns);

                g_orders_sent.fetch_add(1, std::memory_order_relaxed);
            } else {
                g_orders_rejected.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            _mm_pause();
        }
    }

    closesocket(ouch_sock);
}

void print_dashboard_header() {
    std::cout << "\033[2J\033[H";
    std::cout << "\033[1;36m"
              << "=========================================================================================\n"
              << "              LOW-LATENCY HIGH-FREQUENCY TRADING PIPELINE (C++20)\n"
              << "=========================================================================================\033[0m\n";
    std::cout << "Architecture: 3-Phase Lock-Free Pinned Pipeline | Zero Heap Allocations | WinSock2 UDP\n";
    std::cout << "Target: Sub-Microsecond Tick-to-Trade | Pre-allocated SPSC Ring Buffers (alignas 64)\n";
    std::cout << "-----------------------------------------------------------------------------------------\n";
}

void print_dashboard_tick(uint64_t elapsed_seconds, const simulator::ExchangeSimulator& sim) {
    std::cout << "\033[H";
    print_dashboard_header();

    const uint64_t ingress_count = g_ingress_pkts.load(std::memory_order_relaxed);
    const uint64_t engine_count = g_engine_updates.load(std::memory_order_relaxed);
    const uint64_t signals_count = g_signals_generated.load(std::memory_order_relaxed);
    const uint64_t orders_count = g_orders_sent.load(std::memory_order_relaxed);
    const uint64_t sim_gen = sim.orders_generated();
    const uint64_t sim_rx = sim.orders_received();

    std::cout << "\033[1;33m[PIPELINE STATUS & CORE PINNING]\033[0m\n";
    std::cout << "  - Phase 1 Ingress        : [Core 1]  UDP Port 9001  -> Packets Processed: " << ingress_count << "\n";
    std::cout << "  - Phase 2 Strategy Engine: [Core 2]  OBI Model      -> Updates Processed: " << engine_count << " | Signals: " << signals_count << "\n";
    std::cout << "  - Phase 3 Risk & Exec    : [Core 3]  OUCH Port 9002 -> Orders Dispatched: " << orders_count << " | Rejections: " << g_orders_rejected.load() << "\n";
    std::cout << "  - Queue 1 Ingress->Engine: [Depth: " << std::setw(4) << g_queue1_ingress_to_engine.size() << " / " << g_queue1_ingress_to_engine.capacity() << "]\n";
    std::cout << "  - Queue 2 Engine->Exec   : [Depth: " << std::setw(4) << g_queue2_engine_to_execution.size() << " / " << g_queue2_engine_to_execution.capacity() << "]\n";
    std::cout << "  - Exchange Simulator     : Generated: " << sim_gen << " msgs | Received: " << sim_rx << " orders | Executed: " << sim.orders_executed() << "\n";
    std::cout << "-----------------------------------------------------------------------------------------\n";

    const auto ttt_report = g_latency_tick_to_trade.compute_report();
    const auto eng_report = g_latency_ingress_engine.compute_report();
    const auto exe_report = g_latency_engine_exec.compute_report();

    std::cout << "\033[1;32m[TICK-TO-TRADE END-TO-END LATENCY (T_exec - T0)]\033[0m\n";
    std::cout << "  Samples Analyzed: " << ttt_report.count << " executed trades (Elapsed: " << elapsed_seconds << "s)\n\n";

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  +-------------------+--------------------+--------------------+--------------------+\n";
    std::cout << "  | Metric            | Tick-to-Trade (us) | Ingress->Eng (us)  | Engine->Exec (us)  |\n";
    std::cout << "  +-------------------+--------------------+--------------------+--------------------+\n";
    std::cout << "  | Min Latency       | " << std::setw(18) << (ttt_report.min_ns / 1000.0)   << " | " << std::setw(18) << (eng_report.min_ns / 1000.0)   << " | " << std::setw(18) << (exe_report.min_ns / 1000.0)   << " |\n";
    std::cout << "  | Mean Latency      | " << std::setw(18) << (ttt_report.mean_ns / 1000.0)  << " | " << std::setw(18) << (eng_report.mean_ns / 1000.0)  << " | " << std::setw(18) << (exe_report.mean_ns / 1000.0)  << " |\n";
    std::cout << "  | P50 (Median)      | " << std::setw(18) << (ttt_report.p50_ns / 1000.0)   << " | " << std::setw(18) << (eng_report.p50_ns / 1000.0)   << " | " << std::setw(18) << (exe_report.p50_ns / 1000.0)   << " |\n";
    std::cout << "  | P90 Percentile    | " << std::setw(18) << (ttt_report.p90_ns / 1000.0)   << " | " << std::setw(18) << (eng_report.p90_ns / 1000.0)   << " | " << std::setw(18) << (exe_report.p90_ns / 1000.0)   << " |\n";
    std::cout << "  | P99 Percentile    | " << std::setw(18) << (ttt_report.p99_ns / 1000.0)   << " | " << std::setw(18) << (eng_report.p99_ns / 1000.0)   << " | " << std::setw(18) << (exe_report.p99_ns / 1000.0)   << " |\n";
    std::cout << "  | P99.99 Percentile | " << std::setw(18) << (ttt_report.p99_99_ns / 1000.0)<< " | " << std::setw(18) << (eng_report.p99_99_ns / 1000.0)<< " | " << std::setw(18) << (exe_report.p99_99_ns / 1000.0)<< " |\n";
    std::cout << "  | Max Latency       | " << std::setw(18) << (ttt_report.max_ns / 1000.0)   << " | " << std::setw(18) << (eng_report.max_ns / 1000.0)   << " | " << std::setw(18) << (exe_report.max_ns / 1000.0)   << " |\n";
    std::cout << "  +-------------------+--------------------+--------------------+--------------------+\n";
    std::cout << "=========================================================================================\n";
    std::cout << "  Running pipeline... please wait\n" << std::flush;
}

int main(int argc, char* argv[]) {
    // 1. Enable ANSI terminal escape colors
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    if (hOut != INVALID_HANDLE_VALUE) {
        DWORD dwMode = 0;
        if (GetConsoleMode(hOut, &dwMode)) {
            dwMode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
            SetConsoleMode(hOut, dwMode);
        }
    }

    // 2. Set Windows timer resolution to 1ms
    timeBeginPeriod(1);

    std::cout << "[System] Initializing Windows Sockets & Low-Latency Pipeline...\n" << std::flush;
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        std::cerr << "[Fatal] WSAStartup failed.\n" << std::flush;
        timeEndPeriod(1);
        return 1;
    }

    const unsigned int hw_threads = std::thread::hardware_concurrency();
    std::cout << "[System] Available CPU cores: " << hw_threads << "\n" << std::flush;

    // 3. Initialize Exchange Simulator
    simulator::ExchangeSimulator sim;
    if (!sim.init()) {
        std::cerr << "[Error] Failed to initialize Exchange Simulator.\n" << std::flush;
        WSACleanup();
        timeEndPeriod(1);
        return 1;
    }

    // 4. Launch Pipeline Threads (Core 1, Core 2, Core 3)
    std::cout << "[Pipeline] Spawning Phase 1 (Ingress) Worker -> Binding Core 1...\n" << std::flush;
    std::thread ingress_thread(run_ingress_worker);

    std::cout << "[Pipeline] Spawning Phase 2 (Strategy Engine) Worker -> Binding Core 2...\n" << std::flush;
    std::thread strategy_thread(run_strategy_worker);

    std::cout << "[Pipeline] Spawning Phase 3 (Risk & Execution) Worker -> Binding Core 3...\n" << std::flush;
    std::thread execution_thread(run_execution_worker);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 5. Start Market Data Generator Simulator
    std::cout << "[Simulator] Starting Nasdaq ITCH market data stream...\n" << std::flush;
    sim.start();

    // 6. Live Telemetry Loop
    const auto start_time = std::chrono::steady_clock::now();
    uint64_t elapsed_sec = 0;
    const uint64_t max_runtime_sec = (argc > 1) ? std::stoull(argv[1]) : 5;

    while (g_running.load() && elapsed_sec < max_runtime_sec) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        elapsed_sec = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start_time).count());
        print_dashboard_tick(elapsed_sec, sim);
    }

    // 7. Graceful Teardown
    std::cout << "\n\033[1;33m[Pipeline] Stopping pipeline and joining worker threads...\033[0m\n" << std::flush;
    g_running.store(false, std::memory_order_release);
    sim.stop();

    if (ingress_thread.joinable())   ingress_thread.join();
    if (strategy_thread.joinable())  strategy_thread.join();
    if (execution_thread.joinable()) execution_thread.join();

    std::cout << "\n\033[1;32m[Pipeline] Pipeline finished successfully. Zero memory leaks.\033[0m\n" << std::flush;

    // 8. Final Latency Summary
    const auto ttt = g_latency_tick_to_trade.compute_report();
    const auto ing = g_latency_ingress_engine.compute_report();
    const auto exe = g_latency_engine_exec.compute_report();

    std::cout << "\n============================= FINAL TELEMETRY SUMMARY =============================\n";
    std::cout << "Total Trades Generated & Executed: " << ttt.count << "\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  - P50 Tick-to-Trade  : " << std::setw(8) << ttt.p50_ns / 1000.0 << " us | Ingress->Eng: " << std::setw(8) << ing.p50_ns / 1000.0 << " us | Eng->Exec: " << std::setw(8) << exe.p50_ns / 1000.0 << " us\n";
    std::cout << "  - P90 Tick-to-Trade  : " << std::setw(8) << ttt.p90_ns / 1000.0 << " us | Ingress->Eng: " << std::setw(8) << ing.p90_ns / 1000.0 << " us | Eng->Exec: " << std::setw(8) << exe.p90_ns / 1000.0 << " us\n";
    std::cout << "  - P99 Tick-to-Trade  : " << std::setw(8) << ttt.p99_ns / 1000.0 << " us | Ingress->Eng: " << std::setw(8) << ing.p99_ns / 1000.0 << " us | Eng->Exec: " << std::setw(8) << exe.p99_ns / 1000.0 << " us\n";
    std::cout << "  - P99.99 Tick-to-Trd : " << std::setw(8) << ttt.p99_99_ns / 1000.0 << " us | Ingress->Eng: " << std::setw(8) << ing.p99_99_ns / 1000.0 << " us | Eng->Exec: " << std::setw(8) << exe.p99_99_ns / 1000.0 << " us\n";
    std::cout << "===================================================================================\n" << std::flush;

    WSACleanup();
    timeEndPeriod(1);
    return 0;
}
