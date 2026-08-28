#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include "protocol/itch.hpp"
#include "protocol/ouch.hpp"
#include <atomic>
#include <chrono>
#include <thread>
#include <iostream>
#include <random>
#include <cstring>

#pragma comment(lib, "ws2_32.lib")

namespace hft::simulator {

/**
 * @brief Synthetic Nasdaq Matching Engine Simulator
 * 
 * Runs on a dedicated simulator thread:
 * 1. Broadcasts ITCH 5.0 binary order book updates over UDP (127.0.0.1:9001).
 * 2. Receives and processes outbound OUCH orders from Phase 3 over UDP (127.0.0.1:9002).
 */
class ExchangeSimulator {
public:
    static constexpr uint16_t ITCH_PORT = 9001;
    static constexpr uint16_t OUCH_PORT = 9002;
    static constexpr const char* IP_ADDR = "127.0.0.1";

    ExchangeSimulator() noexcept
        : running_(false),
          itch_sock_(INVALID_SOCKET),
          ouch_sock_(INVALID_SOCKET),
          orders_generated_(0),
          orders_received_(0),
          orders_executed_(0) {}

    ~ExchangeSimulator() {
        stop();
    }

    bool init() noexcept {
        itch_sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (itch_sock_ == INVALID_SOCKET) {
            std::cerr << "[Simulator] Failed to create ITCH socket (WSA Error: " << WSAGetLastError() << ").\n" << std::flush;
            return false;
        }

        int reuse = 1;
        setsockopt(itch_sock_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

        std::memset(&itch_dest_addr_, 0, sizeof(itch_dest_addr_));
        itch_dest_addr_.sin_family = AF_INET;
        itch_dest_addr_.sin_port = htons(ITCH_PORT);
        inet_pton(AF_INET, IP_ADDR, &itch_dest_addr_.sin_addr);

        ouch_sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (ouch_sock_ == INVALID_SOCKET) {
            std::cerr << "[Simulator] Failed to create OUCH socket (WSA Error: " << WSAGetLastError() << ").\n" << std::flush;
            return false;
        }

        setsockopt(ouch_sock_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

        sockaddr_in ouch_bind_addr{};
        ouch_bind_addr.sin_family = AF_INET;
        ouch_bind_addr.sin_port = htons(OUCH_PORT);
        ouch_bind_addr.sin_addr.s_addr = INADDR_ANY;

        if (bind(ouch_sock_, reinterpret_cast<sockaddr*>(&ouch_bind_addr), sizeof(ouch_bind_addr)) == SOCKET_ERROR) {
            std::cerr << "[Simulator] Failed to bind OUCH socket to port " << OUCH_PORT 
                      << " (WSA Error: " << WSAGetLastError() << ")\n" << std::flush;
            return false;
        }

        u_long mode = 1;
        ioctlsocket(ouch_sock_, FIONBIO, &mode);

        return true;
    }

    void start() {
        running_.store(true, std::memory_order_release);
        sim_thread_ = std::thread(&ExchangeSimulator::run, this);
    }

    void stop() {
        if (running_.load(std::memory_order_acquire)) {
            running_.store(false, std::memory_order_release);
            if (sim_thread_.joinable()) {
                sim_thread_.join();
            }
        }

        if (itch_sock_ != INVALID_SOCKET) {
            closesocket(itch_sock_);
            itch_sock_ = INVALID_SOCKET;
        }
        if (ouch_sock_ != INVALID_SOCKET) {
            closesocket(ouch_sock_);
            ouch_sock_ = INVALID_SOCKET;
        }
    }

    [[nodiscard]] uint64_t orders_generated() const noexcept { return orders_generated_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t orders_received() const noexcept { return orders_received_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t orders_executed() const noexcept { return orders_executed_.load(std::memory_order_relaxed); }

private:
    void run() {
        std::mt19937_64 rng(1337);
        std::uniform_int_distribution<uint32_t> price_offset_dist(0, 10);
        std::uniform_int_distribution<uint32_t> size_dist(50, 400);
        std::uniform_int_distribution<uint32_t> action_dist(0, 100);

        uint64_t next_order_ref = 100'000;
        uint32_t mid_price = 150'0000; // NVDA $150.0000

        uint8_t tx_buffer[256];
        uint8_t rx_buffer[256];

        // Seed initial two-sided market
        for (int i = 0; i < 5; ++i) {
            send_add_order(tx_buffer, ++next_order_ref, 'B', mid_price - (5 - i) * 100, 1000 + i * 200, "NVDA    ");
            send_add_order(tx_buffer, ++next_order_ref, 'S', mid_price + (i + 1) * 100, 1000 + i * 200, "NVDA    ");
        }

        // Circular tracking of active simulated order refs for realistic cancels
        static constexpr size_t ACTIVE_POOL_SIZE = 1024;
        uint64_t active_refs[ACTIVE_POOL_SIZE];
        size_t active_pool_count = 0;
        size_t pool_head = 0;

        int wave = 0;
        char dominant_side = 'B';

        while (running_.load(std::memory_order_relaxed)) {
            // Drain incoming OUCH orders
            while (true) {
                sockaddr_in sender_addr{};
                int sender_len = sizeof(sender_addr);
                int bytes_read = recvfrom(ouch_sock_, reinterpret_cast<char*>(rx_buffer), sizeof(rx_buffer), 0,
                                          reinterpret_cast<sockaddr*>(&sender_addr), &sender_len);
                if (bytes_read <= 0) break;

                orders_received_.fetch_add(1, std::memory_order_relaxed);
                const auto* enter = reinterpret_cast<const protocol::OUCH_EnterOrder*>(rx_buffer);
                if (enter->msg_type == 'O') {
                    orders_executed_.fetch_add(1, std::memory_order_relaxed);
                }
            }

            wave++;
            if (wave % 20 == 0) {
                dominant_side = (dominant_side == 'B') ? 'S' : 'B';
            }

            for (int k = 0; k < 6; ++k) {
                const uint32_t act_roll = action_dist(rng);

                if (act_roll < 55 || active_pool_count < 20) {
                    // Add Order
                    char side = (k % 3 == 0) ? ((dominant_side == 'B') ? 'S' : 'B') : dominant_side;
                    uint32_t price = (side == 'B') ? (mid_price - price_offset_dist(rng) * 100) 
                                                   : (mid_price + price_offset_dist(rng) * 100);
                    uint32_t shares = size_dist(rng) * ((side == dominant_side) ? 3 : 1);

                    uint64_t new_ref = ++next_order_ref;
                    send_add_order(tx_buffer, new_ref, side, price, shares, "NVDA    ");

                    active_refs[pool_head] = new_ref;
                    pool_head = (pool_head + 1) % ACTIVE_POOL_SIZE;
                    if (active_pool_count < ACTIVE_POOL_SIZE) active_pool_count++;
                } else if (act_roll < 75 && active_pool_count > 0) {
                    // Delete / Cancel Order
                    size_t target_slot = (pool_head + ACTIVE_POOL_SIZE - 1 - (rng() % std::min<size_t>(active_pool_count, 32))) % ACTIVE_POOL_SIZE;
                    uint64_t cancel_ref = active_refs[target_slot];
                    if (cancel_ref != 0) {
                        send_delete_order(tx_buffer, cancel_ref);
                        active_refs[target_slot] = 0;
                    }
                } else if (active_pool_count > 0) {
                    // Execute Partial
                    size_t target_slot = (pool_head + ACTIVE_POOL_SIZE - 1 - (rng() % std::min<size_t>(active_pool_count, 16))) % ACTIVE_POOL_SIZE;
                    uint64_t exec_ref = active_refs[target_slot];
                    if (exec_ref != 0) {
                        send_execute_order(tx_buffer, exec_ref, 50);
                    }
                }
                orders_generated_.fetch_add(1, std::memory_order_relaxed);
            }

            auto spin_target = std::chrono::high_resolution_clock::now() + std::chrono::microseconds(40);
            while (std::chrono::high_resolution_clock::now() < spin_target) {
                _mm_pause();
            }
        }
    }

    inline void send_add_order(uint8_t* buffer, uint64_t order_ref, char side, uint32_t price, uint32_t shares, const char* symbol) noexcept {
        auto* pkt = reinterpret_cast<protocol::ITCH_AddOrder*>(buffer);
        pkt->msg_type = 'A';
        pkt->locate_code = _byteswap_ushort(1);
        pkt->tracking_number = _byteswap_ushort(0);
        
        uint64_t now_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        pkt->timestamp[0] = static_cast<uint8_t>((now_ns >> 40) & 0xFF);
        pkt->timestamp[1] = static_cast<uint8_t>((now_ns >> 32) & 0xFF);
        pkt->timestamp[2] = static_cast<uint8_t>((now_ns >> 24) & 0xFF);
        pkt->timestamp[3] = static_cast<uint8_t>((now_ns >> 16) & 0xFF);
        pkt->timestamp[4] = static_cast<uint8_t>((now_ns >> 8) & 0xFF);
        pkt->timestamp[5] = static_cast<uint8_t>(now_ns & 0xFF);

        pkt->order_ref_number = _byteswap_uint64(order_ref);
        pkt->buy_sell = side;
        pkt->shares = _byteswap_ulong(shares);
        std::memcpy(pkt->stock, symbol, 8);
        pkt->price = _byteswap_ulong(price);

        sendto(itch_sock_, reinterpret_cast<const char*>(buffer), sizeof(protocol::ITCH_AddOrder), 0,
               reinterpret_cast<const sockaddr*>(&itch_dest_addr_), sizeof(itch_dest_addr_));
    }

    inline void send_execute_order(uint8_t* buffer, uint64_t order_ref, uint32_t shares) noexcept {
        auto* pkt = reinterpret_cast<protocol::ITCH_OrderExecuted*>(buffer);
        pkt->msg_type = 'E';
        pkt->locate_code = _byteswap_ushort(1);
        pkt->tracking_number = _byteswap_ushort(0);
        std::memset(pkt->timestamp, 0, 6);
        pkt->order_ref_number = _byteswap_uint64(order_ref);
        pkt->executed_shares = _byteswap_ulong(shares);
        pkt->match_number = _byteswap_uint64(12345);

        sendto(itch_sock_, reinterpret_cast<const char*>(buffer), sizeof(protocol::ITCH_OrderExecuted), 0,
               reinterpret_cast<const sockaddr*>(&itch_dest_addr_), sizeof(itch_dest_addr_));
    }

    inline void send_delete_order(uint8_t* buffer, uint64_t order_ref) noexcept {
        auto* pkt = reinterpret_cast<protocol::ITCH_OrderDelete*>(buffer);
        pkt->msg_type = 'D';
        pkt->locate_code = _byteswap_ushort(1);
        pkt->tracking_number = _byteswap_ushort(0);
        std::memset(pkt->timestamp, 0, 6);
        pkt->order_ref_number = _byteswap_uint64(order_ref);

        sendto(itch_sock_, reinterpret_cast<const char*>(buffer), sizeof(protocol::ITCH_OrderDelete), 0,
               reinterpret_cast<const sockaddr*>(&itch_dest_addr_), sizeof(itch_dest_addr_));
    }

    std::atomic<bool> running_;
    SOCKET itch_sock_;
    SOCKET ouch_sock_;
    sockaddr_in itch_dest_addr_{};
    std::thread sim_thread_;

    std::atomic<uint64_t> orders_generated_;
    std::atomic<uint64_t> orders_received_;
    std::atomic<uint64_t> orders_executed_;
};

} // namespace hft::simulator
