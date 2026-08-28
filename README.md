# Low-Latency Trading Pipeline (C++20)

A sub-microsecond, 3-phase market-data-to-execution pipeline in **C++20**, engineered for zero dynamic memory allocations post-initialization and zero lock contention on the critical path.

---

## Technical Architecture

```mermaid
graph TD
    subgraph Phase 1: Ingress (Core 1)
        UDP[WinSock2 UDP Socket] -->|Raw Bytes| Ingress[Zero-Copy ITCH Parser]
        Ingress -->|T0 Timestamp + Msg| Q1[(SPSC Queue #1)]
    end
    
    subgraph Phase 2: Strategy Engine (Core 2)
        Q1 -->|Pop Message| Engine[Flat Order Book & OBI Strategy]
        Engine -->|Signal + T0 + T1| Q2[(SPSC Queue #2)]
    end
    
    subgraph Phase 3: Risk & Execution (Core 3)
        Q2 -->|Pop Signal| Risk[Pre-Trade Risk Engine]
        Risk -->|Pass| Formatter[OUCH Formatter]
        Formatter -->|Formatted Bytes| Output[WinSock2 UDP Execution]
        Formatter -->|T_exec - T0| Telemetry[(Telemetry Aggregator)]
    end
    
    subgraph Monitoring & Simulation (Core 0 / Main)
        Simulator[Exchange Simulator (UDP)] -.->|ITCH UDP Packets (Port 9001)| UDP
        Output -.->|OUCH UDP Orders (Port 9002)| Simulator
        Telemetry --> Stats[P50 / P90 / P99 / P99.99 Dashboard]
    end
```

---

## Core Components

| Component | File Path | Description |
|---|---|---|
| **Build System** | [`CMakeLists.txt`](file:///c:/Users/LOQ/Desktop/hft/CMakeLists.txt) | C++20 release build, `/O2` / `-O3`, links `ws2_32` and `winmm`. |
| **Lock-Free Queue** | [`queue/spsc.hpp`](file:///c:/Users/LOQ/Desktop/hft/queue/spsc.hpp) | SPSC circular ring buffer, `alignas(64)` cache-aligned, zero dynamic allocations. |
| **ITCH Protocol** | [`protocol/itch.hpp`](file:///c:/Users/LOQ/Desktop/hft/protocol/itch.hpp) | Packed Nasdaq ITCH 5.0 binary message structs & zero-copy binary parser. |
| **OUCH Protocol** | [`protocol/ouch.hpp`](file:///c:/Users/LOQ/Desktop/hft/protocol/ouch.hpp) | Nasdaq OUCH 4.2 wire format and zero-allocation binary order serializer. |
| **Order Book & Strategy** | [`engine/orderbook.hpp`](file:///c:/Users/LOQ/Desktop/hft/engine/orderbook.hpp) | Flat array $O(1)$ limit order book, top-10 level depth, Order Book Imbalance (OBI) strategy. |
| **Pre-Trade Risk Engine** | [`engine/risk.hpp`](file:///c:/Users/LOQ/Desktop/hft/engine/risk.hpp) | Bitwise checks for max position limit, max order quantity, price collars, and atomic kill switch. |
| **CPU Thread Affinity** | [`pipeline/affinity.hpp`](file:///c:/Users/LOQ/Desktop/hft/pipeline/affinity.hpp) | Hardware core isolation using Windows `SetThreadAffinityMask` on logical CPU cores. |
| **Exchange Simulator** | [`simulator/exchange.hpp`](file:///c:/Users/LOQ/Desktop/hft/simulator/exchange.hpp) | Synthetic Nasdaq matching engine broadcasting ITCH over UDP (9001) and receiving OUCH orders (9002). |
| **Telemetry & Stats** | [`telemetry/stats.hpp`](file:///c:/Users/LOQ/Desktop/hft/telemetry/stats.hpp) | High-precision timer and percentile analytics (Min, Mean, P50, P90, P99, P99.99, Max). |
| **Main Entry & Wiring** | [`src/main.cpp`](file:///c:/Users/LOQ/Desktop/hft/src/main.cpp) | Pipeline bootloader, worker threads pinning (Cores 1, 2, 3), and live terminal dashboard. |

---

## Building and Running

### Requirements
- **Compiler**: MSVC (Visual Studio 2022 C++20) or Clang/LLVM
- **Build Tool**: CMake 3.20+
- **Platform**: Windows 10/11 (x64)

### Build Commands
```powershell
# 1. Configure CMake project in Release mode
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# 2. Compile optimized binary
cmake --build build --config Release
```

### Execution
```powershell
# Run pipeline with live telemetry dashboard for 5 seconds (or specify custom duration in seconds)
.\build\Release\low_latency_pipeline.exe 5
```
