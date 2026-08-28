#pragma once

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <intrin.h>

namespace hft::protocol {

#pragma pack(push, 1)

/**
 * @brief Nasdaq OUCH 4.2 Outbound Order Entry Packet (Type 'O')
 * Total Size: 49 bytes (fixed-width, binary formatted)
 */
struct OUCH_EnterOrder {
    char     msg_type;          // 'O' (Enter Order)
    char     order_token[14];   // Unique client token
    char     buy_sell;          // 'B' or 'S'
    uint32_t shares;            // Big-endian binary shares
    char     stock[8];          // Stock ticker space-padded
    uint32_t price;             // Big-endian binary price (fixed 4-dec)
    uint32_t time_in_force;     // Big-endian (0 = IOC, 99999 = Day)
    char     firm[4];           // Firm identifier
    char     display;           // 'Y' = Visible, 'N' = Hidden
    char     capacity;          // 'A' = Agency, 'P' = Principal
    char     iso_eligible;      // 'Y' / 'N'
    uint32_t min_quantity;      // Minimum execution quantity
    char     cross_type;        // 'N' = Continuous market
};

/**
 * @brief Nasdaq OUCH 4.2 Inbound Execution / Acceptance Response (Type 'A')
 */
struct OUCH_OrderAccepted {
    char     msg_type;          // 'A'
    uint8_t  timestamp[8];      // Nanoseconds
    char     order_token[14];   // Client order token
    char     buy_sell;          // 'B' or 'S'
    uint32_t shares;            // Quantity accepted
    char     stock[8];          // Symbol
    uint32_t price;             // Limit price
    uint32_t time_in_force;
    char     firm[4];
    char     display;
    uint64_t order_reference;
    char     capacity;
    char     iso_eligible;
    uint32_t min_quantity;
    char     cross_type;
    char     order_state;       // 'L' = Live
};

/**
 * @brief Nasdaq OUCH 4.2 Order Executed Response (Type 'E')
 */
struct OUCH_OrderExecuted {
    char     msg_type;          // 'E'
    uint8_t  timestamp[8];
    char     order_token[14];
    uint32_t executed_shares;
    uint32_t execution_price;
    char     liquidity_flag;
    uint64_t match_number;
};

/**
 * @brief Nasdaq OUCH 4.2 Order Rejected Response (Type 'J')
 */
struct OUCH_OrderRejected {
    char     msg_type;          // 'J'
    uint8_t  timestamp[8];
    char     order_token[14];
    char     reject_code;       // Reason code
};

#pragma pack(pop)

/**
 * @brief Internal Trade Signal emitted from Phase 2 (Strategy) to Phase 3 (Risk & Execution)
 * Cache-aligned to 64 bytes
 */
struct alignas(64) TradeSignal {
    uint64_t t0_ingress_ns;     // Ingress time (T0)
    uint64_t t1_strategy_ns;    // Strategy evaluation complete time (T1)
    uint64_t signal_id;         // Monotonic signal identifier
    uint32_t price;             // Limit price
    uint32_t shares;            // Order quantity
    double   imbalance;         // Calculated Order Book Imbalance (OBI) [-1.0, +1.0]
    char     side;              // 'B' or 'S'
    char     symbol[8];         // e.g. "NVDA    "
    uint8_t  _pad[15];          // Aligns structure to exactly 64 bytes
};

static_assert(sizeof(TradeSignal) == 64, "TradeSignal must be 64 bytes");

/**
 * @brief Zero-allocation OUCH packet formatter
 */
class OUCHFormatter {
public:
    /**
     * @brief Formats an Enter Order packet into pre-allocated memory
     * @param dest Buffer of at least sizeof(OUCH_EnterOrder)
     * @param token_id Monotonic integer order token
     * @param side 'B' or 'S'
     * @param shares Quantity
     * @param symbol 8-char ticker symbol
     * @param price Limit price (4 decimal fixed point)
     * @return Number of bytes written (sizeof(OUCH_EnterOrder))
     */
    static inline size_t format_enter_order(uint8_t* dest,
                                            uint64_t token_id,
                                            char side,
                                            uint32_t shares,
                                            const char* symbol,
                                            uint32_t price) noexcept {
        auto* pkt = reinterpret_cast<OUCH_EnterOrder*>(dest);
        pkt->msg_type = 'O';

        // Fast zero-allocation token formatting (e.g. "ORD0000000001")
        std::memset(pkt->order_token, ' ', sizeof(pkt->order_token));
        char temp_token[16];
        int token_len = snprintf(temp_token, sizeof(temp_token), "ORD%010llu", static_cast<unsigned long long>(token_id));
        if (token_len > 0) {
            size_t copy_len = (token_len < 14) ? token_len : 14;
            std::memcpy(pkt->order_token, temp_token, copy_len);
        }

        pkt->buy_sell = side;
        pkt->shares = _byteswap_ulong(shares);
        std::memcpy(pkt->stock, symbol, 8);
        pkt->price = _byteswap_ulong(price);
        pkt->time_in_force = _byteswap_ulong(0); // IOC (Immediate or Cancel)
        std::memcpy(pkt->firm, "HFT1", 4);
        pkt->display = 'Y';
        pkt->capacity = 'P'; // Principal
        pkt->iso_eligible = 'N';
        pkt->min_quantity = _byteswap_ulong(1);
        pkt->cross_type = 'N';

        return sizeof(OUCH_EnterOrder);
    }
};

} // namespace hft::protocol
