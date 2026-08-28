#pragma once

#include <cstdint>
#include <cstring>
#include <string_view>
#include <chrono>
#include <intrin.h>

#ifdef DELETE
#undef DELETE
#endif

namespace hft::protocol {

#pragma pack(push, 1)

// Endian conversion utilities (Nasdaq ITCH uses Big-Endian / Network Order)
inline uint16_t be16_to_cpu(uint16_t val) noexcept {
    return _byteswap_ushort(val);
}

inline uint32_t be32_to_cpu(uint32_t val) noexcept {
    return _byteswap_ulong(val);
}

inline uint64_t be64_to_cpu(uint64_t val) noexcept {
    return _byteswap_uint64(val);
}

// 48-bit Big-Endian timestamp conversion
inline uint64_t be48_to_cpu(const uint8_t* bytes) noexcept {
    return (static_cast<uint64_t>(bytes[0]) << 40) |
           (static_cast<uint64_t>(bytes[1]) << 32) |
           (static_cast<uint64_t>(bytes[2]) << 24) |
           (static_cast<uint64_t>(bytes[3]) << 16) |
           (static_cast<uint64_t>(bytes[4]) << 8)  |
           (static_cast<uint64_t>(bytes[5]));
}

/**
 * @brief Nasdaq ITCH 5.0 Message Type 'A' - Add Order
 * Size: 36 bytes
 */
struct ITCH_AddOrder {
    char     msg_type;          // 'A'
    uint16_t locate_code;
    uint16_t tracking_number;
    uint8_t  timestamp[6];      // Nanoseconds since midnight (48-bit)
    uint64_t order_ref_number;  // Unique order reference
    char     buy_sell;          // 'B' = Buy, 'S' = Sell
    uint32_t shares;            // Quantity
    char     stock[8];          // Ticker symbol padded with spaces
    uint32_t price;             // Price (4 decimal fixed point, e.g. 1500000 = $150.0000)
};

/**
 * @brief Nasdaq ITCH 5.0 Message Type 'E' - Order Executed
 * Size: 31 bytes
 */
struct ITCH_OrderExecuted {
    char     msg_type;          // 'E'
    uint16_t locate_code;
    uint16_t tracking_number;
    uint8_t  timestamp[6];
    uint64_t order_ref_number;
    uint32_t executed_shares;
    uint64_t match_number;
};

/**
 * @brief Nasdaq ITCH 5.0 Message Type 'X' - Order Cancel (Partial)
 * Size: 23 bytes
 */
struct ITCH_OrderCancel {
    char     msg_type;          // 'X'
    uint16_t locate_code;
    uint16_t tracking_number;
    uint8_t  timestamp[6];
    uint64_t order_ref_number;
    uint32_t canceled_shares;
};

/**
 * @brief Nasdaq ITCH 5.0 Message Type 'D' - Order Delete (Full)
 * Size: 19 bytes
 */
struct ITCH_OrderDelete {
    char     msg_type;          // 'D'
    uint16_t locate_code;
    uint16_t tracking_number;
    uint8_t  timestamp[6];
    uint64_t order_ref_number;
};

#pragma pack(pop)

enum class MarketAction : uint8_t {
    ADD_ORDER = 0,
    EXECUTE_ORDER = 1,
    CANCEL_ORDER = 2,
    DELETE_ORDER = 3,
    UNKNOWN_ACTION = 255
};

/**
 * @brief Internal normalized market update passed across SPSC Queue 1 to the Strategy Engine
 * Size: 64 bytes (fits perfectly into 1 cache line)
 */
struct alignas(64) MarketUpdateMessage {
    uint64_t     t0_timestamp_ns;   // Ingress hardware/high-res timestamp
    uint64_t     order_ref;         // Order ID
    uint32_t     price;             // Price (fixed point, scaled x100 or x10000)
    uint32_t     shares;            // Quantity
    MarketAction action;            // ADD_ORDER, EXECUTE_ORDER, CANCEL_ORDER, DELETE_ORDER
    char         side;              // 'B' or 'S'
    char         symbol[8];         // e.g. "NVDA    "
    uint8_t      _padding[30];      // Explicit padding to 64 bytes
};

static_assert(sizeof(MarketUpdateMessage) == 64, "MarketUpdateMessage must be exactly 64 bytes");

/**
 * @brief Zero-copy ITCH binary message parser
 */
class ITCHParser {
public:
    /**
     * @brief Parse a raw UDP payload containing ITCH messages without dynamic allocation
     * @param buffer Raw network buffer
     * @param len Buffer length in bytes
     * @param t0 Ingress timestamp
     * @param out_msg Normalized output message
     * @return true if a valid message was parsed, false otherwise
     */
    static inline bool parse_single(const uint8_t* buffer, size_t len, uint64_t t0, MarketUpdateMessage& out_msg) noexcept {
        if (len < 1) [[unlikely]] return false;

        const char msg_type = static_cast<char>(buffer[0]);
        out_msg.t0_timestamp_ns = t0;

        switch (msg_type) {
            case 'A': { // Add Order
                if (len < sizeof(ITCH_AddOrder)) [[unlikely]] return false;
                const auto* m = reinterpret_cast<const ITCH_AddOrder*>(buffer);
                out_msg.action = MarketAction::ADD_ORDER;
                out_msg.order_ref = be64_to_cpu(m->order_ref_number);
                out_msg.side = m->buy_sell;
                out_msg.shares = be32_to_cpu(m->shares);
                out_msg.price = be32_to_cpu(m->price);
                std::memcpy(out_msg.symbol, m->stock, 8);
                return true;
            }
            case 'E': { // Order Executed
                if (len < sizeof(ITCH_OrderExecuted)) [[unlikely]] return false;
                const auto* m = reinterpret_cast<const ITCH_OrderExecuted*>(buffer);
                out_msg.action = MarketAction::EXECUTE_ORDER;
                out_msg.order_ref = be64_to_cpu(m->order_ref_number);
                out_msg.shares = be32_to_cpu(m->executed_shares);
                out_msg.price = 0; // Looked up in orderbook
                out_msg.side = ' ';
                return true;
            }
            case 'X': { // Order Cancel
                if (len < sizeof(ITCH_OrderCancel)) [[unlikely]] return false;
                const auto* m = reinterpret_cast<const ITCH_OrderCancel*>(buffer);
                out_msg.action = MarketAction::CANCEL_ORDER;
                out_msg.order_ref = be64_to_cpu(m->order_ref_number);
                out_msg.shares = be32_to_cpu(m->canceled_shares);
                out_msg.price = 0;
                out_msg.side = ' ';
                return true;
            }
            case 'D': { // Order Delete
                if (len < sizeof(ITCH_OrderDelete)) [[unlikely]] return false;
                const auto* m = reinterpret_cast<const ITCH_OrderDelete*>(buffer);
                out_msg.action = MarketAction::DELETE_ORDER;
                out_msg.order_ref = be64_to_cpu(m->order_ref_number);
                out_msg.shares = 0;
                out_msg.price = 0;
                out_msg.side = ' ';
                return true;
            }
            default:
                return false;
        }
    }
};

} // namespace hft::protocol
