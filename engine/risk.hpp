#pragma once

#include "protocol/ouch.hpp"
#include <atomic>
#include <cstdint>
#include <string_view>

namespace hft::engine {

enum class RiskCheckResult : uint8_t {
    PASSED = 0,
    REJECTED_KILL_SWITCH = 1,
    REJECTED_MAX_POSITION = 2,
    REJECTED_MAX_QTY = 3,
    REJECTED_PRICE_COLLAR = 4,
    REJECTED_MAX_NOTIONAL = 5,
};

inline const char* risk_result_to_string(RiskCheckResult res) noexcept {
    switch (res) {
        case RiskCheckResult::PASSED:                return "PASSED";
        case RiskCheckResult::REJECTED_KILL_SWITCH:  return "REJECTED_KILL_SWITCH";
        case RiskCheckResult::REJECTED_MAX_POSITION: return "REJECTED_MAX_POSITION";
        case RiskCheckResult::REJECTED_MAX_QTY:      return "REJECTED_MAX_QTY";
        case RiskCheckResult::REJECTED_PRICE_COLLAR: return "REJECTED_PRICE_COLLAR";
        case RiskCheckResult::REJECTED_MAX_NOTIONAL: return "REJECTED_MAX_NOTIONAL";
        default:                                     return "UNKNOWN";
    }
}

/**
 * @brief Pre-Trade Bitwise Risk Engine
 * 
 * Sub-microsecond validation performed on Core 3 before any packet is formatted and dispatched.
 */
class PreTradeRiskEngine {
public:
    struct RiskLimits {
        int64_t  max_position{10'000};       // Max net shares held (-10,000 to +10,000)
        uint32_t max_order_qty{1'000};       // Max shares per single order
        uint32_t min_price_collar{100'0000}; // $100.0000 minimum limit price
        uint32_t max_price_collar{300'0000}; // $300.0000 maximum limit price
        uint64_t max_order_notional{500'000'0000}; // $500,000 total notional per order
    };

    explicit PreTradeRiskEngine(RiskLimits limits = RiskLimits{}) noexcept
        : limits_(limits),
          current_position_(0),
          kill_switch_(false),
          orders_checked_(0),
          orders_passed_(0),
          orders_rejected_(0) {}

    /**
     * @brief Validate an outbound trade signal against pre-trade risk checks
     * @param signal The candidate trade signal
     * @return RiskCheckResult indicating pass or exact failure code
     */
    [[nodiscard]] inline RiskCheckResult validate(const protocol::TradeSignal& signal) noexcept {
        orders_checked_++;

        // 1. Global Kill Switch Check
        if (kill_switch_.load(std::memory_order_relaxed)) [[unlikely]] {
            orders_rejected_++;
            return RiskCheckResult::REJECTED_KILL_SWITCH;
        }

        // 2. Maximum Single-Order Quantity Check
        if (signal.shares == 0 || signal.shares > limits_.max_order_qty) [[unlikely]] {
            orders_rejected_++;
            return RiskCheckResult::REJECTED_MAX_QTY;
        }

        // 3. Price Collar Validation
        if (signal.price < limits_.min_price_collar || signal.price > limits_.max_price_collar) [[unlikely]] {
            orders_rejected_++;
            return RiskCheckResult::REJECTED_PRICE_COLLAR;
        }

        // 4. Maximum Notional Value Check (shares * price)
        const uint64_t notional = static_cast<uint64_t>(signal.shares) * static_cast<uint64_t>(signal.price);
        if (notional > limits_.max_order_notional) [[unlikely]] {
            orders_rejected_++;
            return RiskCheckResult::REJECTED_MAX_NOTIONAL;
        }

        // 5. Maximum Position Limit Check
        const int64_t delta = (signal.side == 'B') ? static_cast<int64_t>(signal.shares) 
                                                   : -static_cast<int64_t>(signal.shares);
        const int64_t projected_position = current_position_ + delta;
        if (projected_position > limits_.max_position || projected_position < -limits_.max_position) [[unlikely]] {
            orders_rejected_++;
            return RiskCheckResult::REJECTED_MAX_POSITION;
        }

        // Passed all risk checks: update position tracking
        current_position_ = projected_position;
        orders_passed_++;
        return RiskCheckResult::PASSED;
    }

    /**
     * @brief Arm or disarm the global emergency kill switch
     */
    inline void set_kill_switch(bool active) noexcept {
        kill_switch_.store(active, std::memory_order_release);
    }

    [[nodiscard]] inline bool is_kill_switch_active() const noexcept {
        return kill_switch_.load(std::memory_order_acquire);
    }

    inline void on_fill(char side, uint32_t shares) noexcept {
        // Adjust confirmed actual position if needed
        (void)side;
        (void)shares;
    }

    [[nodiscard]] inline int64_t current_position() const noexcept {
        return current_position_;
    }

    [[nodiscard]] inline uint64_t orders_checked() const noexcept { return orders_checked_; }
    [[nodiscard]] inline uint64_t orders_passed() const noexcept { return orders_passed_; }
    [[nodiscard]] inline uint64_t orders_rejected() const noexcept { return orders_rejected_; }

private:
    RiskLimits           limits_;
    int64_t              current_position_;
    std::atomic<bool>    kill_switch_;
    uint64_t             orders_checked_;
    uint64_t             orders_passed_;
    uint64_t             orders_rejected_;
};

} // namespace hft::engine
