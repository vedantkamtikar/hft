#pragma once

#include "protocol/itch.hpp"
#include "protocol/ouch.hpp"
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <array>
#include <optional>

namespace hft::engine {

/**
 * @brief Fixed-size Flat Limit Order Book
 * 
 * Design characteristics:
 * - Zero dynamic memory allocations during runtime.
 * - Direct array indexing by price ticks for O(1) level updates.
 * - Pre-allocated fixed-capacity open-addressing hash table for active order reference lookups.
 * - Real-time 10-level price depth maintenance and Order Book Imbalance (OBI) computation.
 */
class FlatOrderBook {
public:
    static constexpr uint32_t TICK_SIZE = 100;                 // 0.0100 fixed-point (1 cent)
    static constexpr uint32_t BASE_PRICE = 100'0000;           // $100.0000
    static constexpr uint32_t MAX_PRICE  = 300'0000;           // $300.0000
    static constexpr size_t   NUM_PRICE_SLOTS = (MAX_PRICE - BASE_PRICE) / TICK_SIZE + 1; // 20,001 slots
    static constexpr size_t   MAX_ACTIVE_ORDERS = 65536;       // Power-of-2 for fast bitmask indexing
    static constexpr size_t   ORDER_MASK = MAX_ACTIVE_ORDERS - 1;
    static constexpr size_t   MAX_DEPTH_LEVELS = 10;

    struct PriceLevel {
        uint32_t volume{0};
        uint32_t order_count{0};
    };

    struct StoredOrder {
        uint64_t order_ref{0};
        uint32_t price{0};
        uint32_t shares{0};
        char     side{' '}; // 'B' or 'S'
        bool     active{false};
    };

    struct DepthLevel {
        uint32_t price{0};
        uint32_t volume{0};
    };

    struct TopDepth {
        std::array<DepthLevel, MAX_DEPTH_LEVELS> bids{};
        std::array<DepthLevel, MAX_DEPTH_LEVELS> asks{};
        uint32_t total_bid_vol{0};
        uint32_t total_ask_vol{0};
        double   imbalance{0.0};
    };

    FlatOrderBook() noexcept {
        reset();
    }

    void reset() noexcept {
        std::memset(bid_levels_, 0, sizeof(bid_levels_));
        std::memset(ask_levels_, 0, sizeof(ask_levels_));
        std::memset(orders_table_, 0, sizeof(orders_table_));
        best_bid_idx_ = 0;
        best_ask_idx_ = NUM_PRICE_SLOTS - 1;
        has_bids_ = false;
        has_asks_ = false;
    }

    /**
     * @brief Apply an Add Order event
     */
    inline void on_add(uint64_t order_ref, char side, uint32_t price, uint32_t shares) noexcept {
        const size_t slot = price_to_slot(price);
        if (slot >= NUM_PRICE_SLOTS) [[unlikely]] return;

        store_order(order_ref, side, price, shares);

        if (side == 'B') {
            bid_levels_[slot].volume += shares;
            bid_levels_[slot].order_count++;
            if (!has_bids_ || slot > best_bid_idx_) {
                best_bid_idx_ = slot;
                has_bids_ = true;
            }
        } else if (side == 'S') {
            ask_levels_[slot].volume += shares;
            ask_levels_[slot].order_count++;
            if (!has_asks_ || slot < best_ask_idx_) {
                best_ask_idx_ = slot;
                has_asks_ = true;
            }
        }
    }

    /**
     * @brief Apply an Order Executed or Canceled event (partial or full quantity reduction)
     */
    inline void on_execute_or_cancel(uint64_t order_ref, uint32_t reduced_shares) noexcept {
        StoredOrder* ord = find_order(order_ref);
        if (!ord || !ord->active) [[unlikely]] return;

        const uint32_t actual_reduction = (reduced_shares >= ord->shares) ? ord->shares : reduced_shares;
        const size_t slot = price_to_slot(ord->price);
        if (slot >= NUM_PRICE_SLOTS) [[unlikely]] return;

        if (ord->side == 'B') {
            if (bid_levels_[slot].volume >= actual_reduction) {
                bid_levels_[slot].volume -= actual_reduction;
            } else {
                bid_levels_[slot].volume = 0;
            }
            if (actual_reduction == ord->shares) {
                if (bid_levels_[slot].order_count > 0) bid_levels_[slot].order_count--;
                ord->active = false;
                if (bid_levels_[slot].volume == 0 && slot == best_bid_idx_) {
                    recalculate_best_bid();
                }
            } else {
                ord->shares -= actual_reduction;
            }
        } else if (ord->side == 'S') {
            if (ask_levels_[slot].volume >= actual_reduction) {
                ask_levels_[slot].volume -= actual_reduction;
            } else {
                ask_levels_[slot].volume = 0;
            }
            if (actual_reduction == ord->shares) {
                if (ask_levels_[slot].order_count > 0) ask_levels_[slot].order_count--;
                ord->active = false;
                if (ask_levels_[slot].volume == 0 && slot == best_ask_idx_) {
                    recalculate_best_ask();
                }
            } else {
                ord->shares -= actual_reduction;
            }
        }
    }

    /**
     * @brief Apply an Order Delete event (full removal)
     */
    inline void on_delete(uint64_t order_ref) noexcept {
        StoredOrder* ord = find_order(order_ref);
        if (!ord || !ord->active) [[unlikely]] return;

        const size_t slot = price_to_slot(ord->price);
        if (slot < NUM_PRICE_SLOTS) {
            if (ord->side == 'B') {
                if (bid_levels_[slot].volume >= ord->shares) {
                    bid_levels_[slot].volume -= ord->shares;
                } else {
                    bid_levels_[slot].volume = 0;
                }
                if (bid_levels_[slot].order_count > 0) bid_levels_[slot].order_count--;
                if (bid_levels_[slot].volume == 0 && slot == best_bid_idx_) {
                    recalculate_best_bid();
                }
            } else if (ord->side == 'S') {
                if (ask_levels_[slot].volume >= ord->shares) {
                    ask_levels_[slot].volume -= ord->shares;
                } else {
                    ask_levels_[slot].volume = 0;
                }
                if (ask_levels_[slot].order_count > 0) ask_levels_[slot].order_count--;
                if (ask_levels_[slot].volume == 0 && slot == best_ask_idx_) {
                    recalculate_best_ask();
                }
            }
        }
        ord->active = false;
    }

    /**
     * @brief Process an inbound market update from Phase 1
     */
    inline void process_update(const protocol::MarketUpdateMessage& msg) noexcept {
        switch (msg.action) {
            case protocol::MarketAction::ADD_ORDER:
                on_add(msg.order_ref, msg.side, msg.price, msg.shares);
                break;
            case protocol::MarketAction::EXECUTE_ORDER:
            case protocol::MarketAction::CANCEL_ORDER:
                on_execute_or_cancel(msg.order_ref, msg.shares);
                break;
            case protocol::MarketAction::DELETE_ORDER:
                on_delete(msg.order_ref);
                break;
            default:
                break;
        }
    }

    /**
     * @brief Compute the 10-level price depth and Order Book Imbalance (OBI)
     * @return TopDepth struct containing top 10 bids, top 10 asks, and imbalance
     */
    [[nodiscard]] inline TopDepth compute_top_depth() const noexcept {
        TopDepth depth{};
        uint32_t bid_count = 0;
        uint32_t ask_count = 0;

        if (has_bids_) {
            for (int64_t i = static_cast<int64_t>(best_bid_idx_); i >= 0 && bid_count < MAX_DEPTH_LEVELS; --i) {
                if (bid_levels_[i].volume > 0) {
                    depth.bids[bid_count] = { slot_to_price(static_cast<size_t>(i)), bid_levels_[i].volume };
                    depth.total_bid_vol += bid_levels_[i].volume;
                    bid_count++;
                }
            }
        }

        if (has_asks_) {
            for (size_t i = best_ask_idx_; i < NUM_PRICE_SLOTS && ask_count < MAX_DEPTH_LEVELS; ++i) {
                if (ask_levels_[i].volume > 0) {
                    depth.asks[ask_count] = { slot_to_price(i), ask_levels_[i].volume };
                    depth.total_ask_vol += ask_levels_[i].volume;
                    ask_count++;
                }
            }
        }

        const uint32_t sum_vol = depth.total_bid_vol + depth.total_ask_vol;
        if (sum_vol > 0) {
            depth.imbalance = static_cast<double>(static_cast<int64_t>(depth.total_bid_vol) - static_cast<int64_t>(depth.total_ask_vol)) 
                            / static_cast<double>(sum_vol);
        } else {
            depth.imbalance = 0.0;
        }

        return depth;
    }

    [[nodiscard]] inline bool has_valid_market() const noexcept {
        return has_bids_ && has_asks_ && (best_bid_price() < best_ask_price());
    }

    [[nodiscard]] inline uint32_t best_bid_price() const noexcept {
        return has_bids_ ? slot_to_price(best_bid_idx_) : 0;
    }

    [[nodiscard]] inline uint32_t best_ask_price() const noexcept {
        return has_asks_ ? slot_to_price(best_ask_idx_) : 0;
    }

    [[nodiscard]] inline uint32_t best_bid_volume() const noexcept {
        return has_bids_ ? bid_levels_[best_bid_idx_].volume : 0;
    }

    [[nodiscard]] inline uint32_t best_ask_volume() const noexcept {
        return has_asks_ ? ask_levels_[best_ask_idx_].volume : 0;
    }

private:
    [[nodiscard]] static constexpr size_t price_to_slot(uint32_t price) noexcept {
        if (price < BASE_PRICE) return 0;
        size_t slot = (price - BASE_PRICE) / TICK_SIZE;
        return (slot < NUM_PRICE_SLOTS) ? slot : (NUM_PRICE_SLOTS - 1);
    }

    [[nodiscard]] static constexpr uint32_t slot_to_price(size_t slot) noexcept {
        return BASE_PRICE + static_cast<uint32_t>(slot * TICK_SIZE);
    }

    inline void recalculate_best_bid() noexcept {
        for (int64_t i = static_cast<int64_t>(best_bid_idx_); i >= 0; --i) {
            if (bid_levels_[i].volume > 0) {
                best_bid_idx_ = static_cast<size_t>(i);
                has_bids_ = true;
                return;
            }
        }
        has_bids_ = false;
        best_bid_idx_ = 0;
    }

    inline void recalculate_best_ask() noexcept {
        for (size_t i = best_ask_idx_; i < NUM_PRICE_SLOTS; ++i) {
            if (ask_levels_[i].volume > 0) {
                best_ask_idx_ = i;
                has_asks_ = true;
                return;
            }
        }
        has_asks_ = false;
        best_ask_idx_ = NUM_PRICE_SLOTS - 1;
    }

    inline void store_order(uint64_t ref, char side, uint32_t price, uint32_t shares) noexcept {
        size_t idx = static_cast<size_t>(ref) & ORDER_MASK;
        for (size_t probe = 0; probe < 32; ++probe) {
            size_t slot = (idx + probe) & ORDER_MASK;
            if (!orders_table_[slot].active || orders_table_[slot].order_ref == ref) {
                orders_table_[slot] = { ref, price, shares, side, true };
                return;
            }
        }
        orders_table_[idx] = { ref, price, shares, side, true };
    }

    [[nodiscard]] inline StoredOrder* find_order(uint64_t ref) noexcept {
        size_t idx = static_cast<size_t>(ref) & ORDER_MASK;
        for (size_t probe = 0; probe < 32; ++probe) {
            size_t slot = (idx + probe) & ORDER_MASK;
            if (orders_table_[slot].order_ref == ref) {
                return &orders_table_[slot];
            }
            if (!orders_table_[slot].active && orders_table_[slot].order_ref == 0) {
                return nullptr;
            }
        }
        return nullptr;
    }

    PriceLevel bid_levels_[NUM_PRICE_SLOTS];
    PriceLevel ask_levels_[NUM_PRICE_SLOTS];
    StoredOrder orders_table_[MAX_ACTIVE_ORDERS];

    size_t best_bid_idx_{0};
    size_t best_ask_idx_{NUM_PRICE_SLOTS - 1};
    bool   has_bids_{false};
    bool   has_asks_{false};
};

/**
 * @brief Order Book Imbalance (OBI) Trading Strategy
 * 
 * Generates trading signals when top-10 level book imbalance exceeds thresholds:
 * - OBI > +0.35: Strong buying pressure -> Buy limit order placed at Best Ask
 * - OBI < -0.35: Strong selling pressure -> Sell limit order placed at Best Bid
 */
class OBIStrategy {
public:
    static constexpr double DEFAULT_BUY_THRESHOLD  = +0.30;
    static constexpr double DEFAULT_SELL_THRESHOLD = -0.30;
    static constexpr uint32_t DEFAULT_ORDER_QTY    = 100;

    explicit OBIStrategy(double buy_thresh = DEFAULT_BUY_THRESHOLD,
                         double sell_thresh = DEFAULT_SELL_THRESHOLD,
                         uint32_t order_qty = DEFAULT_ORDER_QTY) noexcept
        : buy_threshold_(buy_thresh),
          sell_threshold_(sell_thresh),
          order_qty_(order_qty),
          signal_counter_(0),
          last_signal_side_(' '),
          update_counter_(0),
          last_signal_update_(0) {}

    /**
     * @brief Evaluates current order book state and optionally generates a TradeSignal
     * @param book FlatOrderBook instance
     * @param t0 Ingress timestamp
     * @param t1 Current strategy timestamp
     * @param symbol Ticker symbol
     * @param out_signal Output trade signal
     * @return true if a signal was triggered, false otherwise
     */
    inline bool evaluate(const FlatOrderBook& book,
                         uint64_t t0,
                         uint64_t t1,
                         const char* symbol,
                         protocol::TradeSignal& out_signal) noexcept {
        if (!book.has_valid_market()) [[unlikely]] return false;

        update_counter_++;
        const auto depth = book.compute_top_depth();
        const double obi = depth.imbalance;

        if (obi >= buy_threshold_ && (last_signal_side_ != 'B' || (update_counter_ - last_signal_update_) >= 20)) {
            out_signal.t0_ingress_ns = t0;
            out_signal.t1_strategy_ns = t1;
            out_signal.signal_id = ++signal_counter_;
            out_signal.side = 'B';
            out_signal.price = book.best_ask_price(); // Take liquidity at best ask
            out_signal.shares = order_qty_;
            out_signal.imbalance = obi;
            std::memcpy(out_signal.symbol, symbol, 8);
            last_signal_side_ = 'B';
            last_signal_update_ = update_counter_;
            return true;
        } else if (obi <= sell_threshold_ && (last_signal_side_ != 'S' || (update_counter_ - last_signal_update_) >= 20)) {
            out_signal.t0_ingress_ns = t0;
            out_signal.t1_strategy_ns = t1;
            out_signal.signal_id = ++signal_counter_;
            out_signal.side = 'S';
            out_signal.price = book.best_bid_price(); // Take liquidity at best bid
            out_signal.shares = order_qty_;
            out_signal.imbalance = obi;
            std::memcpy(out_signal.symbol, symbol, 8);
            last_signal_side_ = 'S';
            last_signal_update_ = update_counter_;
            return true;
        }

        return false;
    }

    void reset() noexcept {
        last_signal_side_ = ' ';
        update_counter_ = 0;
        last_signal_update_ = 0;
    }

private:
    double   buy_threshold_;
    double   sell_threshold_;
    uint32_t order_qty_;
    uint64_t signal_counter_;
    char     last_signal_side_;
    uint64_t update_counter_;
    uint64_t last_signal_update_;
};

} // namespace hft::engine
