#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

namespace hft::queue {

constexpr size_t CACHE_LINE_SIZE = 64;

/**
 * @brief Lock-free Single-Producer Single-Consumer (SPSC) Ring Buffer
 * 
 * Engineered for sub-microsecond tick-to-trade critical paths:
 * - Cache-aligned (64 bytes) to eliminate false sharing between producer and consumer cores.
 * - Bitmask-based modular arithmetic (Capacity must be a power of two).
 * - Zero dynamic heap allocation: flat internal buffer.
 * - Minimal atomic synchronization using acquire/release memory semantics with cached remote indices.
 */
template <typename T, size_t Capacity>
class alignas(CACHE_LINE_SIZE) SPSCQueue {
    static_assert((Capacity != 0) && ((Capacity & (Capacity - 1)) == 0),
                  "Capacity must be a power of two!");
    static_assert(std::is_trivially_copyable_v<T> || std::is_nothrow_move_constructible_v<T>,
                  "Type T must be trivially copyable or nothrow move constructible");

public:
    static constexpr size_t MASK = Capacity - 1;

    SPSCQueue() noexcept
        : write_idx_(0),
          cached_read_idx_(0),
          read_idx_(0),
          cached_write_idx_(0) {}

    ~SPSCQueue() = default;

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;
    SPSCQueue(SPSCQueue&&) = delete;
    SPSCQueue& operator=(SPSCQueue&&) = delete;

    /**
     * @brief Push an element into the queue (Producer thread only)
     * @return true if pushed, false if queue is full
     */
    [[nodiscard]] inline bool push(const T& item) noexcept {
        const size_t current_write = write_idx_.load(std::memory_order_relaxed);
        
        // Fast path check against cached read index
        if ((current_write - cached_read_idx_) >= Capacity) {
            // Refresh cached read index
            cached_read_idx_ = read_idx_.load(std::memory_order_acquire);
            if ((current_write - cached_read_idx_) >= Capacity) {
                return false; // Queue is full
            }
        }

        buffer_[current_write & MASK] = item;
        write_idx_.store(current_write + 1, std::memory_order_release);
        return true;
    }

    /**
     * @brief Emplace an element into the queue in-place (Producer thread only)
     */
    template <typename... Args>
    [[nodiscard]] inline bool emplace(Args&&... args) noexcept {
        const size_t current_write = write_idx_.load(std::memory_order_relaxed);

        if ((current_write - cached_read_idx_) >= Capacity) {
            cached_read_idx_ = read_idx_.load(std::memory_order_acquire);
            if ((current_write - cached_read_idx_) >= Capacity) {
                return false;
            }
        }

        buffer_[current_write & MASK] = T(std::forward<Args>(args)...);
        write_idx_.store(current_write + 1, std::memory_order_release);
        return true;
    }

    /**
     * @brief Pop an element from the queue (Consumer thread only)
     * @return true if popped, false if queue is empty
     */
    [[nodiscard]] inline bool pop(T& item) noexcept {
        const size_t current_read = read_idx_.load(std::memory_order_relaxed);

        // Fast path check against cached write index
        if (current_read == cached_write_idx_) {
            // Refresh cached write index
            cached_write_idx_ = write_idx_.load(std::memory_order_acquire);
            if (current_read == cached_write_idx_) {
                return false; // Queue is empty
            }
        }

        item = buffer_[current_read & MASK];
        read_idx_.store(current_read + 1, std::memory_order_release);
        return true;
    }

    /**
     * @brief Direct pointer access to top element (Consumer thread only)
     * @return pointer to element if available, nullptr if empty
     */
    [[nodiscard]] inline T* front() noexcept {
        const size_t current_read = read_idx_.load(std::memory_order_relaxed);

        if (current_read == cached_write_idx_) {
            cached_write_idx_ = write_idx_.load(std::memory_order_acquire);
            if (current_read == cached_write_idx_) {
                return nullptr;
            }
        }

        return &buffer_[current_read & MASK];
    }

    /**
     * @brief Pop current element after front() inspection (Consumer thread only)
     */
    inline void pop() noexcept {
        const size_t current_read = read_idx_.load(std::memory_order_relaxed);
        read_idx_.store(current_read + 1, std::memory_order_release);
    }

    [[nodiscard]] inline size_t size() const noexcept {
        const size_t w = write_idx_.load(std::memory_order_relaxed);
        const size_t r = read_idx_.load(std::memory_order_relaxed);
        return (w >= r) ? (w - r) : 0;
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return read_idx_.load(std::memory_order_relaxed) == write_idx_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] inline bool full() const noexcept {
        return size() >= Capacity;
    }

    [[nodiscard]] static constexpr size_t capacity() noexcept {
        return Capacity;
    }

private:
    // Producer variables (written only by Producer)
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> write_idx_{0};
    alignas(CACHE_LINE_SIZE) size_t cached_read_idx_{0};

    // Consumer variables (written only by Consumer)
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> read_idx_{0};
    alignas(CACHE_LINE_SIZE) size_t cached_write_idx_{0};

    // Flat pre-allocated buffer
    alignas(CACHE_LINE_SIZE) T buffer_[Capacity];
};

} // namespace hft::queue
