#pragma once

#include <cstdint>
#include <vector>
#include <algorithm>
#include <cmath>
#include <numeric>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

namespace hft::telemetry {

/**
 * @brief High-resolution timer for nanosecond-level benchmarking on Windows
 */
class HighResTimer {
public:
    HighResTimer() noexcept {
        LARGE_INTEGER freq;
        QueryPerformanceFrequency(&freq);
        frequency_ = freq.QuadPart;
    }

    [[nodiscard]] inline uint64_t now_ns() const noexcept {
        LARGE_INTEGER counter;
        QueryPerformanceCounter(&counter);
        return static_cast<uint64_t>((counter.QuadPart * 1'000'000'000ULL) / frequency_);
    }

    [[nodiscard]] inline uint64_t now_ticks() const noexcept {
        LARGE_INTEGER counter;
        QueryPerformanceCounter(&counter);
        return counter.QuadPart;
    }

    [[nodiscard]] inline uint64_t ticks_to_ns(uint64_t ticks) const noexcept {
        return static_cast<uint64_t>((ticks * 1'000'000'000ULL) / frequency_);
    }

private:
    int64_t frequency_{1};
};

/**
 * @brief Zero-allocation lock-free latency recorder
 */
template <size_t MaxSamples = 200'000>
class LatencyAggregator {
public:
    LatencyAggregator() noexcept : sample_count_(0) {}

    inline void record(uint64_t latency_ns) noexcept {
        const size_t idx = sample_count_.fetch_add(1, std::memory_order_relaxed);
        if (idx < MaxSamples) {
            samples_[idx] = latency_ns;
        }
    }

    struct LatencyReport {
        size_t count{0};
        double min_ns{0.0};
        double max_ns{0.0};
        double mean_ns{0.0};
        double p50_ns{0.0};
        double p90_ns{0.0};
        double p99_ns{0.0};
        double p99_9_ns{0.0};
        double p99_99_ns{0.0};
    };

    LatencyReport compute_report() const {
        LatencyReport rep{};
        const size_t total = std::min(sample_count_.load(std::memory_order_relaxed), MaxSamples);
        if (total == 0) return rep;

        std::vector<uint64_t> sorted_samples(samples_, samples_ + total);
        std::sort(sorted_samples.begin(), sorted_samples.end());

        rep.count = total;
        rep.min_ns = static_cast<double>(sorted_samples.front());
        rep.max_ns = static_cast<double>(sorted_samples.back());

        const double sum = std::accumulate(sorted_samples.begin(), sorted_samples.end(), 0.0);
        rep.mean_ns = sum / static_cast<double>(total);

        rep.p50_ns = get_percentile(sorted_samples, 0.50);
        rep.p90_ns = get_percentile(sorted_samples, 0.90);
        rep.p99_ns = get_percentile(sorted_samples, 0.99);
        rep.p99_9_ns = get_percentile(sorted_samples, 0.999);
        rep.p99_99_ns = get_percentile(sorted_samples, 0.9999);

        return rep;
    }

    void reset() noexcept {
        sample_count_.store(0, std::memory_order_relaxed);
    }

private:
    static double get_percentile(const std::vector<uint64_t>& sorted, double p) {
        if (sorted.empty()) return 0.0;
        const size_t idx = static_cast<size_t>(std::ceil(p * sorted.size())) - 1;
        return static_cast<double>(sorted[std::min(idx, sorted.size() - 1)]);
    }

    std::atomic<size_t> sample_count_;
    uint64_t samples_[MaxSamples];
};

} // namespace hft::telemetry
