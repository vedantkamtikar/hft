#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <thread>
#include <iostream>
#include <cstdint>

namespace hft::pipeline {

/**
 * @brief Thread affinity and core isolation utilities for Windows
 */
class ThreadAffinity {
public:
    /**
     * @brief Pin the calling thread to a specific logical CPU core
     * @param core_id 0-indexed logical CPU core ID
     * @param set_realtime_priority Optionally set thread to TIME_CRITICAL priority
     * @return true on success, false otherwise
     */
    static inline bool pin_current_thread(size_t core_id, bool set_realtime_priority = true) noexcept {
        const size_t total_cores = std::thread::hardware_concurrency();
        const size_t target_core = (total_cores > 0) ? (core_id % total_cores) : 0;
        
        const DWORD_PTR mask = static_cast<DWORD_PTR>(1ULL << target_core);
        HANDLE thread_handle = GetCurrentThread();

        const DWORD_PTR prev_mask = SetThreadAffinityMask(thread_handle, mask);
        if (prev_mask == 0) {
            std::cerr << "[Affinity] Failed to pin thread to Core " << target_core 
                      << " (Error: " << GetLastError() << ")\n";
            return false;
        }

        if (set_realtime_priority) {
            SetThreadPriority(thread_handle, THREAD_PRIORITY_HIGHEST);
        }

        return true;
    }

    /**
     * @brief Pin an external std::thread to a specific logical CPU core
     * @param th std::thread instance
     * @param core_id 0-indexed logical CPU core ID
     */
    static inline bool pin_thread(std::thread& th, size_t core_id, bool set_realtime_priority = true) noexcept {
        const size_t total_cores = std::thread::hardware_concurrency();
        const size_t target_core = (total_cores > 0) ? (core_id % total_cores) : 0;
        const DWORD_PTR mask = static_cast<DWORD_PTR>(1ULL << target_core);

        HANDLE handle = reinterpret_cast<HANDLE>(th.native_handle());
        if (handle == nullptr || handle == INVALID_HANDLE_VALUE) {
            return false;
        }

        const DWORD_PTR prev = SetThreadAffinityMask(handle, mask);
        if (prev == 0) {
            return false;
        }

        if (set_realtime_priority) {
            SetThreadPriority(handle, THREAD_PRIORITY_HIGHEST);
        }

        return true;
    }
};

} // namespace hft::pipeline
