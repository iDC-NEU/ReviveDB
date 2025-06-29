#pragma once

#include <emmintrin.h>

#include <atomic>
#include <string>

namespace NVMDB {

// Ticket spin locks are used in practice in NUMA systems. Although it has a higher overhead than standard
// spin locks, it scales better for NUMA systems.
// Why? Processes on the same NUMA node as the standard spinlock have an unfair advantage in obtaining the lock.
// Processes on remote NUMA nodes experience lock starvation and degraded performance.
class TicketSpinner {
public:
    TicketSpinner() : next_ticket_(0), now_serving_(0) {}

    void lock() {
        const auto ticket = next_ticket_.fetch_add(1);
        while (true) {
            auto ns = now_serving_.load();
            if (ns == ticket) {
                break;
            }
            // Back off for a number of iterations proportional to the "distance"
            // between my ticket and the currently-served ticket.
            for (size_t i = 0; i < ticket - ns; i++) {
                _mm_pause();
            }
        }
    }

    void unlock() { now_serving_.store(now_serving_.load() + 1); }

private:
    std::atomic<size_t> next_ticket_;
    std::atomic<size_t> now_serving_;
};

// Spinlock implementation for x86 using exponential backoff
template<int max_backoff = 1 << 4, int min_backoff = 1 << 2>
class PassiveSpinner {
private:
    std::atomic<bool> _locked;

public:
    PassiveSpinner() : _locked{false} {}
    PassiveSpinner(const PassiveSpinner&) = delete;
    PassiveSpinner& operator=(const PassiveSpinner&) = delete;

    void lock() {
        int backoff_count = min_backoff;

        while (true) {
            // Return immediately if locked was already false
            if (!_locked.exchange(true)) {
                return;
            }

            // Wait on a read-only copy which gets cached locally to avoid
            // cache invalidation among other waiting threads.
            // Use exponential backoff (to reduce reads)
            do {
                for (int i = 0; i < backoff_count; ++i) {
                    _mm_pause();    // Intel x86 intrinsic to add a delay
                }

                backoff_count = std::min(backoff_count << 1, max_backoff);
            } while (_locked.load());
        }
    }

    void unlock() {
        _locked.store(false);
    }

    [[nodiscard]] bool try_lock() {
        // Attempt to set the lock to true if it is currently false
        bool expected = false;
        return _locked.compare_exchange_strong(expected, true);
    }
};

using PassiveSpinner42 = PassiveSpinner<1 << 4, 1 << 2>;

class SpinLock {
private:
    std::atomic<bool> m_locked;

public:
    SpinLock() : m_locked(false) {}

    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;

    void lock() {
        bool expected = false;
        while (!m_locked.compare_exchange_weak(expected, true,
                                               std::memory_order_acquire,
                                               std::memory_order_relaxed)) {
            expected = false;

#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();  // 在x86架构上使用PAUSE指令
#else
            std::this_thread::yield(); // 在其他架构上让出时间片
#endif
        }
    }

    bool try_lock() {
        bool expected = false;
        return m_locked.compare_exchange_strong(expected, true,
                                                std::memory_order_acquire,
                                                std::memory_order_relaxed);
    }

    void unlock() {
        m_locked.store(false, std::memory_order_release);
    }
};

class ActiveSpinner {
public:
    ActiveSpinner() : lock_bit_(false) {}
    ActiveSpinner(const ActiveSpinner&) = delete;
    ActiveSpinner& operator=(const ActiveSpinner&) = delete;

    void lock() {

        // The load part of the exchange() should synchronise with the store()
        // in the Unlock() method.
        // Therefore, we can use a release-store and acquire-load synchronisation
        // such that the "load/read" part of the exchange() RMW is acquire and the
        // "store/write" part of the RMW is relaxed.
        while (lock_bit_.exchange(true, std::memory_order_acquire)) {
            do {
                for (volatile size_t i = 0; i < 100; i++) {
                }
                // The locally spinning load can be relaxed, since it does not have
                // to synchronise with other operations, it is just used a heuristic
                // to avoid repeated RMWs very quickly.
            } while (lock_bit_.load(std::memory_order_relaxed));
        }
    }

    void unlock() {
        // Synchronise with the load of the exchange() by releasing/committing
        // our memory to the atomic location.
        lock_bit_.store(false, std::memory_order_release);
    }

private:
    std::atomic<bool> lock_bit_;
};

}