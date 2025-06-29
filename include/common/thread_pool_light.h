#pragma once

#include <atomic>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "common/mpmc_queue.h"
#include "common/lightweight_semaphore.h"

namespace util {

class [[nodiscard]] thread_pool_light {
public:
    /**
         * @brief Construct a new thread pool.
         *
         * @param thread_count_ The number of threads to use.
         * The default value is the total number of hardware threads available,
         * as reported by the implementation. This is usually determined by the number of cores in the CPU.
         * If a core is hyperthreaded, it will count as two threads.
     */
    explicit thread_pool_light(size_t thread_count_ = std::thread::hardware_concurrency(), std::string worker_name_ = "thread_pool")
        : thread_count(thread_count_),
          worker_name(std::move(worker_name_)),
          sema(0, 0),
          tasks(thread_count_ * 10),
          threads(std::make_unique<std::thread[]>(thread_count_)) {
        create_threads();
    }

    /**
         * @brief Destruct the thread pool. Waits for all tasks to complete, then destroys all threads.
     */
    ~thread_pool_light() {
        destroy_threads();
    }

    /**
         * @brief Get the number of threads in the pool.
         *
         * @return The number of threads.
     */
    [[nodiscard]] size_t get_thread_count() const { return thread_count; }

    /**
         * @brief Parallelize a loop by automatically splitting it into blocks and submitting each block separately to the queue. The user must use wait_for_tasks() or some other method to ensure that the loop finishes executing, otherwise bad things will happen.
         *
         * @tparam F The type of the function to loop through.
         * @tparam T1 The type of the first index in the loop. Should be a signed or unsigned integer.
         * @tparam T2 The type of the index after the last index in the loop. Should be a signed or unsigned integer. If T1 is not the same as T2, a common type will be automatically inferred.
         * @tparam T The common type of T1 and T2.
         * @param first_index The first index in the loop.
         * @param index_after_last The index after the last index in the loop. The loop will iterate from first_index to (index_after_last - 1) inclusive. In other words, it will be equivalent to "for (T i = first_index; i < index_after_last; ++i)". Note that if index_after_last == first_index, no blocks will be submitted.
         * @param loop The function to loop through. Will be called once per block. Should take exactly two arguments: the first index in the block and the index after the last index in the block. loop(start, end) should typically involve a loop of the form "for (T i = start; i < end; ++i)".
         * @param num_blocks The maximum number of blocks to split the loop into. The default is to use the number of threads in the pool.
     */
    template <typename F, typename T1, typename T2, typename T = std::common_type_t<T1, T2>>
    void push_loop(T1 first_index_, T2 index_after_last_, F&& loop, size_t num_blocks = 0) {
        T first_index = static_cast<T>(first_index_);
        T index_after_last = static_cast<T>(index_after_last_);
        if (num_blocks == 0)
            num_blocks = thread_count;
        if (index_after_last < first_index)
            std::swap(index_after_last, first_index);
        auto total_size = static_cast<size_t>(index_after_last - first_index);
        auto block_size = static_cast<size_t>(total_size / num_blocks);
        if (block_size == 0) {
            block_size = 1;
            num_blocks = (total_size > 1) ? total_size : 1;
        }
        if (total_size > 0) {
            for (size_t i = 0; i < num_blocks; ++i)
                push_task(std::forward<F>(loop), static_cast<T>(i * block_size) + first_index, (i == num_blocks - 1) ? index_after_last : (static_cast<T>((i + 1) * block_size) + first_index));
        }
    }

    /**
         * @brief Parallelize a loop by automatically splitting it into blocks and submitting each block separately to the queue. The user must use wait_for_tasks() or some other method to ensure that the loop finishes executing, otherwise bad things will happen. This overload is used for the special case where the first index is 0.
         *
         * @tparam F The type of the function to loop through.
         * @tparam T The type of the loop indices. Should be a signed or unsigned integer.
         * @param index_after_last The index after the last index in the loop. The loop will iterate from 0 to (index_after_last - 1) inclusive. In other words, it will be equivalent to "for (T i = 0; i < index_after_last; ++i)". Note that if index_after_last == 0, no blocks will be submitted.
         * @param loop The function to loop through. Will be called once per block. Should take exactly two arguments: the first index in the block and the index after the last index in the block. loop(start, end) should typically involve a loop of the form "for (T i = start; i < end; ++i)".
         * @param num_blocks The maximum number of blocks to split the loop into. The default is to use the number of threads in the pool.
     */
    template <typename F, typename T>
    inline void push_loop(const T index_after_last, F&& loop, const size_t num_blocks = 0) {
        push_loop(0, index_after_last, std::forward<F>(loop), num_blocks);
    }

    /**
         * @brief Push a function with zero or more arguments, but no return value, into the task queue. Does not return a future, so the user must use wait_for_tasks() or some other method to ensure that the task finishes executing, otherwise bad things will happen.
         *
         * @tparam F The type of the function.
         * @tparam A The types of the arguments.
         * @param task The function to push.
         * @param args The zero or more arguments to pass to the function. Note that if the task is a class member function, the first argument must be a pointer to the object, i.e. &object (or this), followed by the actual arguments.
     */
    template <typename F, typename... A>
    inline void push_task(F&& task, A&&... args) {
        std::function<void()> task_function = std::bind(std::forward<F>(task), std::forward<A>(args)...);
        tasks.push(std::move(task_function));
        sema.signal();
    }

private:
    void create_threads() {
        running.store(true, std::memory_order_release);
        for (size_t i = 0; i < thread_count; ++i) {
            threads[i] = std::thread(&thread_pool_light::worker, this);
        }
    }

    void destroy_threads() {
        running.store(false, std::memory_order_release);
        sema.signal((ssize_t)thread_count);
        for (size_t i = 0; i < thread_count; ++i) {
            threads[i].join();
        }
    }

    void worker() {
        pthread_setname_np(pthread_self(), worker_name.c_str());
        std::function<void()> task;
        while (running.load(std::memory_order_relaxed)) {
            // wait a task
            while (!sema.wait());
            // get the task
            while (running.load(std::memory_order_relaxed) && !tasks.try_pop(task));
            // if sema is emitted by destructor
            if (!running.load(std::memory_order_acquire)) {
                return;
            }
            // apply the task
            task();
        }
    }

    const size_t thread_count{};

    const std::string worker_name;

    std::atomic<bool> running = {false};

    moodycamel::LightweightSemaphore sema;

    rigtorp::MPMCQueue<std::function<void()>> tasks;

    std::unique_ptr<std::thread[]> threads = nullptr;
};

}