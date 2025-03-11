#pragma once

#include "pod5_format/pod5_format_export.h"

#include <functional>
#include <memory>

namespace pod5 {

class POD5_FORMAT_EXPORT ThreadPoolStrand {
public:
    virtual ~ThreadPoolStrand() = default;
    virtual void post(std::function<void()> callback) = 0;
};

class POD5_FORMAT_EXPORT ThreadPool {
public:
    virtual ~ThreadPool() = default;
    virtual std::shared_ptr<ThreadPoolStrand> create_strand() = 0;
    virtual void post(std::function<void()> callback) = 0;
    /// Stops the thread pool and drains all active work.
    ///
    /// Further calls to create_strand() or post() (including on an existing strand created from
    /// this pool) will throw.
    virtual void stop_and_drain() = 0;

    /// Waits for the threads to process all posted work.
    virtual void wait_for_drain() = 0;
};

POD5_FORMAT_EXPORT std::shared_ptr<ThreadPool> make_thread_pool(std::size_t worker_threads);
}  // namespace pod5
