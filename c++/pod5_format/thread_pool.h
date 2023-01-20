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
};

POD5_FORMAT_EXPORT std::shared_ptr<ThreadPool> make_thread_pool(std::size_t worker_threads);
}  // namespace pod5
