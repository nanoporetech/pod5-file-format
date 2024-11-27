#include "pod5_format/thread_pool.h"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace pod5 {

namespace {

class ThreadPoolImpl : public ThreadPool, public std::enable_shared_from_this<ThreadPoolImpl> {
public:
    ThreadPoolImpl(std::size_t worker_count)
    {
        assert(worker_count > 0);
        for (std::size_t i = 0; i < std::max<std::size_t>(1, worker_count); ++i) {
            m_threads.emplace_back([&] { run_thread(); });
        }
    }

    ~ThreadPoolImpl() { stop_and_drain(); }

    void run_thread()
    {
        bool keep_alive = true;
        std::optional<WorkItem> work;

        while (keep_alive) {
            {
                std::unique_lock<std::mutex> lock{m_work_mutex};

                if (work) {
                    if (work->strand_id != NO_STRAND) {
                        m_busy_strands[work->strand_id] = false;
                    }
                    work = std::nullopt;
                }

                // find the first piece of work whose strand isn't already busy
                for (auto it = m_work.begin(); it != m_work.end(); ++it) {
                    if (it->strand_id == NO_STRAND || !m_busy_strands.at(it->strand_id)) {
                        if (it->strand_id != NO_STRAND) {
                            m_busy_strands[it->strand_id] = true;
                        }
                        work = std::move(*it);
                        m_work.erase(it);
                        break;
                    }
                }

                if (!work) {
                    if (m_keep_alive) {
                        m_work_ready.wait(lock);
                        keep_alive = m_keep_alive || !m_work.empty();
                    } else {
                        // If there wasn't any work for us to pick up, any remaining work must be
                        // for strands with running tasks (in which case the workers handling those
                        // tasks will pick them up. This will work because once a task finishes, the
                        // worker will check for work *before* checking m_keep_alive. Thus it's safe
                        // for *this* worker to exit.
                        keep_alive = false;
                    }
                    continue;
                }
            }
            assert(work);

            if (work->callback) {
                work->callback();
            }
        }
    }

    void post(std::function<void()> callback) override
    {
        {
            std::lock_guard<std::mutex> l{m_work_mutex};
            if (!m_keep_alive) {
                throw std::logic_error{"ThreadPool: post() called after stop_and_drain()"};
            }
            m_work.emplace_back(WorkItem{std::move(callback), NO_STRAND});
        }

        m_work_ready.notify_one();
    }

    void post(std::function<void()> callback, uint64_t const strand_id)
    {
        assert(strand_id != NO_STRAND);

        std::lock_guard<std::mutex> l{m_work_mutex};
        if (!m_keep_alive) {
            throw std::logic_error{"ThreadPool: post() called after stop_and_drain()"};
        }
        if (m_busy_strands.size() <= strand_id) {
            m_busy_strands.resize(strand_id + 1);
        }
        m_work.emplace_back(WorkItem{std::move(callback), strand_id});

        // only send a wakeup if the strand isn't already busy - it's generally more efficient to do
        // this outside the lock, but the conditional makes that hard to reason about
        if (!m_busy_strands.at(strand_id)) {
            m_work_ready.notify_one();
        }
    }

    void stop_and_drain() override
    {
        {
            std::lock_guard<std::mutex> lock{m_work_mutex};
            m_keep_alive = false;
        }
        m_work_ready.notify_all();
        for (auto & thread : m_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        assert(m_work.empty());
    }

    std::shared_ptr<ThreadPoolStrand> create_strand() override;

private:
    struct WorkItem {
        std::function<void()> callback;
        uint64_t strand_id;

        explicit operator bool() const { return !!callback; }
    };

    static constexpr uint64_t NO_STRAND = UINT64_MAX;

    std::mutex m_work_mutex;
    bool m_keep_alive{true};
    std::condition_variable m_work_ready;
    std::deque<WorkItem> m_work;
    std::vector<bool> m_busy_strands;

    std::atomic<uint64_t> m_next_strand_id{0};
    std::vector<std::thread> m_threads;
};

class StrandImpl : public ThreadPoolStrand {
public:
    StrandImpl(std::shared_ptr<ThreadPoolImpl> pool, uint64_t const strand_id)
    : m_pool(std::move(pool))
    , m_strand_id(strand_id)
    {
    }

    void post(std::function<void()> callback) override
    {
        m_pool->post(std::move(callback), m_strand_id);
    }

    std::shared_ptr<ThreadPoolImpl> m_pool;
    uint64_t m_strand_id;
};

std::shared_ptr<ThreadPoolStrand> ThreadPoolImpl::create_strand()
{
    uint64_t strand_id;
    {
        std::lock_guard<std::mutex> l{m_work_mutex};
        if (!m_keep_alive) {
            throw std::logic_error{"ThreadPool: create_strand() called after stop_and_drain()"};
        }
        strand_id = m_next_strand_id++;
    }
    return std::make_shared<StrandImpl>(shared_from_this(), strand_id);
}
}  // namespace

std::shared_ptr<ThreadPool> make_thread_pool(std::size_t worker_threads)
{
    return std::make_shared<ThreadPoolImpl>(worker_threads);
}

}  // namespace pod5
