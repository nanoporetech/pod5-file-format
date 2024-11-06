#include "pod5_format/thread_pool.h"

#include <catch2/catch.hpp>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

TEST_CASE("Thread pool runs tasks concurrently", "[thread_pool]")
{
    using namespace std::chrono_literals;

    auto const explicit_stop = GENERATE(true, false);
    CAPTURE(explicit_stop);

    auto const use_strands = GENERATE(true, false);
    CAPTURE(use_strands);

    // semaphores only in std lib in c++20, so fake them
    std::mutex sem_mutex;
    int sem1 = 2;
    std::condition_variable cv1;
    int sem2 = 2;
    std::condition_variable cv2;

    auto const create_task = [&]() -> std::function<void()> {
        return [&] {
            std::unique_lock<std::mutex> l{sem_mutex};
            sem1--;
            if (sem1 > 0) {
                cv1.wait(l, [&] { return sem1 == 0; });
            } else {
                l.unlock();
                cv1.notify_all();
                std::this_thread::sleep_for(1ms);
                l.lock();
            }

            sem2--;
            if (sem2 > 0) {
                cv2.wait(l, [&] { return sem2 == 0; });
            } else {
                l.unlock();
                cv2.notify_all();
            }
        };
    };

    auto thread_pool = pod5::make_thread_pool(2);
    std::shared_ptr<pod5::ThreadPoolStrand> strands[2];
    if (use_strands) {
        for (unsigned i = 0; i < 2; ++i) {
            strands[i] = thread_pool->create_strand();
            strands[i]->post(create_task());
        }
    } else {
        thread_pool->post(create_task());
        thread_pool->post(create_task());
    }

    if (explicit_stop) {
        thread_pool->stop_and_drain();
    } else {
        thread_pool.reset();
        for (unsigned i = 0; i < 2; ++i) {
            strands[i].reset();
        }
    }

    REQUIRE(sem1 == 0);
    REQUIRE(sem2 == 0);
}

TEST_CASE("Tasks on the same strand are serialised", "[thread_pool]")
{
    using namespace std::chrono_literals;

    auto const explicit_stop = GENERATE(true, false);
    CAPTURE(explicit_stop);

    std::mutex seq_mutex;
    std::vector<int> seq;
    seq.reserve(4);

    auto const create_task = [&](int const num) -> std::function<void()> {
        return [&, num] {
            {
                std::lock_guard<std::mutex> l{seq_mutex};
                seq.push_back(num);
            }
            std::this_thread::sleep_for(50ms);
            {
                std::lock_guard<std::mutex> l{seq_mutex};
                seq.push_back(num);
            }
        };
    };

    auto thread_pool = pod5::make_thread_pool(2);
    auto strand = thread_pool->create_strand();
    strand->post(create_task(0));
    strand->post(create_task(1));

    if (explicit_stop) {
        thread_pool->stop_and_drain();
    } else {
        thread_pool.reset();
        strand.reset();
    }

    REQUIRE(seq.size() == 4);
    if (seq[0] == 0) {
        REQUIRE(seq == (std::vector<int>{0, 0, 1, 1}));
    } else {
        REQUIRE(seq == (std::vector<int>{1, 1, 0, 0}));
    }
}
