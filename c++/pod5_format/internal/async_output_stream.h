#pragma once

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/util/future.h>
#include <boost/thread/synchronized_value.hpp>

#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>

class AsyncOutputStream : public arrow::io::OutputStream {
public:
    AsyncOutputStream(std::shared_ptr<OutputStream> const& main_stream)
            : m_has_error(false),
              m_submitted_writes(0),
              m_completed_writes(0),
              m_submitted_byte_writes(0),
              m_completed_byte_writes(0),
              m_exit(false),
              m_main_stream(main_stream),
              m_write_thread([&] { run_write_thread(); }) {}

    ~AsyncOutputStream() {
        (void)Flush();
        m_exit = true;
        m_write_thread.join();
    }

    virtual arrow::Status Close() override {
        ARROW_RETURN_NOT_OK(Flush());
        m_exit = true;
        return m_main_stream->Close();
    }

    arrow::Future<> CloseAsync() override {
        ARROW_RETURN_NOT_OK(Flush());
        m_exit = true;
        return m_main_stream->CloseAsync();
    }

    arrow::Status Abort() override { return m_main_stream->Abort(); }

    arrow::Result<int64_t> Tell() const override { return m_submitted_byte_writes.load(); }

    bool closed() const override { return m_main_stream->closed(); }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> buffer, arrow::AllocateBuffer(nbytes));
        auto const char_data = static_cast<char const*>(data);
        std::copy(char_data, char_data + nbytes, buffer->mutable_data());
        return Write(buffer);
    }

    arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override {
        if (m_has_error) {
            return *m_error;
        }

        std::size_t const BUFFER_SIZE = 10 * 1024 * 1024;  // 10mb pending writes max
        while ((m_submitted_byte_writes - m_completed_byte_writes) > BUFFER_SIZE) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        m_submitted_byte_writes += data->size();

        std::lock_guard<std::mutex> lock(m_write_mutex);

        m_submitted_writes += 1;
        m_write_requests.push_back(data);
        m_work_available.notify_all();

        return arrow::Status::OK();
    }

    arrow::Status Flush() override {
        if (m_has_error) {
            return *m_error;
        }

        auto wait_for_write_count = m_submitted_writes.load();
        while (m_completed_writes.load() < wait_for_write_count) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        return m_main_stream->Flush();
    }

private:
    void run_write_thread() {
        while (!m_exit) {
            std::shared_ptr<arrow::Buffer> buffer;
            {
                std::unique_lock<std::mutex> lock(m_write_mutex);
                m_work_available.wait_for(lock, std::chrono::milliseconds(100),
                                          [&] { return !m_write_requests.empty() || m_exit; });
                if (!m_write_requests.size()) {
                    continue;
                }

                buffer = std::move(m_write_requests.front());
                m_write_requests.pop_front();
            }

            assert(buffer);

            auto result = m_main_stream->Write(buffer);
            m_completed_byte_writes += buffer->size();
            m_completed_writes += 1;
            if (!result.ok()) {
                m_error = result;
                m_has_error = true;
                break;
            }
        }

        std::unique_lock<std::mutex> lock(m_write_mutex);
        assert(m_write_requests.size() == 0);
    }

    std::mutex m_write_mutex;
    std::condition_variable m_work_available;
    std::deque<std::shared_ptr<arrow::Buffer>> m_write_requests;

    boost::synchronized_value<arrow::Status> m_error;
    std::atomic<bool> m_has_error;

    std::atomic<std::size_t> m_submitted_writes;
    std::atomic<std::size_t> m_completed_writes;
    std::atomic<std::size_t> m_submitted_byte_writes;
    std::atomic<std::size_t> m_completed_byte_writes;
    std::atomic<bool> m_exit;

    std::shared_ptr<OutputStream> m_main_stream;

    std::thread m_write_thread;
};
