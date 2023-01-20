#pragma once

#include "pod5_format/thread_pool.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/util/future.h>
#include <boost/thread/synchronized_value.hpp>

#include <condition_variable>
#include <deque>
#include <iostream>
#include <thread>

namespace pod5 {

class AsyncOutputStream : public arrow::io::OutputStream {
public:
    AsyncOutputStream(
        std::shared_ptr<OutputStream> const & main_stream,
        std::shared_ptr<ThreadPool> const & thread_pool)
    : m_has_error(false)
    , m_submitted_writes(0)
    , m_completed_writes(0)
    , m_submitted_byte_writes(0)
    , m_completed_byte_writes(0)
    , m_main_stream(main_stream)
    , m_strand(thread_pool->create_strand())
    {
    }

    ~AsyncOutputStream()
    {
        // Flush ensures all in flight async writes are completed.
        (void)Flush();
    }

    virtual arrow::Status Close() override
    {
        ARROW_RETURN_NOT_OK(Flush());
        return m_main_stream->Close();
    }

    arrow::Future<> CloseAsync() override
    {
        ARROW_RETURN_NOT_OK(Flush());
        return m_main_stream->CloseAsync();
    }

    arrow::Status Abort() override { return m_main_stream->Abort(); }

    arrow::Result<int64_t> Tell() const override { return m_submitted_byte_writes.load(); }

    bool closed() const override { return m_main_stream->closed(); }

    arrow::Status Write(void const * data, int64_t nbytes) override
    {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> buffer, arrow::AllocateBuffer(nbytes));
        auto const char_data = static_cast<char const *>(data);
        std::copy(char_data, char_data + nbytes, buffer->mutable_data());
        return Write(buffer);
    }

    arrow::Status Write(std::shared_ptr<arrow::Buffer> const & data) override
    {
        if (m_has_error) {
            return *m_error;
        }

        std::size_t const BUFFER_SIZE = 10 * 1024 * 1024;  // 10mb pending writes max
        while ((m_submitted_byte_writes - m_completed_byte_writes) > BUFFER_SIZE) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        m_submitted_byte_writes += data->size();

        m_submitted_writes += 1;
        m_strand->post([&, data] {
            if (m_has_error) {
                return;
            }
            auto result = m_main_stream->Write(data);
            m_completed_byte_writes += data->size();
            if (!result.ok()) {
                m_error = result;
                m_has_error = true;
            }

            // Ensure we do this after editing all the other members, in order to prevent `Flush`
            // returning until we are done.
            m_completed_writes += 1;
        });

        return arrow::Status::OK();
    }

    arrow::Status Flush() override
    {
        // Wait for our completed writes to match our submitted writes,
        // this guarantees our async operations are finished.
        auto wait_for_write_count = m_submitted_writes.load();
        while (m_completed_writes.load() < wait_for_write_count && !m_has_error) {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }

        if (m_has_error) {
            return *m_error;
        }

        return m_main_stream->Flush();
    }

private:
    boost::synchronized_value<arrow::Status> m_error;
    std::atomic<bool> m_has_error;

    std::atomic<std::size_t> m_submitted_writes;
    std::atomic<std::size_t> m_completed_writes;
    std::atomic<std::size_t> m_submitted_byte_writes;
    std::atomic<std::size_t> m_completed_byte_writes;

    std::shared_ptr<OutputStream> m_main_stream;
    std::shared_ptr<ThreadPoolStrand> m_strand;
};

}  // namespace pod5
