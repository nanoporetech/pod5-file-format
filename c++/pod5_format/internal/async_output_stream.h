#pragma once

#include "pod5_format/file_output_stream.h"
#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/thread_pool.h"

#include <arrow/buffer.h>
#include <arrow/util/future.h>
#include <boost/thread/synchronized_value.hpp>
#include <gsl/gsl-lite.hpp>

#include <condition_variable>
#include <deque>
#include <thread>

namespace pod5 {

class AsyncOutputStream : public FileOutputStream {
    struct PrivateDummy {};

public:
    static arrow::Result<std::shared_ptr<AsyncOutputStream>> make(
        std::shared_ptr<OutputStream> const & main_stream,
        std::shared_ptr<ThreadPool> const & thread_pool,
        arrow::MemoryPool * memory_pool = arrow::default_memory_pool())
    {
        return std::make_shared<AsyncOutputStream>(
            main_stream, thread_pool, memory_pool, PrivateDummy{});
    }

    ~AsyncOutputStream() { (void)Close(); }

    arrow::Status Close() override
    {
        // flush all output
        ARROW_RETURN_NOT_OK(Flush());

        // truncate excess data
        ARROW_RETURN_NOT_OK(truncate_file());

        // and close stream
        return m_main_stream->Close();
    }

    arrow::Future<> CloseAsync() override
    {
        // flush all output
        ARROW_RETURN_NOT_OK(Flush());

        // truncate excess data
        ARROW_RETURN_NOT_OK(truncate_file());

        // and close stream
        return m_main_stream->CloseAsync();
    }

    arrow::Status Abort() override { return m_main_stream->Abort(); }

    arrow::Result<int64_t> Tell() const override
    {
        return m_actual_bytes_written - m_file_start_offset;
    }

    bool closed() const override { return m_main_stream->closed(); }

    arrow::Status Write(void const * data, int64_t nbytes) override
    {
        POD5_TRACE_FUNCTION();
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Buffer> buffer, arrow::AllocateBuffer(nbytes, m_memory_pool));
        auto const char_data = static_cast<std::uint8_t const *>(data);
        std::copy(char_data, char_data + nbytes, buffer->mutable_data());
        return Write(buffer);
    }

    arrow::Status Write(std::shared_ptr<arrow::Buffer> const & data) override
    {
        POD5_TRACE_FUNCTION();
        if (m_has_error) {
            return *m_error;
        }

        std::size_t const BUFFER_SIZE = 10 * 1024 * 1024;  // 10mb pending writes max
        while ((m_submitted_byte_writes - m_completed_byte_writes) > BUFFER_SIZE) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        m_submitted_byte_writes += data->size();
        m_actual_bytes_written += data->size();

        m_submitted_writes += 1;
        m_strand->post([&, data] {
            POD5_TRACE_FUNCTION();
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
        POD5_TRACE_FUNCTION();
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

    int get_file_descriptor() const
    {
        return static_cast<arrow::io::FileOutputStream *>(m_main_stream.get())->file_descriptor();
    }

    void set_file_start_offset(std::size_t val) override { m_file_start_offset = val; }

    AsyncOutputStream(
        std::shared_ptr<OutputStream> const & main_stream,
        std::shared_ptr<ThreadPool> const & thread_pool,
        arrow::MemoryPool * memory_pool,
        PrivateDummy)
    : m_has_error{false}
    , m_submitted_writes{0}
    , m_completed_writes{0}
    , m_submitted_byte_writes{0}
    , m_completed_byte_writes{0}
    , m_actual_bytes_written{0}
    , m_main_stream{main_stream}
    , m_file_start_offset{0}
    , m_strand{thread_pool->create_strand()}
    , m_memory_pool(memory_pool)
    {
    }

protected:
    virtual arrow::Status write_final_chunk() { return arrow::Status::OK(); }

    virtual arrow::Status write_cache() { return arrow::Status::OK(); }

    virtual arrow::Status truncate_file() { return arrow::Status::OK(); }

    arrow::MemoryPool * memory_pool() { return m_memory_pool; }

    boost::synchronized_value<arrow::Status> m_error;
    std::atomic<bool> m_has_error;

    std::atomic<std::size_t> m_submitted_writes;
    std::atomic<std::size_t> m_completed_writes;
    std::atomic<std::size_t> m_submitted_byte_writes;
    std::atomic<std::size_t> m_completed_byte_writes;
    // this represents the number of data bytes written (excluding any padding for alignment)
    // used for truncating the file for instance
    std::int64_t m_actual_bytes_written;

    std::shared_ptr<OutputStream> m_main_stream;

private:
    std::size_t m_file_start_offset;
    std::shared_ptr<ThreadPoolStrand> m_strand;
    arrow::MemoryPool * m_memory_pool;
};

}  // namespace pod5
