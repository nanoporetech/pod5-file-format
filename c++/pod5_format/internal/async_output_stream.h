#pragma once

#include "pod5_format/file_output_stream.h"
#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/thread_pool.h"

#include <arrow/buffer.h>
#include <arrow/util/future.h>
#include <gsl/gsl-lite.hpp>

#include <cassert>
#include <condition_variable>
#include <deque>
#include <thread>

namespace pod5 {

class AsyncOutputStream : public FileOutputStream {
    struct PrivateDummy {};

public:
    static arrow::Result<std::shared_ptr<AsyncOutputStream>> make(
        std::string const & file_path,
        std::shared_ptr<ThreadPool> const & thread_pool,
        bool flush_on_batch_complete,
        arrow::MemoryPool * memory_pool = arrow::default_memory_pool(),
        bool keep_file_open = true)
    {
        return std::make_shared<AsyncOutputStream>(
            file_path,
            thread_pool,
            flush_on_batch_complete,
            memory_pool,
            keep_file_open,
            PrivateDummy{});
    }

    ~AsyncOutputStream() { (void)Close(); }

    arrow::Status Close() override
    {
        // flush all output
        ARROW_RETURN_NOT_OK(Flush());

        // and close stream
        std::lock_guard<std::mutex> l{m_file_handle_mutex};
        if (m_file_handle) {
            fclose(m_file_handle);
            m_file_handle = nullptr;
        }
        return arrow::Status::OK();
    }

    arrow::Future<> CloseAsync() override
    {
        ARROW_RETURN_NOT_OK(Close());
        return FileOutputStream::CloseAsync();
    }

    arrow::Status Abort() override
    {
        std::lock_guard<std::mutex> l{m_file_handle_mutex};
        if (m_file_handle) {
            fclose(m_file_handle);
            m_file_handle = nullptr;
        }
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override
    {
        return m_actual_bytes_written - m_file_start_offset;
    }

    bool closed() const override { return m_file_handle == nullptr; }

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
            return error();
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

            std::lock_guard<std::mutex> l{m_file_handle_mutex};
            auto file_handle = get_or_open_file_handle(l);
            if (!file_handle) {
                set_error(arrow::Status::IOError("Failed to open file handle for writing"));
                return;
            }
            if (fwrite(data->data(), 1, (std::size_t)data->size(), file_handle)
                != (std::size_t)data->size())
            {
                set_error(arrow::Status::IOError("Failed to write data to file"));
                return;
            }
            m_completed_byte_writes += data->size();

            // Ensure we do this after editing all the other members, in order to prevent `Flush`
            // returning until we are done.
            m_completed_writes += 1;

            // Close the file handle if we do not have further writes pending:
            if (m_submitted_writes == m_completed_writes) {
                close_file_handle(l);
            }
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
            return error();
        }

        // No file handle so nothing to flush
        std::lock_guard<std::mutex> l{m_file_handle_mutex};
        if (!m_file_handle) {
            return arrow::Status::OK();
        }

        if (fflush(m_file_handle) != 0) {
            return arrow::Status::IOError("Error flushing file");
        }
        return arrow::Status::OK();
    }

    void set_file_start_offset(std::size_t val) override { m_file_start_offset = val; }

    arrow::Status batch_complete() override
    {
        if (m_flush_on_batch_complete) {
            return Flush();
        }
        return arrow::Status::OK();
    }

    AsyncOutputStream(
        std::string const & file_path,
        std::shared_ptr<ThreadPool> const & thread_pool,
        bool flush_on_batch_complete,
        arrow::MemoryPool * memory_pool,
        bool keep_file_open,
        PrivateDummy)
    : m_has_error{false}
    , m_submitted_writes{0}
    , m_completed_writes{0}
    , m_submitted_byte_writes{0}
    , m_completed_byte_writes{0}
    , m_actual_bytes_written{0}
    , m_flush_on_batch_complete(flush_on_batch_complete)
    , m_file_path(file_path)
    , m_keep_file_open(keep_file_open)
    , m_file_start_offset{0}
    , m_strand{thread_pool->create_strand()}
    , m_memory_pool(memory_pool)
    {
        m_file_handle = fopen(m_file_path.c_str(), "wb");
        if (!m_file_handle) {
            set_error(arrow::Status::IOError("Failed to open file for writing: ", errno));
        }
        if (!m_keep_file_open) {
            fclose(m_file_handle);
            m_file_handle = nullptr;
        }
    }

private:
    arrow::MemoryPool * memory_pool() { return m_memory_pool; }

    FILE * get_or_open_file_handle([[maybe_unused]] std::lock_guard<std::mutex> & lock)
    {
        if (m_file_handle) {
            return m_file_handle;
        }

        m_file_handle = fopen(m_file_path.c_str(), "ab");
        return m_file_handle;
    }

    void close_file_handle([[maybe_unused]] std::lock_guard<std::mutex> & lock)
    {
        if (m_file_handle && !m_keep_file_open) {
            fclose(m_file_handle);
            m_file_handle = nullptr;
        }
    }

    void set_error(arrow::Status status)
    {
        assert(!status.ok());
        {
            std::lock_guard<std::mutex> l{m_error_mutex};
            m_error = std::move(status);
        }
        m_has_error = true;
    }

    arrow::Status error() const
    {
        std::lock_guard<std::mutex> l{m_error_mutex};
        return m_error;
    }

    std::atomic<bool> m_has_error;

    std::atomic<std::size_t> m_submitted_writes;
    std::atomic<std::size_t> m_completed_writes;
    std::atomic<std::size_t> m_submitted_byte_writes;
    std::atomic<std::size_t> m_completed_byte_writes;
    // this represents the number of data bytes written (excluding any padding for alignment)
    // used for truncating the file for instance
    std::int64_t m_actual_bytes_written;

    bool m_flush_on_batch_complete;

    std::string m_file_path;
    std::mutex m_file_handle_mutex;
    FILE * m_file_handle{nullptr};
    bool m_keep_file_open{false};

    mutable std::mutex m_error_mutex;
    arrow::Status m_error;

    std::size_t m_file_start_offset;
    std::shared_ptr<ThreadPoolStrand> m_strand;
    arrow::MemoryPool * m_memory_pool;
};

}  // namespace pod5
