#pragma once

#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/thread_pool.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/util/future.h>
#include <boost/thread/synchronized_value.hpp>

#include <condition_variable>
#include <deque>
#include <iostream>
#include <thread>

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {
constexpr size_t alignment = 4096;                 // buffer alignment (for block devices)
constexpr size_t megabyte = 256 * alignment;       // 1MB
constexpr size_t fallocate_chunk = 50 * megabyte;  // 50MB
constexpr size_t write_buffer_size = megabyte;     // Arbitrary limit. Seems a good trade-off
                                                   // between memory usage and disk activities

}  // namespace

namespace pod5 {

class AsyncOutputStream : public arrow::io::OutputStream {
public:
    AsyncOutputStream(
        std::shared_ptr<OutputStream> const & main_stream,
        std::shared_ptr<ThreadPool> const & thread_pool)
    : m_has_error{false}
    , m_submitted_writes{0}
    , m_completed_writes{0}
    , m_submitted_byte_writes{0}
    , m_completed_byte_writes{0}
    , m_actual_bytes_written{0}
    , m_main_stream{main_stream}
    , m_file_start_offset{0}
    , m_strand{thread_pool->create_strand()}
    {
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
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> buffer, arrow::AllocateBuffer(nbytes));
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

    void set_file_start_offset(std::size_t val) { m_file_start_offset = val; }

protected:
    virtual arrow::Status write_final_chunk() { return arrow::Status::OK(); }

    virtual arrow::Status write_cache() { return arrow::Status::OK(); }

    virtual arrow::Status truncate_file() { return arrow::Status::OK(); }

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
};

#ifdef __linux__
class AsyncOutputStreamDirectIO : public AsyncOutputStream {
public:
    AsyncOutputStreamDirectIO(
        std::shared_ptr<OutputStream> const & main_stream,
        std::shared_ptr<ThreadPool> const & thread_pool)
    : AsyncOutputStream(main_stream, thread_pool)
    , m_fallocate_offset{0}
    , m_buffer(write_buffer_size, alignment)
    , m_flushed_buffer_copy(alignment, 0)
    , m_buffer_offset{0}
    , m_num_blocks_written{0}
    {
        // pre-allocate file in chunks of [fallocate_chunk] size.
        // NOTE: we will need to reserve more space once we exhaust that limit
        // NOTE: this is needed in order to support directIO
        reserve_space(m_fallocate_offset);
    }

    ~AsyncOutputStreamDirectIO() { Close(); }

    arrow::Status Write(void const * data, int64_t nbytes) override
    {
        return Write(
            arrow::Buffer::Wrap<std::uint8_t>(static_cast<std::uint8_t const *>(data), nbytes));
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

        // restore saved buffer (if any)
        std::size_t const buffer_offset_copy = m_buffer_offset;

        // Restore any data we already wrote to the output on a previous call to `Flush`
        // This code will not restore anything unless the previous call was `Flush`.
        ARROW_RETURN_NOT_OK(restore_saved_buffer());

        std::size_t const new_sz = data->size() + buffer_offset_copy;

        m_submitted_byte_writes += new_sz;
        m_actual_bytes_written += new_sz;

        // ensure we have sufficient space available
        resize(new_sz);

        // fill up our aligned buffer and output
        gsl::span<std::uint8_t const> remaining_data(data->data(), data->size());

        while (!remaining_data.empty()) {
            remaining_data = m_buffer.consume_until_full(remaining_data);

            if (m_buffer.is_full()) {
                ARROW_RETURN_NOT_OK(write_cache());

                // adjust accounting
                m_num_blocks_written += (write_buffer_size / alignment);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Flush() override
    {
        write_final_chunk();

        return AsyncOutputStream::Flush();
    }

protected:
    arrow::Status truncate_file() override
    {
        if (::ftruncate(get_file_descriptor(), m_actual_bytes_written) < 0) {
            return arrow::Status::IOError("Failed to truncate file");
        }

        return arrow::Status::OK();
    }

    arrow::Status write_final_chunk() override
    {
        // work out remaining bytes
        std::size_t const extra_bytes = m_buffer.buffered_size() % alignment;

        // Write any output we have to the main stream, ensuring we write all data available.
        if (extra_bytes != 0) {
            m_num_blocks_written += m_buffer.buffered_size() / alignment;

            // pad buffer and output
            auto const padding_count = alignment - extra_bytes;
            std::vector<std::uint8_t> to_pad(padding_count, 'x');

            m_buffer.consume_until_full(to_pad);
            assert((m_buffer.buffered_size() % alignment) == 0);

            // save last segment as this will have to be re-written
            m_buffer_offset = extra_bytes;

            std::size_t const last_segment_offset =
                m_buffer.buffered_size() - extra_bytes - padding_count;
            auto const last_segment_start = m_buffer.get() + last_segment_offset;

            std::copy(
                last_segment_start, last_segment_start + alignment, m_flushed_buffer_copy.begin());
            assert(m_flushed_buffer_copy.size() == alignment);

            // adjust accounting
            m_submitted_byte_writes += padding_count;

            return write_cache();
        }

        return arrow::Status::OK();
    }

    arrow::Status write_cache() override
    {
        auto data = arrow::Buffer::Wrap<std::uint8_t>(m_buffer.get(), m_buffer.buffered_size());

        m_submitted_writes += 1;

        auto const result = m_main_stream->Write(data);

        m_completed_byte_writes += data->size();

        if (!result.ok()) {
            m_error = result;
            m_has_error = true;
        }

        m_completed_writes += 1;

        m_buffer.clear();

        return arrow::Status::OK();
    }

private:
    // ensures writes are aligned
    class AlignedBuffer {
    public:
        AlignedBuffer(std::size_t capacity, std::size_t alignment)
        : m_buffer(
            nullptr,
            std::free)  // Memory allocated using posix_memalign is freed using std::free
        , m_capacity(capacity)
        , m_size(0)
        {
            std::uint8_t * mem = nullptr;
            auto result = posix_memalign(reinterpret_cast<void **>(&mem), alignment, capacity);
            if ((result != 0) || (mem == nullptr)) {
                throw std::bad_alloc();
            }
            m_buffer.reset(mem);
        }

        // Copy input span to the end of the buffer until this buffer is full.
        //
        // Return any remaining buffer.
        gsl::span<std::uint8_t const> consume_until_full(
            gsl::span<std::uint8_t const> const & input)
        {
            auto const remaining_buffer_bytes = m_capacity - m_size;
            auto const to_copy = std::min(input.size(), remaining_buffer_bytes);

            std::copy(input.begin(), input.begin() + to_copy, m_buffer.get() + m_size);
            m_size += to_copy;

            return input.subspan(to_copy);
        }

        // Find if the buffer is full (m_size == m_capacity)
        bool is_full() const { return m_size == m_capacity; }

        std::uint8_t * get() const { return m_buffer.get(); }

        // Find the number of buffered bytes
        std::size_t buffered_size() const { return m_size; }

        // Reset the buffer so it is empty
        void clear() { m_size = 0; }

    private:
        std::unique_ptr<std::uint8_t, decltype(&std::free)> m_buffer;
        std::size_t m_capacity;
        std::size_t m_size;
    };

    arrow::Status restore_saved_buffer()
    {
        if (m_buffer_offset) {
            m_buffer.clear();

            gsl::span<std::uint8_t const> data(m_flushed_buffer_copy.data(), m_buffer_offset);

            data = m_buffer.consume_until_full(data);
            assert(data.empty());

            // reposition file pointer
            ARROW_RETURN_NOT_OK(reset_file_position(alignment * m_num_blocks_written));

            m_actual_bytes_written -= (std::int64_t)m_buffer_offset;
            m_flushed_buffer_copy.clear();
            m_flushed_buffer_copy.resize(alignment, 0);

            m_buffer_offset = 0;
        }

        return arrow::Status::OK();
    }

    void resize(std::size_t data_size)
    {
        if (m_submitted_byte_writes + data_size > fallocate_chunk) {
            // reserve more space before continuing
            m_fallocate_offset += fallocate_chunk;

            reserve_space(m_fallocate_offset);
        }
    }

    void reserve_space(std::size_t offset)
    {
        if (::fallocate(get_file_descriptor(), 0, offset, fallocate_chunk) < 0) {
            // DirectIO not supported by this platform, ignoring
        }
    }

    arrow::Status reset_file_position(std::size_t offset)
    {
        if (::lseek(get_file_descriptor(), offset, SEEK_SET) < 0) {
            return arrow::Status::IOError("Failed to reset file position");
        }

        return arrow::Status::OK();
    }

    std::size_t m_fallocate_offset;
    AlignedBuffer m_buffer;
    // copy of buffer (unaligned) which has been flushed to the output stream already,
    // intended to be used when we write more data to the flushed stream so we can always
    // write a block of the right size to enable directio.
    std::vector<std::uint8_t> m_flushed_buffer_copy;
    // offset to the next free slot inside the buffer
    // we always write into the buffer from that offset
    std::size_t m_buffer_offset;
    std::size_t m_num_blocks_written;
};
#endif
}  // namespace pod5
