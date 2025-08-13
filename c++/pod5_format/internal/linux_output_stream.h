#pragma once

#include "pod5_format/file_output_stream.h"
#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/io_manager.h"

#include <arrow/buffer.h>
#include <arrow/util/future.h>
#include <gsl/gsl-lite.hpp>

#include <condition_variable>
#include <deque>

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

namespace pod5 {

namespace {
constexpr size_t fallocate_chunk = 50 * 256 * IOManager::Alignment;  // 50MB
}  // namespace

#ifdef __linux__
class LinuxOutputStream : public FileOutputStream {
    struct PrivateDummy {};

public:
    static arrow::Result<std::shared_ptr<LinuxOutputStream>> make(
        std::string const & file_path,
        std::shared_ptr<IOManager> const & io_manager,
        std::size_t write_chunk_size,
        bool use_directio,
        bool use_syncio,
        bool flush_on_batch_complete,
        bool keep_file_open = true)
    {
        auto flags = O_RDWR | O_CREAT;
        if (use_directio) {
            flags |= O_DIRECT;
        }

        if (use_syncio) {
            flags |= O_SYNC;
        }

        auto const initial_file_descriptor = open(file_path.c_str(), flags, 0644);
        if (initial_file_descriptor < 0) {
            return arrow::Status::Invalid("Failed to open file");
        }

        return std::make_shared<LinuxOutputStream>(
            file_path,
            initial_file_descriptor,
            flags,
            io_manager,
            write_chunk_size,
            keep_file_open,
            flush_on_batch_complete,
            PrivateDummy{});
    }

    ~LinuxOutputStream() { (void)Close(); }

    arrow::Status Close() override
    {
        // flush all output
        ARROW_RETURN_NOT_OK(Flush());

        while (!m_queued_writes.empty()) {
            ARROW_RETURN_NOT_OK(process_queued_writes());

            if (!m_queued_writes.empty()) {
                ARROW_RETURN_NOT_OK(m_io_manager->wait_for_event(std::chrono::seconds(1)));
            }
        }

        std::lock_guard<std::mutex> l{m_file_handle_mutex};
        ARROW_ASSIGN_OR_RAISE(auto const file_descriptor, get_or_open_fd(l));

        // truncate excess data
        if (::ftruncate(file_descriptor, m_bytes_written) < 0) {
            return arrow::Status::IOError("Failed to truncate file");
        }

        // and close stream
        return close_fd(l, true);
    }

    arrow::Future<> CloseAsync() override { return Close(); }

    arrow::Status Abort() override
    {
        std::lock_guard<std::mutex> l{m_file_handle_mutex};
        return close_fd(l, true);
    }

    arrow::Result<int64_t> Tell() const override { return m_bytes_written - m_file_start_offset; }

    bool closed() const override { return m_file_descriptor == -1; }

    arrow::Status Write(void const * data, int64_t nbytes) override
    {
        ARROW_RETURN_NOT_OK(allocate_file_space(nbytes));

        auto remaining_data = gsl::make_span(reinterpret_cast<std::uint8_t const *>(data), nbytes);
        while (!remaining_data.empty()) {
            ARROW_ASSIGN_OR_RAISE(
                remaining_data, m_aligned_buffer.consume_until_full(remaining_data));

            if (m_aligned_buffer.is_full()) {
                ARROW_RETURN_NOT_OK(flush_writes(FlushMode::AlignedWrites));
            }
        }

        m_bytes_written += nbytes;

        return arrow::Status::OK();
    }

    arrow::Status Write(std::shared_ptr<arrow::Buffer> const & data) override
    {
        ARROW_RETURN_NOT_OK(allocate_file_space(data->size()));

        auto remaining_data = gsl::make_span(data->data(), data->size());
        while (!remaining_data.empty()) {
            ARROW_ASSIGN_OR_RAISE(
                remaining_data, m_aligned_buffer.consume_until_full(remaining_data));

            if (m_aligned_buffer.is_full()) {
                ARROW_RETURN_NOT_OK(flush_writes(FlushMode::AlignedWrites));
            }
        }

        m_bytes_written += data->size();

        return arrow::Status::OK();
    }

    arrow::Status batch_complete() override
    {
        if (m_flush_on_batch_complete) {
            return flush_writes(FlushMode::AllWrites);
        }
        return arrow::Status::OK();
    }

    arrow::Status Flush() override
    {
        ARROW_RETURN_NOT_OK(flush_writes(FlushMode::AllWrites));

        std::lock_guard<std::mutex> l{m_file_handle_mutex};
        if (m_file_descriptor < 0) {
            return arrow::Status::OK();
        }

        if (fsync(m_file_descriptor) < 0) {
            return arrow::Status::IOError("Error flushing file");
        }

        return arrow::Status::OK();
    }

    void set_file_start_offset(std::size_t val) override { m_file_start_offset = val; }

    void set_flush_on_batch_complete(bool flush_on_batch_complete)
    {
        m_flush_on_batch_complete = flush_on_batch_complete;
    }

    LinuxOutputStream(
        std::string const & file_path,
        int initial_file_descriptor,
        int flags,
        std::shared_ptr<IOManager> const & io_manager,
        std::size_t write_chunk_size,
        bool keep_file_open,
        bool flush_on_batch_complete,
        PrivateDummy)
    : m_file_path(file_path)
    , m_flags(flags)
    , m_file_descriptor(initial_file_descriptor)
    , m_keep_file_open(keep_file_open)
    , m_aligned_buffer(write_chunk_size, io_manager)
    , m_io_manager(io_manager)
    , m_flush_on_batch_complete(flush_on_batch_complete)
    {
        if (!m_keep_file_open) {
            close(m_file_descriptor);
            m_file_descriptor = -1;
        }
    }

protected:
    enum class FlushMode { AllWrites, AlignedWrites };

    class AlignedBuffer {
    public:
        AlignedBuffer(std::size_t capacity, std::shared_ptr<IOManager> const & io_manager)
        : m_io_manager(io_manager)
        , m_capacity(capacity)
        {
        }

        // Copy input span to the end of the buffer until this buffer is full.
        //
        // Return any remaining buffer.
        arrow::Result<gsl::span<std::uint8_t const>> consume_until_full(
            gsl::span<std::uint8_t const> input)
        {
            ARROW_RETURN_NOT_OK(ensure_next_write());

            auto & buffer = m_next_write->get_buffer();
            assert((std::size_t)buffer.capacity() >= m_capacity);
            auto const remaining_buffer_bytes = buffer.capacity() - buffer.size();
            auto const to_copy = std::min(input.size(), (std::size_t)remaining_buffer_bytes);

            std::copy(
                input.begin(), input.begin() + to_copy, buffer.mutable_data() + buffer.size());
            ARROW_RETURN_NOT_OK(buffer.Resize(buffer.size() + to_copy, false));

            return input.subspan(to_copy);
        }

        // Find if the buffer is full (m_size == m_capacity)
        bool is_full() const
        {
            if (!m_next_write) {
                return false;
            }

            return (std::size_t)m_next_write->get_buffer().size() == m_capacity;
        }

        arrow::Result<std::shared_ptr<QueuedWrite>> release_all_writes_and_align(
            std::size_t * out_aligned_write_size)
        {
            ARROW_RETURN_NOT_OK(ensure_next_write());

            *out_aligned_write_size = aligned_write_size(m_next_write->get_buffer().size());

            auto result_write = std::move(m_next_write);
            auto & result_write_buffer = result_write->get_buffer();
            ARROW_ASSIGN_OR_RAISE(m_next_write, m_io_manager->allocate_new_write(m_capacity));
            auto & next_write_buffer = m_next_write->get_buffer();
            std::copy(
                result_write_buffer.data() + *out_aligned_write_size,
                result_write_buffer.data() + result_write_buffer.size(),
                next_write_buffer.mutable_data());
            ARROW_RETURN_NOT_OK(next_write_buffer.Resize(
                result_write_buffer.size() - *out_aligned_write_size, false));

            // Ensure the result write buffer is aligned to our write alignment.
            auto const result_unaligned_size = result_write_buffer.size();
            auto result_aligned_size =
                result_unaligned_size + (-result_unaligned_size & (IOManager::Alignment - 1));
            ARROW_RETURN_NOT_OK(result_write_buffer.Resize(result_aligned_size, false));
            assert(result_write_buffer.size() % IOManager::Alignment == 0);

            result_write->set_state(QueuedWrite::WriteState::ReadyForWrite);
            return result_write;
        }

        arrow::Result<std::shared_ptr<QueuedWrite>> release_aligned_writes()
        {
            ARROW_RETURN_NOT_OK(ensure_next_write());

            auto result_write = std::move(m_next_write);
            auto & result_write_buffer = result_write->get_buffer();
            ARROW_ASSIGN_OR_RAISE(m_next_write, m_io_manager->allocate_new_write(m_capacity));
            auto & next_write_buffer = m_next_write->get_buffer();

            auto aligned_size = aligned_write_size(result_write_buffer.size());
            std::copy(
                result_write_buffer.data() + aligned_size,
                result_write_buffer.data() + result_write_buffer.size(),
                next_write_buffer.mutable_data());
            ARROW_RETURN_NOT_OK(result_write_buffer.Resize(aligned_size, false));
            ARROW_RETURN_NOT_OK(
                next_write_buffer.Resize(result_write_buffer.size() - aligned_size, false));

            result_write->set_state(QueuedWrite::WriteState::ReadyForWrite);
            return result_write;
        }

    private:
        std::size_t aligned_write_size(std::size_t input_size) const
        {
            return (input_size / IOManager::Alignment) * IOManager::Alignment;
        }

        arrow::Status ensure_next_write()
        {
            if (m_next_write) {
                return arrow::Status::OK();
            }

            ARROW_ASSIGN_OR_RAISE(m_next_write, m_io_manager->allocate_new_write(m_capacity));
            assert((std::size_t)m_next_write->get_buffer().capacity() >= m_capacity);
            return arrow::Status::OK();
        }

        std::shared_ptr<QueuedWrite> m_next_write;
        std::shared_ptr<IOManager> m_io_manager;
        std::size_t m_capacity;
    };

    arrow::Result<int> get_or_open_fd([[maybe_unused]] std::lock_guard<std::mutex> & lock)
    {
        if (m_file_descriptor >= 0) {
            return m_file_descriptor;
        }

        m_file_descriptor = open(m_file_path.c_str(), m_flags, 0644);
        if (m_file_descriptor < 0) {
            return arrow::Status::IOError("Failed to open file for writing");
        }
        return m_file_descriptor;
    }

    arrow::Status close_fd([[maybe_unused]] std::lock_guard<std::mutex> & lock, bool force = false)
    {
        if (m_keep_file_open && !force) {
            return arrow::Status::OK();
        }

        if (close(m_file_descriptor) != 0) {
            return arrow::Status::IOError("Error closing file");
        }
        m_file_descriptor = -1;
        return arrow::Status::OK();
    }

    arrow::Status flush_writes(FlushMode flush_mode)
    {
        std::size_t write_offset{};
        std::shared_ptr<QueuedWrite> released_data;

        if (flush_mode == FlushMode::AllWrites) {
            std::size_t aligned_write_size = 0;
            ARROW_ASSIGN_OR_RAISE(
                released_data, m_aligned_buffer.release_all_writes_and_align(&aligned_write_size));
            write_offset = m_bytes_submitted_to_manager;
            m_bytes_submitted_to_manager += aligned_write_size;
        } else if (flush_mode == FlushMode::AlignedWrites) {
            ARROW_ASSIGN_OR_RAISE(released_data, m_aligned_buffer.release_aligned_writes());
            write_offset = m_bytes_submitted_to_manager;
            m_bytes_submitted_to_manager += released_data->get_buffer().size();
        } else {
            assert(false);
            return arrow::Status::Invalid("Invalid FlushMode Passed.");
        }

        assert(released_data->get_buffer().size() % IOManager::Alignment == 0);

        if (released_data->get_buffer().size() == 0) {
            return arrow::Status::OK();
        }

        std::lock_guard<std::mutex> lock(m_file_handle_mutex);
        ARROW_ASSIGN_OR_RAISE(auto const file_descriptor, get_or_open_fd(lock));
        released_data->prepare_for_write(file_descriptor, write_offset);

        m_queued_writes.emplace_back(released_data);
        ARROW_RETURN_NOT_OK(m_io_manager->write_buffer(std::move(released_data)));

        ARROW_RETURN_NOT_OK(process_queued_writes());

        if (m_queued_writes.empty()) {
            return close_fd(lock);
        } else {
            // If we have queued writes, we keep the file open.
            return arrow::Status::OK();
        }
    }

    arrow::Status process_queued_writes()
    {
        for (auto it = m_queued_writes.begin(); it != m_queued_writes.end();) {
            if ((*it)->state() == QueuedWrite::WriteState::Completed) {
                ARROW_RETURN_NOT_OK(m_io_manager->return_used_write(std::move(*it)));
                it = m_queued_writes.erase(it);
            } else {
                ++it;
            }
        }

        return arrow::Status::OK();
    }

    arrow::Status allocate_file_space(std::size_t new_write_size)
    {
        auto new_total_size = m_bytes_written + new_write_size;
        if (new_total_size > m_fallocate_offset) {
            // reserve more space before continuing
            m_fallocate_offset += fallocate_chunk;

            std::lock_guard<std::mutex> lock(m_file_handle_mutex);
            ARROW_ASSIGN_OR_RAISE(auto const file_descriptor, get_or_open_fd(lock));

            // If this fails, we will just write less optimially, so we ignore the result.
            ::fallocate(file_descriptor, 0, m_fallocate_offset, fallocate_chunk);
        }

        return arrow::Status::OK();
    }

    std::string m_file_path;
    int m_flags;
    std::mutex m_file_handle_mutex;
    int m_file_descriptor;
    bool m_keep_file_open{false};

    AlignedBuffer m_aligned_buffer;
    std::vector<std::shared_ptr<QueuedWrite>> m_queued_writes;
    std::shared_ptr<IOManager> m_io_manager;
    std::size_t m_fallocate_offset{0};
    std::size_t m_file_start_offset{0};
    std::size_t m_bytes_written{0};
    std::size_t m_bytes_submitted_to_manager{0};
    bool m_flush_on_batch_complete;
};
#endif

}  // namespace pod5
