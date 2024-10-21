#pragma once

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>

#ifdef __linux__
#include <sys/uio.h>
#endif

#include <atomic>
#include <cassert>
#include <chrono>
#include <memory>

namespace pod5 {

#ifdef __linux__
class QueuedWrite {
public:
    QueuedWrite() = default;

    QueuedWrite(std::unique_ptr<arrow::ResizableBuffer> && buffer) : m_buffer(std::move(buffer)) {}

    arrow::Status reset_queued_write()
    {
        assert(m_state != WriteState::ReadyForWrite);
        assert(m_state != WriteState::InFlight);
        m_iovec = {};
        m_state = WriteState::Empty;
        m_file_offset = -1;
        m_file_descriptor = -1;
        return m_buffer->Resize(0, false);
    }

    void prepare_for_write(int file_descriptor, std::uint64_t offset)
    {
        m_file_descriptor = file_descriptor;
        m_file_offset = offset;
        m_iovec = {.iov_base = m_buffer->mutable_data(), .iov_len = (std::size_t)m_buffer->size()};
        set_state(WriteState::ReadyForWrite);
    }

    arrow::ResizableBuffer & get_buffer() { return *m_buffer; }

    arrow::Buffer const & get_buffer() const { return *m_buffer; }

    int file_descriptor() const { return m_file_descriptor; }

    std::uint64_t file_offset() const { return m_file_offset; }

    iovec * get_iovec_for_buffer() { return &m_iovec; }

    enum class WriteState { Empty, ReadyForWrite, InFlight, Completed };

    WriteState state() const { return m_state; }

    void set_state(WriteState state) { m_state = state; }

private:
    std::unique_ptr<arrow::ResizableBuffer> m_buffer;
    std::uint64_t m_file_offset{(std::uint64_t)-1};
    iovec m_iovec{};
    int m_file_descriptor{-1};
    WriteState m_state{WriteState::Empty};
};
#endif

class IOManager {
public:
    constexpr static size_t Alignment = 4096;  // buffer alignment (for block devices)
    constexpr static size_t CachedBufferCount = 5;

    virtual ~IOManager() = default;

#ifdef __linux__
    virtual arrow::Result<std::shared_ptr<QueuedWrite>> allocate_new_write(
        std::size_t capacity) = 0;
    virtual arrow::Status return_used_write(std::shared_ptr<QueuedWrite> && used_write) = 0;

    virtual arrow::Status write_buffer(std::shared_ptr<QueuedWrite> && data) = 0;

    virtual arrow::Status wait_for_event(std::chrono::nanoseconds timeout) { return {}; }
#endif
};

#ifdef __linux__
arrow::Result<std::shared_ptr<IOManager>> make_sync_io_manager(
    arrow::MemoryPool * memory_pool = arrow::default_memory_pool());
#endif

}  // namespace pod5
