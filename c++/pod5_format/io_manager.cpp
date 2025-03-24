#include "pod5_format/io_manager.h"

#ifdef __linux__
#include <unistd.h>
#endif

namespace pod5 {

#ifdef __linux__
class IOManagerSyncImpl : public IOManager {
public:
    IOManagerSyncImpl(arrow::MemoryPool * memory_pool) : m_memory_pool(memory_pool) {}

    arrow::Result<std::shared_ptr<QueuedWrite>> allocate_new_write(std::size_t capacity) override
    {
        if (m_queued_writes.size()) {
            auto new_write = m_queued_writes.back();
            m_queued_writes.pop_back();
            ARROW_RETURN_NOT_OK(new_write->reset_queued_write());
            ARROW_RETURN_NOT_OK(new_write->get_buffer().Reserve(capacity));
            assert((std::size_t)new_write->get_buffer().capacity() >= capacity);
            return new_write;
        }

        ARROW_ASSIGN_OR_RAISE(
            std::unique_ptr<arrow::ResizableBuffer> buffer,
            arrow::AllocateResizableBuffer(capacity, IOManager::Alignment, m_memory_pool));
        ARROW_RETURN_NOT_OK(buffer->Resize(0, false));
        assert((std::size_t)buffer->capacity() >= capacity);
        assert((std::size_t)buffer->size() == 0);
        return std::make_shared<QueuedWrite>(std::move(buffer));
    }

    arrow::Status return_used_write(std::shared_ptr<QueuedWrite> && used_write) override
    {
        if (m_queued_writes.size() < CachedBufferCount) {
            m_queued_writes.push_back(std::move(used_write));
        }
        used_write.reset();
        return arrow::Status::OK();
    }

    arrow::Status write_buffer(std::shared_ptr<QueuedWrite> && data) override
    {
        auto result = lseek(data->file_descriptor(), data->file_offset(), SEEK_SET);
        if (result < 0) {
            return arrow::Status::IOError("Error seeking in file");
        }

        result =
            write(data->file_descriptor(), data->get_buffer().data(), data->get_buffer().size());
        if (result < 0) {
            return arrow::Status::IOError(
                "Error writing to file: ",
                errno,
                " desc: ",
                data->file_descriptor(),
                " offset: ",
                data->file_offset(),
                " size: ",
                data->get_buffer().size());
        }

        data->set_state(QueuedWrite::WriteState::Completed);

        return {};
    }

private:
    arrow::MemoryPool * m_memory_pool;
    std::vector<std::shared_ptr<QueuedWrite>> m_queued_writes;
};

arrow::Result<std::shared_ptr<IOManager>> make_sync_io_manager(arrow::MemoryPool * memory_pool)
{
    return std::make_shared<IOManagerSyncImpl>(memory_pool);
}
#endif

}  // namespace pod5
