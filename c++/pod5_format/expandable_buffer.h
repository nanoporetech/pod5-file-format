#pragma once

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <gsl/gsl-lite.hpp>

namespace pod5 {

template <typename T>
class ExpandableBuffer {
public:
    static constexpr int EXPANSION_FACTOR = 2;

    ExpandableBuffer(arrow::MemoryPool * pool = nullptr) { m_pool = pool; }

    arrow::Status init_buffer(arrow::MemoryPool * pool)
    {
        m_pool = pool;
        return clear();
    }

    std::size_t size() const
    {
        if (!m_buffer) {
            return 0;
        }
        return m_buffer->size() / sizeof(T);
    }

    std::uint8_t * mutable_data() { return m_buffer->mutable_data(); }

    std::shared_ptr<arrow::Buffer> get_buffer() const { return m_buffer; }

    arrow::Status clear()
    {
        if (!m_buffer || m_buffer.use_count() > 1) {
            ARROW_ASSIGN_OR_RAISE(m_buffer, arrow::AllocateResizableBuffer(0, m_pool));
            return arrow::Status::OK();
        } else {
            return m_buffer->Resize(0);
        }
    }

    gsl::span<T const> get_data_span() const
    {
        if (!m_buffer) {
            return {};
        }

        return gsl::make_span(m_buffer->data(), m_buffer->size()).template as_span<T const>();
    }

    arrow::Status append(T const & new_value)
    {
        auto const bytes_span =
            gsl::make_span(&new_value, 1).template as_span<std::uint8_t const>();

        return append_bytes(bytes_span);
    }

    arrow::Status append_array(gsl::span<T const> const & new_value_span)
    {
        auto const bytes_span = new_value_span.template as_span<std::uint8_t const>();

        return append_bytes(bytes_span);
    }

    arrow::Status resize(std::int64_t new_size)
    {
        ARROW_RETURN_NOT_OK(reserve(new_size));
        return m_buffer->Resize(new_size);
    }

    arrow::Status reserve(std::int64_t new_capacity)
    {
        assert(m_buffer);
        auto const old_size = m_buffer->size();
        if (m_buffer.use_count() > 1) {
            std::shared_ptr<arrow::ResizableBuffer> buffer;
            ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(old_size, m_pool));

            std::copy(m_buffer->data(), m_buffer->data() + old_size, buffer->mutable_data());
            std::swap(m_buffer, buffer);
        }

        if (new_capacity > m_buffer->capacity()) {
            ARROW_RETURN_NOT_OK(m_buffer->Reserve(new_capacity * EXPANSION_FACTOR));
        }
        return arrow::Status::OK();
    }

private:
    arrow::Status append_bytes(gsl::span<std::uint8_t const> const & bytes_span)
    {
        auto old_size = 0;
        if (!m_buffer) {
            ARROW_ASSIGN_OR_RAISE(
                m_buffer, arrow::AllocateResizableBuffer(bytes_span.size(), m_pool));
        } else {
            old_size = m_buffer->size();
        }
        auto const new_size = old_size + bytes_span.size();
        ARROW_RETURN_NOT_OK(reserve(new_size));

        ARROW_RETURN_NOT_OK(m_buffer->Resize(new_size));
        std::copy(bytes_span.begin(), bytes_span.end(), m_buffer->mutable_data() + old_size);
        return arrow::Status::OK();
    }

    std::shared_ptr<arrow::ResizableBuffer> m_buffer;
    arrow::MemoryPool * m_pool = nullptr;
};

}  // namespace pod5
