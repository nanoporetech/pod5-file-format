#pragma once

#include "pod5_format/dictionary_writer.h"
#include "pod5_format/read_table_schema.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>

namespace pod5 {

class DictionaryWriter;

namespace detail {
template <typename ArrayType>
class BuilderHelper;
template <typename ArrayType, typename ElementArrayType>
class ListBuilderHelper;

template <>
class BuilderHelper<UuidArray> : public arrow::FixedSizeBinaryBuilder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const & uuid_type, arrow::MemoryPool * pool)
    : arrow::FixedSizeBinaryBuilder(find_storage_type(uuid_type), pool)
    {
        assert(byte_width() == 16);
    }

    static std::shared_ptr<arrow::DataType> find_storage_type(
        std::shared_ptr<arrow::DataType> const & uuid_type)
    {
        assert(uuid_type->id() == arrow::Type::EXTENSION);
        auto uuid_extension = std::static_pointer_cast<arrow::ExtensionType>(uuid_type);
        return uuid_extension->storage_type();
    }

    arrow::Status Append(boost::uuids::uuid const & uuid)
    {
        return static_cast<arrow::FixedSizeBinaryBuilder *>(this)->Append(uuid.begin());
    }
};

template <>
class BuilderHelper<arrow::FloatArray> : public arrow::FloatBuilder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::FloatBuilder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::UInt8Array> : public arrow::UInt8Builder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::UInt8Builder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::UInt16Array> : public arrow::UInt16Builder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::UInt16Builder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::Int16Array> : public arrow::Int16Builder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::Int16Builder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::UInt32Array> : public arrow::UInt32Builder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::UInt32Builder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::UInt64Array> : public arrow::UInt64Builder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::UInt64Builder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::BooleanArray> : public arrow::BooleanBuilder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::BooleanBuilder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::NumericArray<arrow::TimestampType>> : public arrow::TimestampBuilder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const & type, arrow::MemoryPool * pool)
    : arrow::TimestampBuilder(type, pool)
    {
    }
};

template <>
class BuilderHelper<arrow::StringArray> : public arrow::StringBuilder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::StringBuilder(pool)
    {
    }
};

template <>
class BuilderHelper<arrow::MapArray> {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : m_key_builder(std::make_shared<arrow::StringBuilder>(pool))
    , m_item_builder(std::make_shared<arrow::StringBuilder>(pool))
    , m_map_builder(pool, m_key_builder, m_item_builder)
    {
    }

    arrow::Status Finish(std::shared_ptr<arrow::Array> * dest)
    {
        return m_map_builder.Finish(dest);
    }

    arrow::Status Reserve(std::size_t rows) { return m_map_builder.Reserve(rows); }

    arrow::Status Append(std::vector<std::pair<std::string, std::string>> const & items)
    {
        ARROW_RETURN_NOT_OK(m_map_builder.Append());  // start new slot
        for (auto const & pair : items) {
            ARROW_RETURN_NOT_OK(m_key_builder->Append(pair.first));
            ARROW_RETURN_NOT_OK(m_item_builder->Append(pair.second));
        }
        return arrow::Status::OK();
    }

private:
    std::shared_ptr<arrow::StringBuilder> m_key_builder;
    std::shared_ptr<arrow::StringBuilder> m_item_builder;
    arrow::MapBuilder m_map_builder;
};

template <>
class BuilderHelper<arrow::DictionaryArray> : public arrow::Int16Builder {
public:
    BuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : arrow::Int16Builder(pool)
    {
    }

    void set_dict_writer(std::shared_ptr<DictionaryWriter> const & writer)
    {
        m_dict_writer = writer;
    }

    arrow::Status Finish(std::shared_ptr<arrow::Array> * dest)
    {
        arrow::Int16Builder * index_builder = this;
        ARROW_ASSIGN_OR_RAISE(auto indices, index_builder->Finish());
        ARROW_ASSIGN_OR_RAISE(*dest, m_dict_writer->build_dictionary_array(indices));
        return arrow::Status::OK();
    }

private:
    std::shared_ptr<DictionaryWriter> m_dict_writer;
};

template <typename ElementArrayType>
class ListBuilderHelper<arrow::ListArray, ElementArrayType> {
public:
    ListBuilderHelper(std::shared_ptr<arrow::DataType> const &, arrow::MemoryPool * pool)
    : m_array_builder(std::make_shared<BuilderHelper<ElementArrayType>>(nullptr, pool))
    , m_builder(pool, m_array_builder)
    {
    }

    arrow::Status Reserve(std::size_t rows)
    {
        ARROW_RETURN_NOT_OK(m_builder.Reserve(rows));
        return m_array_builder->Reserve(rows);
    }

    arrow::Status Finish(std::shared_ptr<arrow::Array> * dest) { return m_builder.Finish(dest); }

    template <typename Items>
    arrow::Status Append(Items const & items)
    {
        ARROW_RETURN_NOT_OK(m_builder.Append());  // start new slot
        return m_array_builder->AppendValues(items.data(), items.size());
    }

private:
    std::shared_ptr<BuilderHelper<ElementArrayType>> m_array_builder;
    arrow::ListBuilder m_builder;
};

}  // namespace detail

template <typename... Args>
class FieldBuilder {
public:
    using BuilderTuple = std::tuple<typename Args::BuilderType...>;

    template <typename SchamaDescription>
    FieldBuilder(std::shared_ptr<SchamaDescription> const & desc_base, arrow::MemoryPool * pool)
    : m_builders(typename Args::BuilderType(
        desc_base->fields()[Args::WriteIndex::value]->datatype(),
        pool)...)
    {
    }

    template <typename FieldType>
    std::tuple_element_t<FieldType::WriteIndex::value, BuilderTuple> & get_builder(FieldType)
    {
        return std::get<FieldType::WriteIndex::value>(m_builders);
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Array>>> finish_columns()
    {
        arrow::Status result;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.resize(std::tuple_size<decltype(m_builders)>::value);

        detail::for_each_in_tuple(m_builders, [&](auto & element, std::size_t index) {
            if (result.ok()) {
                result = element.Finish(&columns[index]);
                assert(columns[index] || !result.ok());
            }
        });

        if (!result.ok()) {
            return result;
        }

        return columns;
    }

    arrow::Status reserve(std::size_t row_count)
    {
        arrow::Status result;
        detail::for_each_in_tuple(m_builders, [&](auto & element, std::size_t _) {
            if (result.ok()) {
                result = element.Reserve(row_count);
            }
        });
        return result;
    }

    template <typename... AppendArgs>
    arrow::Status append(AppendArgs const &... args)
    {
        auto args_list = std::forward_as_tuple(args...);

        arrow::Status result;
        for_each_in_tuple_zipped(
            m_builders, args_list, [&](auto & builder, auto & item, std::size_t _) {
                if (result.ok()) {
                    result = builder.Append(item);
                }
            });
        return result;
    }

private:
    BuilderTuple m_builders;
};

}  // namespace pod5
