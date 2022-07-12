#pragma once

#include "pod5_format/expandable_buffer.h"
#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"

#include <arrow/io/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <gsl/gsl-lite.hpp>

#include <chrono>
#include <cstdint>
#include <map>

namespace pod5 {

namespace detail {

template <typename T>
class PrimitiveDictionaryKeyBuilder {
public:
    arrow::Status init_buffer(arrow::MemoryPool* pool) { return m_values.init_buffer(pool); }

    arrow::Status append(T const& value) { return m_values.append(value); }

    std::size_t length() const { return m_values.size(); }
    std::shared_ptr<arrow::Buffer> get_data() const { return m_values.get_buffer(); }

private:
    ExpandableBuffer<T> m_values;
};

// Working around std::vector<bool> and arrow bool types not playing well together.
template <>
class PrimitiveDictionaryKeyBuilder<bool> {
public:
    arrow::Status init_buffer(arrow::MemoryPool* pool) { return m_values.init_buffer(pool); }

    arrow::Status append(bool value) {
        ARROW_RETURN_NOT_OK(m_values.resize((m_bit_length / 8) + 1));
        auto mutable_data = m_values.mutable_data();
        arrow::bit_util::SetBitTo(mutable_data, m_bit_length, value);
        m_bit_length += 1;

        return arrow::Status::OK();
    }

    std::size_t length() const { return m_bit_length; }
    std::shared_ptr<arrow::Buffer> get_data() const { return m_values.get_buffer(); }

private:
    ExpandableBuffer<std::uint8_t> m_values;
    std::size_t m_bit_length = 0;
};

class StringDictionaryKeyBuilder {
public:
    arrow::Status init_buffer(arrow::MemoryPool* pool) {
        ARROW_RETURN_NOT_OK(m_offset_values.init_buffer(pool));
        return m_string_values.init_buffer(pool);
    }

    arrow::Status append(std::string const& value) {
        ARROW_RETURN_NOT_OK(m_offset_values.append(m_string_values.size()));
        return m_string_values.append_array(
                gsl::make_span(value.data(), value.size()).as_span<std::uint8_t const>());
    }

    std::size_t length() const { return m_offset_values.size(); }

    std::shared_ptr<arrow::Buffer> get_string_data() const { return m_string_values.get_buffer(); }
    gsl::span<std::int32_t const> get_typed_offset_data() const {
        return m_offset_values.get_data_span();
    }

private:
    ExpandableBuffer<std::int32_t> m_offset_values;
    ExpandableBuffer<std::uint8_t> m_string_values;
};

class StringMapDictionaryKeyBuilder {
public:
    arrow::Status init_buffer(arrow::MemoryPool* pool) {
        ARROW_RETURN_NOT_OK(m_offset_values.init_buffer(pool));
        ARROW_RETURN_NOT_OK(m_key_builder.init_buffer(pool));
        return m_value_builder.init_buffer(pool);
    }

    arrow::Status append(pod5::RunInfoData::MapType const& value) {
        ARROW_RETURN_NOT_OK(m_offset_values.append(m_key_builder.length()));
        for (auto const& item : value) {
            ARROW_RETURN_NOT_OK(m_key_builder.append(item.first));
            ARROW_RETURN_NOT_OK(m_value_builder.append(item.second));
        }
        return arrow::Status::OK();
    }

    std::size_t length() const { return m_offset_values.size(); }

    StringDictionaryKeyBuilder const& key_builder() const { return m_key_builder; }
    StringDictionaryKeyBuilder const& value_builder() const { return m_value_builder; }
    gsl::span<std::int32_t const> get_typed_offset_data() const {
        return m_offset_values.get_data_span();
    }

private:
    ExpandableBuffer<std::int32_t> m_offset_values;
    StringDictionaryKeyBuilder m_key_builder;
    StringDictionaryKeyBuilder m_value_builder;
};

template <std::size_t CurrentIndex, typename BuilderTuple, typename Arg>
arrow::Result<std::size_t> unpack_struct_builder_args(BuilderTuple& builders, Arg&& arg) {
    auto& builder = std::get<CurrentIndex>(builders);
    auto index = builder.length();
    ARROW_RETURN_NOT_OK(builder.append(arg));
    return index;
}

template <std::size_t CurrentIndex, typename BuilderTuple, typename FirstArg, typename... Args>
arrow::Result<std::size_t> unpack_struct_builder_args(BuilderTuple& builder,
                                                      FirstArg&& first_arg,
                                                      Args&&... args) {
    ARROW_RETURN_NOT_OK(unpack_struct_builder_args<CurrentIndex>(builder, first_arg));
    return unpack_struct_builder_args<CurrentIndex + 1>(builder, std::forward<Args&&>(args)...);
}

template <typename T, typename F, int... Is>
void for_each(T&& t, F f, std::integer_sequence<int, Is...>) {
    auto l = {(f(std::get<Is>(t)), 0)...};
    (void)l;
}

template <typename... Ts, typename F>
void for_each_in_tuple(std::tuple<Ts...>& t, F f) {
    detail::for_each(t, f, std::make_integer_sequence<int, sizeof...(Ts)>());
}

}  // namespace detail

template <typename... BuilderTypes>
class StructBuilder {
public:
    StructBuilder(arrow::MemoryPool* pool) {
        detail::for_each_in_tuple(m_builders, [&](auto& x) { (void)x.init_buffer(pool); });
    }

    template <typename... Args>
    arrow::Result<std::size_t> append(Args&&... args) {
        return detail::unpack_struct_builder_args<0>(m_builders, std::forward<Args&&>(args)...);
    }

    std::tuple<BuilderTypes...>& builders() { return m_builders; }

private:
    std::tuple<BuilderTypes...> m_builders;
};

class POD5_FORMAT_EXPORT DictionaryWriter {
public:
    virtual ~DictionaryWriter() = default;

    pod5::Result<std::shared_ptr<arrow::Array>> build_dictionary_array(
            std::shared_ptr<arrow::Array> const& indices);
    virtual pod5::Result<std::shared_ptr<arrow::Array>> get_value_array() = 0;
    virtual std::size_t item_count() = 0;
};

class POD5_FORMAT_EXPORT PoreWriter : public DictionaryWriter {
public:
    PoreWriter(arrow::MemoryPool* pool);

    pod5::Result<PoreDictionaryIndex> add(PoreData const& pore_data) {
        return m_builder.append(pore_data.channel, pore_data.well, pore_data.pore_type);
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array();
    std::size_t item_count();

private:
    std::shared_ptr<arrow::StructType> m_type;
    StructBuilder<detail::PrimitiveDictionaryKeyBuilder<std::uint16_t>,
                  detail::PrimitiveDictionaryKeyBuilder<std::uint8_t>,
                  detail::StringDictionaryKeyBuilder>
            m_builder;
};

class POD5_FORMAT_EXPORT EndReasonWriter : public DictionaryWriter {
public:
    EndReasonWriter(arrow::MemoryPool* pool);

    pod5::Result<EndReasonDictionaryIndex> add(EndReasonData const& end_reason_data) {
        return m_builder.append(end_reason_data.name, end_reason_data.forced);
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array();
    std::size_t item_count();

private:
    std::shared_ptr<arrow::StructType> m_type;
    StructBuilder<detail::StringDictionaryKeyBuilder, detail::PrimitiveDictionaryKeyBuilder<bool>>
            m_builder;
};

class POD5_FORMAT_EXPORT CalibrationWriter : public DictionaryWriter {
public:
    CalibrationWriter(arrow::MemoryPool* pool);

    pod5::Result<CalibrationDictionaryIndex> add(CalibrationData const& calibration_data) {
        return m_builder.append(calibration_data.offset, calibration_data.scale);
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array();
    std::size_t item_count();

private:
    std::shared_ptr<arrow::StructType> m_type;
    StructBuilder<detail::PrimitiveDictionaryKeyBuilder<float>,
                  detail::PrimitiveDictionaryKeyBuilder<float>>
            m_builder;
};

class POD5_FORMAT_EXPORT RunInfoWriter : public DictionaryWriter {
public:
    RunInfoWriter(arrow::MemoryPool* pool);

    pod5::Result<RunInfoDictionaryIndex> add(RunInfoData const& run_info_data) {
        return m_builder.append(
                run_info_data.acquisition_id, run_info_data.acquisition_start_time,
                run_info_data.adc_max, run_info_data.adc_min, run_info_data.context_tags,
                run_info_data.experiment_name, run_info_data.flow_cell_id,
                run_info_data.flow_cell_product_code, run_info_data.protocol_name,
                run_info_data.protocol_run_id, run_info_data.protocol_start_time,
                run_info_data.sample_id, run_info_data.sample_rate, run_info_data.sequencing_kit,
                run_info_data.sequencer_position, run_info_data.sequencer_position_type,
                run_info_data.software, run_info_data.system_name, run_info_data.system_type,
                run_info_data.tracking_id);
    }

    pod5::Result<std::shared_ptr<arrow::Array>> get_value_array();
    std::size_t item_count();

private:
    std::shared_ptr<arrow::StructType> m_type;
    StructBuilder<detail::StringDictionaryKeyBuilder,
                  detail::PrimitiveDictionaryKeyBuilder<std::int64_t>,
                  detail::PrimitiveDictionaryKeyBuilder<std::int16_t>,
                  detail::PrimitiveDictionaryKeyBuilder<std::int16_t>,
                  detail::StringMapDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::PrimitiveDictionaryKeyBuilder<std::int64_t>,
                  detail::StringDictionaryKeyBuilder,
                  detail::PrimitiveDictionaryKeyBuilder<std::uint16_t>,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringDictionaryKeyBuilder,
                  detail::StringMapDictionaryKeyBuilder>
            m_builder;
};

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<PoreWriter>> make_pore_writer(
        arrow::MemoryPool* pool);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<EndReasonWriter>> make_end_reason_writer(
        arrow::MemoryPool* pool);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<CalibrationWriter>> make_calibration_writer(
        arrow::MemoryPool* pool);

POD5_FORMAT_EXPORT arrow::Result<std::shared_ptr<RunInfoWriter>> make_run_info_writer(
        arrow::MemoryPool* pool);

}  // namespace pod5
