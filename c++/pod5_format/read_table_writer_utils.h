#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/read_table_utils.h"
#include "pod5_format/result.h"

#include <arrow/io/type_fwd.h>
#include <gsl/gsl-lite.hpp>

#include <chrono>
#include <cstdint>
#include <map>

namespace pod5 {

namespace detail {
template <typename T>
class PrimitiveDictionaryKeyBuilder {
public:
    void append(T const& value) { m_values.push_back(value); }

    std::size_t length() const { return m_values.size(); }
    gsl::span<T const> get_data() const { return gsl::make_span(m_values); }

private:
    std::vector<T> m_values;
};

// Working around std::vector<bool> and arrow bool types not playing well together.
template <>
class PrimitiveDictionaryKeyBuilder<bool> {
public:
    void append(bool const& value) { m_values.push_back(value); }

    std::size_t length() const { return m_values.size(); }
    gsl::span<std::uint8_t const> get_data() const { return gsl::make_span(m_values); }

private:
    std::vector<std::uint8_t> m_values;
};

class StringDictionaryKeyBuilder {
public:
    void append(std::string const& value) {
        m_offset_values.push_back(m_string_values.size());
        m_string_values.insert(m_string_values.end(), value.begin(), value.end());
    }

    std::size_t length() const { return m_offset_values.size(); }

    gsl::span<std::uint8_t const> get_string_data() const {
        return gsl::make_span(m_string_values);
    }
    gsl::span<std::int32_t const> get_offset_data() const {
        return gsl::make_span(m_offset_values);
    }

private:
    std::vector<std::uint8_t> m_string_values;
    std::vector<std::int32_t> m_offset_values;
};

class StringMapDictionaryKeyBuilder {
public:
    void append(pod5::RunInfoData::MapType const& value) {
        m_offset_values.push_back(m_key_builder.length());
        for (auto const& item : value) {
            m_key_builder.append(item.first);
            m_value_builder.append(item.second);
        }
    }

    std::size_t length() const { return m_offset_values.size(); }

    StringDictionaryKeyBuilder const& key_builder() const { return m_key_builder; }
    StringDictionaryKeyBuilder const& value_builder() const { return m_value_builder; }
    gsl::span<std::int32_t const> get_offset_data() const {
        return gsl::make_span(m_offset_values);
    }

private:
    std::vector<std::int32_t> m_offset_values;
    StringDictionaryKeyBuilder m_key_builder;
    StringDictionaryKeyBuilder m_value_builder;
};

template <std::size_t CurrentIndex, typename BuilderTuple, typename Arg>
std::size_t unpack_struct_builder_args(BuilderTuple& builders, Arg&& arg) {
    auto& builder = std::get<CurrentIndex>(builders);
    auto index = builder.length();
    builder.append(arg);
    return index;
}

template <std::size_t CurrentIndex, typename BuilderTuple, typename FirstArg, typename... Args>
std::size_t unpack_struct_builder_args(BuilderTuple& builder,
                                       FirstArg&& first_arg,
                                       Args&&... args) {
    unpack_struct_builder_args<CurrentIndex>(builder, first_arg);
    return unpack_struct_builder_args<CurrentIndex + 1>(builder, std::forward<Args&&>(args)...);
}
}  // namespace detail

template <typename... BuilderTypes>
class StructBuilder {
public:
    template <typename... Args>
    std::size_t append(Args&&... args) {
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