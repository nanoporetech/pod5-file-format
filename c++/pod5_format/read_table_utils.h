#pragma once

#include "pod5_format/pod5_format_export.h"

#include <boost/functional/hash_fwd.hpp>
#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace pod5 {

using SignalTableRowIndex = std::uint64_t;

using PoreDictionaryIndex = std::int16_t;
using EndReasonDictionaryIndex = std::int16_t;
using RunInfoDictionaryIndex = std::int16_t;

class ReadData {
public:
    ReadData() = default;

    /// \brief Create a new read data structure to add to a read.
    /// \param read_id The read id for the read entry.
    /// \param read_number Read number for this read.
    /// \param start_sample The sample which this read starts at.
    /// \param median_before The median of the read chunk prior to the start of this read.
    /// \param end_reason The dictionary index of the end reason name which caused this read to complete.
    /// \param end_reason_forced Boolean value indicating if the read end was forced.
    /// \param run_info_id The dictionary index of the run info for this read.
    /// \param num_minknow_events The number of minknow events in the read.
    ReadData(
        boost::uuids::uuid const & read_id,
        std::uint32_t read_number,
        std::uint64_t start_sample,
        std::uint16_t channel,
        std::uint8_t well,
        PoreDictionaryIndex pore_type,
        float calibration_offset,
        float calibration_scale,
        float median_before,
        EndReasonDictionaryIndex end_reason,
        bool end_reason_forced,
        RunInfoDictionaryIndex run_info,
        std::uint64_t num_minknow_events,
        float tracked_scaling_scale,
        float tracked_scaling_shift,
        float predicted_scaling_scale,
        float predicted_scaling_shift,
        std::uint32_t num_reads_since_mux_change,
        float time_since_mux_change)
    : read_id(read_id)
    , read_number(read_number)
    , start_sample(start_sample)
    , median_before(median_before)
    , end_reason(end_reason)
    , end_reason_forced(end_reason_forced)
    , run_info(run_info)
    , num_minknow_events(num_minknow_events)
    , tracked_scaling_scale(tracked_scaling_scale)
    , tracked_scaling_shift(tracked_scaling_shift)
    , predicted_scaling_scale(predicted_scaling_scale)
    , predicted_scaling_shift(predicted_scaling_shift)
    , num_reads_since_mux_change(num_reads_since_mux_change)
    , time_since_mux_change(time_since_mux_change)
    , channel(channel)
    , well(well)
    , pore_type(pore_type)
    , calibration_offset(calibration_offset)
    , calibration_scale(calibration_scale)
    {
    }

    // V1 Fields
    boost::uuids::uuid read_id;
    std::uint32_t read_number;
    std::uint64_t start_sample;
    float median_before;
    EndReasonDictionaryIndex end_reason;
    bool end_reason_forced;
    RunInfoDictionaryIndex run_info;

    // V2 Fields
    std::uint64_t num_minknow_events;
    float tracked_scaling_scale;
    float tracked_scaling_shift;
    float predicted_scaling_scale;
    float predicted_scaling_shift;
    std::uint32_t num_reads_since_mux_change;
    float time_since_mux_change;

    // V3 Fields
    std::uint16_t channel;
    std::uint8_t well;
    PoreDictionaryIndex pore_type;
    float calibration_offset;
    float calibration_scale;
};

inline bool operator==(ReadData const & a, ReadData const & b)
{
    return a.read_id == b.read_id && a.read_number == b.read_number
           && a.start_sample == b.start_sample && a.median_before == b.median_before
           && a.end_reason == b.end_reason && a.end_reason_forced == b.end_reason_forced
           && a.run_info == b.run_info && a.num_minknow_events == b.num_minknow_events
           && a.tracked_scaling_scale == b.tracked_scaling_scale
           && a.tracked_scaling_shift == b.tracked_scaling_shift
           && a.predicted_scaling_scale == b.predicted_scaling_scale
           && a.predicted_scaling_shift == b.predicted_scaling_shift
           && a.num_reads_since_mux_change == b.num_reads_since_mux_change
           && a.time_since_mux_change == b.time_since_mux_change && a.channel == b.channel
           && a.well == b.well && a.pore_type == b.pore_type
           && a.calibration_offset == b.calibration_offset
           && a.calibration_scale == b.calibration_scale;
}

class RunInfoData {
public:
    using MapType = std::vector<std::pair<std::string, std::string>>;

    RunInfoData(
        std::string const & acquisition_id,
        std::int64_t acquisition_start_time,
        std::int16_t adc_max,
        std::int16_t adc_min,
        MapType const & context_tags,
        std::string const & experiment_name,
        std::string const & flow_cell_id,
        std::string const & flow_cell_product_code,
        std::string const & protocol_name,
        std::string const & protocol_run_id,
        std::int64_t protocol_start_time,
        std::string const & sample_id,
        std::uint16_t sample_rate,
        std::string const & sequencing_kit,
        std::string const & sequencer_position,
        std::string const & sequencer_position_type,
        std::string const & software,
        std::string const & system_name,
        std::string const & system_type,
        MapType const & tracking_id)
    : acquisition_id(acquisition_id)
    , acquisition_start_time(acquisition_start_time)
    , adc_max(adc_max)
    , adc_min(adc_min)
    , context_tags(context_tags)
    , experiment_name(experiment_name)
    , flow_cell_id(flow_cell_id)
    , flow_cell_product_code(flow_cell_product_code)
    , protocol_name(protocol_name)
    , protocol_run_id(protocol_run_id)
    , protocol_start_time(protocol_start_time)
    , sample_id(sample_id)
    , sample_rate(sample_rate)
    , sequencing_kit(sequencing_kit)
    , sequencer_position(sequencer_position)
    , sequencer_position_type(sequencer_position_type)
    , software(software)
    , system_name(system_name)
    , system_type(system_type)
    , tracking_id(tracking_id)
    {
    }

    static std::int64_t convert_from_system_clock(std::chrono::system_clock::time_point value)
    {
        return value.time_since_epoch() / std::chrono::milliseconds(1);
    }

    static std::chrono::system_clock::time_point convert_to_system_clock(
        std::int64_t since_epoch_ms)
    {
        return std::chrono::system_clock::time_point() + std::chrono::milliseconds(since_epoch_ms);
    }

    std::string acquisition_id;
    std::int64_t acquisition_start_time;
    std::int16_t adc_max;
    std::int16_t adc_min;
    MapType context_tags;
    std::string experiment_name;
    std::string flow_cell_id;
    std::string flow_cell_product_code;
    std::string protocol_name;
    std::string protocol_run_id;
    std::int64_t protocol_start_time;
    std::string sample_id;
    std::uint16_t sample_rate;
    std::string sequencing_kit;
    std::string sequencer_position;
    std::string sequencer_position_type;
    std::string software;
    std::string system_name;
    std::string system_type;
    MapType tracking_id;
};

inline bool operator==(RunInfoData const & a, RunInfoData const & b)
{
    return a.acquisition_id == b.acquisition_id
           && a.acquisition_start_time == b.acquisition_start_time && a.adc_max == b.adc_max
           && a.adc_min == b.adc_min && a.context_tags == b.context_tags
           && a.experiment_name == b.experiment_name && a.flow_cell_id == b.flow_cell_id
           && a.flow_cell_product_code == b.flow_cell_product_code
           && a.protocol_name == b.protocol_name && a.protocol_run_id == b.protocol_run_id
           && a.protocol_start_time == b.protocol_start_time && a.sample_id == b.sample_id
           && a.sample_rate == b.sample_rate && a.sequencing_kit == b.sequencing_kit
           && a.sequencer_position == b.sequencer_position
           && a.sequencer_position_type == b.sequencer_position_type && a.software == b.software
           && a.system_name == b.system_name && a.system_type == b.system_type
           && a.tracking_id == b.tracking_id;
}

enum class ReadEndReason : std::uint8_t {
    unknown,
    mux_change,
    unblock_mux_change,
    data_service_unblock_mux_change,
    signal_positive,
    signal_negative,

    last_end_reason = signal_negative
};

inline char const * end_reason_as_string(ReadEndReason reason)
{
    static_assert(
        ReadEndReason::last_end_reason == ReadEndReason::signal_negative,
        "Need to add new end reason to this function");
    switch (reason) {
    case ReadEndReason::mux_change:
        return "mux_change";
    case ReadEndReason::unblock_mux_change:
        return "unblock_mux_change";
    case ReadEndReason::data_service_unblock_mux_change:
        return "data_service_unblock_mux_change";
    case ReadEndReason::signal_positive:
        return "signal_positive";
    case ReadEndReason::signal_negative:
        return "signal_negative";
    case ReadEndReason::unknown:
        break;
    }
    return "unknown";
}

inline ReadEndReason end_reason_from_string(std::string const & reason)
{
    static_assert(
        ReadEndReason::last_end_reason == ReadEndReason::signal_negative,
        "Need to add new end reason to this function");
    if (reason == "unknown") {
        return ReadEndReason::unknown;
    } else if (reason == "mux_change") {
        return ReadEndReason::mux_change;
    } else if (reason == "unblock_mux_change") {
        return ReadEndReason::unblock_mux_change;
    } else if (reason == "data_service_unblock_mux_change") {
        return ReadEndReason::data_service_unblock_mux_change;
    } else if (reason == "signal_positive") {
        return ReadEndReason::signal_positive;
    } else if (reason == "signal_negative") {
        return ReadEndReason::signal_negative;
    }

    return ReadEndReason::unknown;
}

/// \brief Input query to a search for a number of read ids in a file:
class POD5_FORMAT_EXPORT ReadIdSearchInput {
public:
    struct InputId {
        boost::uuids::uuid id;
        std::size_t index;
    };

    ReadIdSearchInput(gsl::span<boost::uuids::uuid const> const & input_ids);

    std::size_t read_id_count() const { return m_search_read_ids.size(); }

    InputId const & operator[](std::size_t i) const { return m_search_read_ids[i]; }

private:
    std::vector<InputId> m_search_read_ids;
};

}  // namespace pod5
