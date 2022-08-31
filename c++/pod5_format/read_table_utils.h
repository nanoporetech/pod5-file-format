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
using CalibrationDictionaryIndex = std::int16_t;
using EndReasonDictionaryIndex = std::int16_t;
using RunInfoDictionaryIndex = std::int16_t;

class ReadData {
public:
    ReadData() = default;
    /// \brief Create a new read data structure to add to a read.
    /// \param read_id The read id for the read entry.
    /// \param pore Pore dictionary index which this read was sequenced with.
    /// \param calibration Calibration dictionary index which is used to calibrate this read.
    /// \param read_number Read number for this read.
    /// \param start_sample The sample which this read starts at.
    /// \param median_before The median of the read chunk prior to the start of this read.
    /// \param end_reason The dictionary index of the end reason which caused this read to complete.
    /// \param run_info The dictionary index of the run info for this read.
    ReadData(boost::uuids::uuid const& read_id,
             PoreDictionaryIndex pore,
             CalibrationDictionaryIndex calibration,
             std::uint32_t read_number,
             std::uint64_t start_sample,
             float median_before,
             EndReasonDictionaryIndex end_reason,
             RunInfoDictionaryIndex run_info)
            : read_id(read_id),
              pore(pore),
              calibration(calibration),
              read_number(read_number),
              start_sample(start_sample),
              median_before(median_before),
              end_reason(end_reason),
              run_info(run_info) {}

    void set_v1_fields(uint64_t num_minknow_events_in,
                       float tracked_scaling_scale_in,
                       float tracked_scaling_shift_in,
                       float predicted_scaling_scale_in,
                       float predicted_scaling_shift_in,
                       bool trust_tracked_scale_in,
                       bool trust_tracked_shift_in) {
        num_minknow_events = num_minknow_events_in;
        tracked_scaling_scale = tracked_scaling_scale_in;
        tracked_scaling_shift = tracked_scaling_shift_in;
        predicted_scaling_scale = predicted_scaling_scale_in;
        predicted_scaling_shift = predicted_scaling_shift_in;
        trust_tracked_scale = trust_tracked_scale_in;
        trust_tracked_shift = trust_tracked_shift_in;
    }

    boost::uuids::uuid read_id;
    PoreDictionaryIndex pore;
    CalibrationDictionaryIndex calibration;
    std::uint32_t read_number;
    std::uint64_t start_sample;
    float median_before;
    EndReasonDictionaryIndex end_reason;
    RunInfoDictionaryIndex run_info;

    uint64_t num_minknow_events = 0;
    float tracked_scaling_scale = std::numeric_limits<float>::quiet_NaN();
    float tracked_scaling_shift = std::numeric_limits<float>::quiet_NaN();
    float predicted_scaling_scale = std::numeric_limits<float>::quiet_NaN();
    float predicted_scaling_shift = std::numeric_limits<float>::quiet_NaN();
    bool trust_tracked_scale = false;
    bool trust_tracked_shift = false;
};

inline bool operator==(ReadData const& a, ReadData const& b) {
    return a.read_id == b.read_id && a.pore == b.pore && a.calibration == b.calibration &&
           a.read_number == b.read_number && a.start_sample == b.start_sample &&
           a.median_before == b.median_before && a.end_reason == b.end_reason &&
           a.run_info == b.run_info;
}

class RunInfoData {
public:
    using MapType = std::vector<std::pair<std::string, std::string>>;
    RunInfoData(std::string const& acquisition_id,
                std::int64_t acquisition_start_time,
                std::int16_t adc_max,
                std::int16_t adc_min,
                MapType const& context_tags,
                std::string const& experiment_name,
                std::string const& flow_cell_id,
                std::string const& flow_cell_product_code,
                std::string const& protocol_name,
                std::string const& protocol_run_id,
                std::int64_t protocol_start_time,
                std::string const& sample_id,
                std::uint16_t sample_rate,
                std::string const& sequencing_kit,
                std::string const& sequencer_position,
                std::string const& sequencer_position_type,
                std::string const& software,
                std::string const& system_name,
                std::string const& system_type,
                MapType const& tracking_id)
            : acquisition_id(acquisition_id),
              acquisition_start_time(acquisition_start_time),
              adc_max(adc_max),
              adc_min(adc_min),
              context_tags(context_tags),
              experiment_name(experiment_name),
              flow_cell_id(flow_cell_id),
              flow_cell_product_code(flow_cell_product_code),
              protocol_name(protocol_name),
              protocol_run_id(protocol_run_id),
              protocol_start_time(protocol_start_time),
              sample_id(sample_id),
              sample_rate(sample_rate),
              sequencing_kit(sequencing_kit),
              sequencer_position(sequencer_position),
              sequencer_position_type(sequencer_position_type),
              software(software),
              system_name(system_name),
              system_type(system_type),
              tracking_id(tracking_id) {}

    static std::int64_t convert_from_system_clock(std::chrono::system_clock::time_point value) {
        return value.time_since_epoch() / std::chrono::milliseconds(1);
    }

    static std::chrono::system_clock::time_point convert_to_system_clock(
            std::int64_t since_epoch_ms) {
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

inline bool operator==(RunInfoData const& a, RunInfoData const& b) {
    return a.acquisition_id == b.acquisition_id &&
           a.acquisition_start_time == b.acquisition_start_time && a.adc_max == b.adc_max &&
           a.adc_min == b.adc_min && a.context_tags == b.context_tags &&
           a.experiment_name == b.experiment_name && a.flow_cell_id == b.flow_cell_id &&
           a.flow_cell_product_code == b.flow_cell_product_code &&
           a.protocol_name == b.protocol_name && a.protocol_run_id == b.protocol_run_id &&
           a.protocol_start_time == b.protocol_start_time && a.sample_id == b.sample_id &&
           a.sample_rate == b.sample_rate && a.sequencing_kit == b.sequencing_kit &&
           a.sequencer_position == b.sequencer_position &&
           a.sequencer_position_type == b.sequencer_position_type && a.software == b.software &&
           a.system_name == b.system_name && a.system_type == b.system_type &&
           a.tracking_id == b.tracking_id;
}

class PoreData {
public:
    PoreData(std::uint16_t channel, std::uint8_t well, char const* pore_type)
            : channel(channel), well(well), pore_type(pore_type) {}

    PoreData(std::uint16_t channel, std::uint8_t well, std::string&& pore_type)
            : channel(channel), well(well), pore_type(std::move(pore_type)) {}

    std::uint16_t channel;
    std::uint8_t well;
    std::string pore_type;
};

inline bool operator==(PoreData const& a, PoreData const& b) {
    return a.channel == b.channel && a.well == b.well && a.pore_type == b.pore_type;
}

class CalibrationData {
public:
    CalibrationData(float offset, float scale) : offset(offset), scale(scale) {}

    float offset;
    float scale;
};

inline bool operator==(CalibrationData const& a, CalibrationData const& b) {
    return a.offset == b.offset && a.scale == b.scale;
}

class EndReasonData {
public:
    enum class ReadEndReason {
        unknown,
        mux_change,
        unblock_mux_change,
        data_service_unblock_mux_change,
        signal_positive,
        signal_negative
    };

    EndReasonData(ReadEndReason name, bool forced)
            : name(end_reason_as_string(name)), forced(forced) {}
    EndReasonData(std::string&& name, bool forced) : name(name), forced(forced) {}

    static char const* end_reason_as_string(ReadEndReason reason) {
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

    std::string name;
    bool forced;
};

inline bool operator==(EndReasonData const& a, EndReasonData const& b) {
    return a.name == b.name && a.forced == b.forced;
}

/// \brief Input query to a search for a number of read ids in a file:
class POD5_FORMAT_EXPORT ReadIdSearchInput {
public:
    struct InputId {
        boost::uuids::uuid id;
        std::size_t index;
    };

    ReadIdSearchInput(gsl::span<boost::uuids::uuid const> const& input_ids);

    std::size_t read_id_count() const { return m_search_read_ids.size(); }

    InputId const& operator[](std::size_t i) const { return m_search_read_ids[i]; }

private:
    std::vector<InputId> m_search_read_ids;
};

}  // namespace pod5
