#pragma once

#include <boost/uuid/uuid.hpp>
#include <gsl/gsl-lite.hpp>

#include <chrono>
#include <cstdint>
#include <map>
#include <string>

namespace mkr {

using SignalTableRowIndex = std::uint64_t;

using PoreDictionaryIndex = std::int16_t;
using CalibrationDictionaryIndex = std::int16_t;
using EndReasonDictionaryIndex = std::int16_t;
using RunInfoDictionaryIndex = std::int16_t;

class ReadData {
public:
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

    boost::uuids::uuid read_id;
    PoreDictionaryIndex pore;
    CalibrationDictionaryIndex calibration;
    std::uint32_t read_number;
    std::uint64_t start_sample;
    float median_before;
    EndReasonDictionaryIndex end_reason;
    RunInfoDictionaryIndex run_info;
};

class RunInfoData {
public:
    RunInfoData(std::string acquisition_id,
                std::chrono::steady_clock::time_point acquisition_start_time,
                std::int16_t adc_max,
                std::int16_t adc_min,
                std::map<std::string, std::string> context_tags,
                std::string experiment_name,
                std::string flow_cell_id,
                std::string flow_cell_product_code,
                std::string protocol_name,
                std::string protocol_run_id,
                std::chrono::steady_clock::time_point protocol_start_time,
                std::string sample_id,
                std::uint16_t sample_rate,
                std::string sequencing_kit,
                std::string sequencer_position,
                std::string sequencer_position_type,
                std::string software,
                std::string system_name,
                std::string system_type,
                std::map<std::string, std::string> tracking_id)
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

    std::string acquisition_id;
    std::chrono::steady_clock::time_point acquisition_start_time;
    std::int16_t adc_max;
    std::int16_t adc_min;
    std::map<std::string, std::string> context_tags;
    std::string experiment_name;
    std::string flow_cell_id;
    std::string flow_cell_product_code;
    std::string protocol_name;
    std::string protocol_run_id;
    std::chrono::steady_clock::time_point protocol_start_time;
    std::string sample_id;
    std::uint16_t sample_rate;
    std::string sequencing_kit;
    std::string sequencer_position;
    std::string sequencer_position_type;
    std::string software;
    std::string system_name;
    std::string system_type;
    std::map<std::string, std::string> tracking_id;
};

class PoreData {
public:
    PoreData(std::uint16_t channel, std::uint8_t well, char const* pore_type)
            : channel(channel), well(well), pore_type(pore_type) {}

    std::uint16_t channel;
    std::uint8_t well;
    std::string pore_type;
};

class CalibrationData {
public:
    CalibrationData(float offset, float scale) : offset(offset), scale(scale) {}

    float offset;
    float scale;
};

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

    EndReasonData(ReadEndReason end_reason, bool forced) : end_reason(end_reason), forced(forced) {}

    char const* end_reason_as_string() const {
        switch (end_reason) {
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

    ReadEndReason end_reason;
    bool forced;
};

}  // namespace mkr