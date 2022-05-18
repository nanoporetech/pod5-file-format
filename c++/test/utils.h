#pragma once

#include "pod5_format/read_table_utils.h"

#include <arrow/result.h>
#include <catch2/catch.hpp>

namespace Catch {
template <typename T>
struct StringMaker<arrow::Result<T>> {
    static std::string convert(arrow::Result<T> const& value) { return value.status().ToString(); }
};
}  // namespace Catch

inline pod5::RunInfoData get_test_run_info_data(std::string suffix = "",
                                                std::int16_t adc_center_offset = 0,
                                                std::int16_t sample_rate = 4000) {
    return pod5::RunInfoData(
            "acquisition_id" + suffix, 1005, 4095 + adc_center_offset, -4096 + adc_center_offset,
            {{"context" + suffix, "tags" + suffix},
             {"other" + suffix, "tagz" + suffix},
             {"third" + suffix, "thing" + suffix}},
            "experiment_name" + suffix, "flow_cell_id" + suffix, "flow_cell_product_code" + suffix,
            "protocol_name" + suffix, "protocol_run_id" + suffix, 200005, "sample_id" + suffix,
            sample_rate, "sequencing_kit" + suffix, "sequencer_position" + suffix,
            "sequencer_position_type" + suffix, "software" + suffix, "system_name" + suffix,
            "system_type" + suffix, {{"tracking" + suffix, "id" + suffix}});
}

inline pod5::CalibrationData get_test_calibration_data() {
    return pod5::CalibrationData(100.0f, 2.0f);
}

inline pod5::EndReasonData get_test_end_reason_data() {
    return pod5::EndReasonData(pod5::EndReasonData::ReadEndReason::mux_change, false);
}

inline pod5::PoreData get_test_pore_data() { return pod5::PoreData(431, 3, "Pore_type"); }