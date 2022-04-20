#pragma once

#include "mkr_format/read_table_utils.h"

#include <arrow/result.h>
#include <catch2/catch.hpp>

namespace Catch {
template <typename T>
struct StringMaker<arrow::Result<T>> {
    static std::string convert(arrow::Result<T> const& value) { return value.status().ToString(); }
};
}  // namespace Catch

inline mkr::RunInfoData get_test_run_info_data(std::string suffix = "",
                                               std::int16_t adc_center_offset = 0,
                                               std::int16_t sample_rate = 4000) {
    return mkr::RunInfoData(
            "acquisition_id" + suffix, std::chrono::system_clock::now(), 4095 + adc_center_offset,
            -4096 + adc_center_offset,
            {{"context" + suffix, "tags" + suffix}, {"other" + suffix, "tagz" + suffix}},
            "experiment_name" + suffix, "flow_cell_id" + suffix, "flow_cell_product_code" + suffix,
            "protocol_name" + suffix, "protocol_run_id" + suffix,
            std::chrono::system_clock::now() - std::chrono::seconds(5), "sample_id" + suffix,
            sample_rate, "sequencing_kit" + suffix, "sequencer_position" + suffix,
            "sequencer_position_type" + suffix, "software" + suffix, "system_name" + suffix,
            "system_type" + suffix, {{"tracking" + suffix, "id" + suffix}});
}

inline mkr::CalibrationData get_test_calibration_data() {
    return mkr::CalibrationData(100.0f, 2.0f);
}

inline mkr::EndReasonData get_test_end_reason_data() {
    return mkr::EndReasonData(mkr::EndReasonData::ReadEndReason::mux_change, false);
}

inline mkr::PoreData get_test_pore_data() { return mkr::PoreData(431, 3, "Pore_type"); }