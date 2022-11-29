#pragma once

#include "pod5_format/read_table_utils.h"

#include <arrow/result.h>
#include <arrow/util/io_util.h>
#include <catch2/catch.hpp>

namespace Catch {
template <typename T>
struct StringMaker<arrow::Result<T>> {
    static std::string convert(arrow::Result<T> const & value) { return value.status().ToString(); }
};
}  // namespace Catch

inline pod5::RunInfoData get_test_run_info_data(
    std::string suffix = "",
    std::int16_t adc_center_offset = 0,
    std::int16_t sample_rate = 4000)
{
    return pod5::RunInfoData(
        "acquisition_id" + suffix,
        1005,
        4095 + adc_center_offset,
        -4096 + adc_center_offset,
        {{"context" + suffix, "tags" + suffix},
         {"other" + suffix, "tagz" + suffix},
         {"third" + suffix, "thing" + suffix}},
        "experiment_name" + suffix,
        "flow_cell_id" + suffix,
        "flow_cell_product_code" + suffix,
        "protocol_name" + suffix,
        "protocol_run_id" + suffix,
        200005,
        "sample_id" + suffix,
        sample_rate,
        "sequencing_kit" + suffix,
        "sequencer_position" + suffix,
        "sequencer_position_type" + suffix,
        "software" + suffix,
        "system_name" + suffix,
        "system_type" + suffix,
        {{"tracking" + suffix, "id" + suffix}});
}

inline arrow::Status remove_file_if_exists(std::string const & file)
{
    ARROW_ASSIGN_OR_RAISE(
        auto arrow_reads_path, ::arrow::internal::PlatformFilename::FromString(file));
    ARROW_ASSIGN_OR_RAISE(bool file_exists, arrow::internal::FileExists(arrow_reads_path));
    if (file_exists) {
        ARROW_RETURN_NOT_OK(arrow::internal::DeleteFile(arrow_reads_path));
    }
    return arrow::Status::OK();
}
