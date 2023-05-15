#pragma once

#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/schema_metadata.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/status.h>

#include <iostream>

namespace pod5 {

static constexpr char const * kArrowMagicBytes = "ARROW1";

struct RecoveredData {
    // Metadata from the original file:
    SchemaMetadataDescription metadata;

    std::size_t recovered_batches = 0;
    arrow::Status failed_batch_status;
    std::size_t recovered_rows = 0;
};

template <typename DestFileType>
arrow::Result<RecoveredData> recover_arrow_file(
    std::shared_ptr<arrow::io::RandomAccessFile> const & file_to_recover,
    DestFileType const & destination_file)
{
    // Check for arrow start file:
    const int32_t magic_size = static_cast<int>(::strlen(kArrowMagicBytes));
    ARROW_ASSIGN_OR_RAISE(auto buffer, file_to_recover->ReadAt(0, magic_size));
    if (buffer->size() < magic_size || memcmp(buffer->data(), kArrowMagicBytes, magic_size)) {
        return arrow::Status::Invalid("Not an Arrow file");
    }

    // Open the stream format within the ipc file:
    ARROW_ASSIGN_OR_RAISE(
        auto input_stream, combined_file_utils::open_sub_file(file_to_recover, 8));
    ARROW_ASSIGN_OR_RAISE(
        auto opened_stream, arrow::ipc::RecordBatchStreamReader::Open(input_stream));

    auto const & expected_schema = destination_file->schema();
    auto schema = opened_stream->schema();
    if (!schema->Equals(*expected_schema, false)) {
        return arrow::Status::Invalid(
            "Recovered file Schema does not match expected schema, version mismatch?");
    }

    RecoveredData recovered_data;
    ARROW_ASSIGN_OR_RAISE(
        recovered_data.metadata, read_schema_key_value_metadata(schema->metadata()));
    while (true) {
        auto result_opt = opened_stream->Next();
        // Check if the batch failed to load:
        if (!result_opt.ok()) {
            recovered_data.failed_batch_status = result_opt.status();
            return recovered_data;
        }

        auto & result = *result_opt;
        if (!result) {
            break;
        }

        recovered_data.recovered_batches += 1;
        recovered_data.recovered_rows += result->num_rows();
        ARROW_RETURN_NOT_OK(destination_file->write_batch(*result));
    }

    return recovered_data;
}

}  // namespace pod5
