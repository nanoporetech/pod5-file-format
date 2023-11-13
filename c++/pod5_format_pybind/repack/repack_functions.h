#pragma once

#include "pod5_format/internal/tracing/tracing.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_builder.h"
#include "pod5_format/signal_table_schema.h"
#include "repack_utils.h"

#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <boost/uuid/uuid_hash.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <numeric>
#include <unordered_set>

namespace repack {

struct ReadReadData {
    std::shared_ptr<pod5::FileReader> input;
    std::vector<pod5::ReadData> reads;
    std::vector<std::size_t> signal_durations;
    std::vector<std::size_t> signal_row_sizes;

    std::vector<boost::uuids::uuid> signal_rows_read_ids;
    std::vector<std::uint64_t> signal_rows;
};

arrow::Result<ReadReadData> read_read_data(
    ReadsTableDictionaryThreadCache & reads_table_cache,
    states::unread_read_table_rows && in_batch)
{
    POD5_TRACE_FUNCTION();

    auto const & source_file = in_batch.input;
    ARROW_ASSIGN_OR_RAISE(
        auto source_read_table_batch, source_file->read_read_record_batch(in_batch.batch_index));

    ARROW_ASSIGN_OR_RAISE(auto columns, source_read_table_batch.columns());

    auto source_reads_pore_type_column =
        std::static_pointer_cast<arrow::Int16Array>(columns.pore_type->indices());
    auto source_reads_end_reason_column =
        std::static_pointer_cast<arrow::Int16Array>(columns.end_reason->indices());
    auto source_reads_run_info_column =
        std::static_pointer_cast<arrow::Int16Array>(columns.run_info->indices());
    auto source_reads_signal_column = source_read_table_batch.signal_column();

    auto batch_rows = std::move(in_batch.batch_rows);
    if (batch_rows.empty()) {
        auto const source_batch_row_count = source_read_table_batch.num_rows();
        batch_rows.resize(source_batch_row_count);
        std::iota(batch_rows.begin(), batch_rows.end(), 0);
    }

    ReadReadData result;
    result.input = source_file;
    result.reads.reserve(batch_rows.size());
    result.signal_rows.reserve(batch_rows.size());
    result.signal_row_sizes.reserve(batch_rows.size());
    for (std::size_t batch_row_index = 0; batch_row_index < batch_rows.size(); ++batch_row_index) {
        auto batch_row = batch_rows[batch_row_index];
        // Find the read params
        auto const & read_id = columns.read_id->Value(batch_row);
        auto const & read_number = columns.read_number->Value(batch_row);
        auto const & start_sample = columns.start_sample->Value(batch_row);
        auto const & channel = columns.channel->Value(batch_row);
        auto const & well = columns.well->Value(batch_row);
        auto const & calibration_offset = columns.calibration_offset->Value(batch_row);
        auto const & calibration_scale = columns.calibration_scale->Value(batch_row);
        auto const & median_before = columns.median_before->Value(batch_row);
        auto const & end_reason_forced = columns.end_reason_forced->Value(batch_row);
        auto const & num_minknow_events = columns.num_minknow_events->Value(batch_row);
        auto const & tracked_scaling_scale = columns.tracked_scaling_scale->Value(batch_row);
        auto const & tracked_scaling_shift = columns.tracked_scaling_shift->Value(batch_row);
        auto const & predicted_scaling_scale = columns.predicted_scaling_scale->Value(batch_row);
        auto const & predicted_scaling_shift = columns.predicted_scaling_shift->Value(batch_row);
        auto const & num_reads_since_mux_change =
            columns.num_reads_since_mux_change->Value(batch_row);
        auto const & time_since_mux_change = columns.time_since_mux_change->Value(batch_row);
        auto const & num_samples = columns.num_samples->Value(batch_row);

        auto const & pore_type_index = source_reads_pore_type_column->Value(batch_row);
        auto const & end_reason_index = source_reads_end_reason_column->Value(batch_row);
        auto const & run_info_index = source_reads_run_info_column->Value(batch_row);

        ARROW_ASSIGN_OR_RAISE(
            auto dest_pore_index,
            reads_table_cache.find_pore_index(
                source_file, source_read_table_batch, pore_type_index));
        ARROW_ASSIGN_OR_RAISE(
            auto dest_run_info_index,
            reads_table_cache.find_run_info_index(
                source_file, source_read_table_batch, run_info_index));

        result.reads.emplace_back(
            read_id,
            read_number,
            start_sample,
            channel,
            well,
            dest_pore_index,
            calibration_offset,
            calibration_scale,
            median_before,
            end_reason_index,
            end_reason_forced,
            dest_run_info_index,
            num_minknow_events,
            tracked_scaling_scale,
            tracked_scaling_shift,
            predicted_scaling_scale,
            predicted_scaling_shift,
            num_reads_since_mux_change,
            time_since_mux_change);
        result.signal_durations.emplace_back(num_samples);

        auto const signal_rows = std::static_pointer_cast<arrow::UInt64Array>(
            source_reads_signal_column->value_slice(batch_row));
        auto const signal_rows_span =
            gsl::make_span(signal_rows->raw_values(), signal_rows->length());

        result.signal_rows.insert(
            result.signal_rows.end(), signal_rows_span.begin(), signal_rows_span.end());
        for (std::size_t i = 0; i < signal_rows_span.size(); ++i) {
            result.signal_rows_read_ids.emplace_back(read_id);
        }
        result.signal_row_sizes.emplace_back(signal_rows_span.size());
    }
    return result;
}

arrow::Status read_signal(
    std::shared_ptr<pod5::FileReader> const & source_file,
    pod5::SignalType input_compression_type,
    std::uint64_t abs_signal_row,
    boost::uuids::uuid read_id,
    pod5::SignalType output_compression_type,
    arrow::FixedSizeBinaryBuilder & read_id_builder,
    pod5::SignalBuilderVariant & signal_builder,
    arrow::UInt32Builder & samples_builder,
    arrow::MemoryPool * pool)
{
    auto signal_rows_span = gsl::make_span(&abs_signal_row, 1);

    // If were using the same compression type in both files, just copy compressed:
    if (input_compression_type == output_compression_type
        && output_compression_type == pod5::SignalType::VbzSignal)
    {
        std::vector<uint32_t> sample_counts;
        ARROW_ASSIGN_OR_RAISE(
            auto extracted_signal,
            source_file->extract_samples_inplace(signal_rows_span, sample_counts));

        assert(1 == extracted_signal.size());
        assert(sample_counts.size() == extracted_signal.size());
        auto signal_span =
            gsl::make_span(extracted_signal.front()->data(), extracted_signal.front()->size());

        ARROW_RETURN_NOT_OK(read_id_builder.Append(read_id.begin()));
        ARROW_RETURN_NOT_OK(boost::apply_visitor(
            pod5::visitors::append_pre_compressed_signal{signal_span}, signal_builder));
        ARROW_RETURN_NOT_OK(samples_builder.Append(sample_counts.front()));
    } else {
        // Find the sample count of the complete read:
        ARROW_ASSIGN_OR_RAISE(
            auto sample_count, source_file->extract_sample_count(signal_rows_span));

        std::vector<std::int16_t> signal(sample_count);
        auto signal_buffer_span = gsl::make_span(signal);
        ARROW_RETURN_NOT_OK(source_file->extract_samples(signal_rows_span, signal_buffer_span));

        ARROW_RETURN_NOT_OK(read_id_builder.Append(read_id.begin()));
        ARROW_RETURN_NOT_OK(boost::apply_visitor(
            pod5::visitors::append_signal{signal_buffer_span, pool}, signal_builder));
        ARROW_RETURN_NOT_OK(samples_builder.Append(sample_count));
    }
    return arrow::Status::OK();
}

struct RequestedSignalReads {
    std::vector<states::shared_variant> complete_requests;
    std::shared_ptr<states::read_split_signal_table_batch_rows> partial_request;
};

arrow::Result<RequestedSignalReads> request_signal_reads(
    std::shared_ptr<pod5::FileReader> const & source_file,
    pod5::SignalType output_compression_type,
    std::size_t signal_batch_size,
    std::vector<boost::uuids::uuid> read_ids,
    std::vector<std::uint64_t> signal_rows,
    std::shared_ptr<states::read_split_signal_table_batch_rows> const & partial_request,
    std::shared_ptr<states::read_read_table_rows_no_signal> const & dest_read_table_rows,
    arrow::MemoryPool * pool)
{
    POD5_TRACE_FUNCTION();

    auto const input_signal_type = source_file->signal_type();

    assert(read_ids.size() == signal_rows.size());

    RequestedSignalReads result;
    auto next_request = partial_request;

    assert(signal_rows.size() == dest_read_table_rows->signal_row_indices.size());

    std::size_t signal_rows_position = 0;
    while (signal_rows_position < signal_rows.size()) {
        if (!next_request) {
            ARROW_ASSIGN_OR_RAISE(
                auto signal_builder, pod5::make_signal_builder(output_compression_type, pool));
            next_request = std::make_shared<states::read_split_signal_table_batch_rows>(
                std::move(signal_builder), pool);
            next_request->patch_rows.reserve(signal_batch_size);
        }
        auto to_write = std::min(
            signal_rows.size() - signal_rows_position,
            signal_batch_size - next_request->patch_rows.size());

        for (std::size_t i = 0; i < to_write; ++i) {
            auto const dest_batch_row_index = signal_rows_position + i;
            assert(dest_batch_row_index < signal_rows.size());
            assert(dest_batch_row_index < dest_read_table_rows->signal_row_indices.size());

            ARROW_RETURN_NOT_OK(read_signal(
                source_file,
                input_signal_type,
                signal_rows[signal_rows_position + i],
                read_ids[signal_rows_position + i],
                output_compression_type,
                *next_request->read_id_builder,
                next_request->signal_builder,
                next_request->samples_builder,
                pool));

            next_request->patch_rows.emplace_back(dest_read_table_rows, dest_batch_row_index);
        }
        signal_rows_position += to_write;

        assert(next_request->row_count() <= signal_batch_size);
        assert(next_request->row_count() <= signal_batch_size);
        if (next_request->row_count() >= signal_batch_size) {
            result.complete_requests.emplace_back(std::move(next_request));
            next_request.reset();
        }
    }

    result.partial_request = next_request;
    return result;
}

struct ReadSignal {
    std::size_t row_count;
    bool final_batch;
    std::vector<std::shared_ptr<arrow::Array>> columns;
};

arrow::Result<ReadSignal> read_signal_data(states::read_split_signal_table_batch_rows & signal_rows)
{
    POD5_TRACE_FUNCTION();

    ReadSignal result;

    pod5::SignalTableSchemaDescription field_locations;
    result.final_batch = signal_rows.final_batch;
    result.row_count = signal_rows.row_count();
    result.columns = {nullptr, nullptr, nullptr};
    ARROW_RETURN_NOT_OK(
        signal_rows.read_id_builder->Finish(&result.columns[field_locations.read_id]));
    ARROW_RETURN_NOT_OK(boost::apply_visitor(
        pod5::visitors::finish_column{&result.columns[field_locations.signal]},
        signal_rows.signal_builder));
    ARROW_RETURN_NOT_OK(
        signal_rows.samples_builder.Finish(&result.columns[field_locations.samples]));
    return result;
}

arrow::Status write_reads(
    std::shared_ptr<pod5::FileWriter> const & output,
    std::vector<pod5::ReadData> const & reads,
    std::vector<std::size_t> const & signal_durations,
    std::vector<std::size_t> const & signal_row_sizes,
    std::vector<pod5::SignalTableRowIndex> const & signal_row_indices)
{
    POD5_TRACE_FUNCTION();
    std::size_t signal_position = 0;
    auto signal_indices_span = gsl::make_span(signal_row_indices);
    for (std::size_t i = 0; i < reads.size(); ++i) {
        auto signal_rows = signal_indices_span.subspan(signal_position, signal_row_sizes[i]);
        signal_position += signal_row_sizes[i];

        ARROW_RETURN_NOT_OK(output->add_complete_read(reads[i], signal_rows, signal_durations[i]));
    }

    return arrow::Status::OK();
}

arrow::Status check_duplicate_read_ids(
    std::unordered_set<boost::uuids::uuid> & output_read_ids,
    std::vector<pod5::ReadData> const & new_reads)
{
    for (auto const & read : new_reads) {
        auto result = output_read_ids.insert(read.read_id);
        if (!result.second) {
            return arrow::Status::Invalid(
                "Duplicate read id ", to_string(read.read_id), " found in file");
        }
    }

    return arrow::Status::OK();
}

}  // namespace repack
