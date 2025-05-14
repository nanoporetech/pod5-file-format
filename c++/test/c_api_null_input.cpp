#include "c_api_test_utils.h"
#include "pod5_format/c_api.h"
#include "utils.h"

#include <bit>
#include <numeric>
#include <string_view>

namespace {

void pod5_reset_error()
{
    pod5_vbz_compressed_signal_max_size(1);
    REQUIRE_POD5_OK(pod5_get_error_no());
    REQUIRE(pod5_get_error_string() == std::string_view{});
}

namespace detail {

template <std::size_t PtrIdx, typename... Args>
constexpr std::size_t ptr_idx_to_arg_idx()
{
    // Count how many pointers we've seen at each arg.
    std::size_t ptr_count[]{static_cast<std::size_t>(std::is_pointer_v<Args>)...};
    std::partial_sum(std::begin(ptr_count), std::end(ptr_count), std::begin(ptr_count));

    // Find which arg matches our index.
    for (std::size_t arg_i = 0; arg_i < std::size(ptr_count); arg_i++) {
        if (ptr_count[arg_i] == PtrIdx + 1) {
            return arg_i;
        }
    }

    throw "Cannot find arg for ptr";
}

template <std::size_t PtrIdx, typename... Args>
void make_ptr_null_impl(std::tuple<Args...> & args, std::uint64_t valid_ptr_bitset)
{
    // Grab the arg that we'll be modifying.
    constexpr std::size_t ArgIdx = ptr_idx_to_arg_idx<PtrIdx, Args...>();
    auto & arg = std::get<ArgIdx>(args);
    using ArgT = std::remove_reference_t<decltype(arg)>;
    static_assert(std::is_pointer_v<ArgT>);

    // If the arg isn't a valid one then replace it with a nullptr.
    auto const valid = (valid_ptr_bitset >> PtrIdx) & 1;
    if (!valid) {
        arg = nullptr;
    }
}

template <typename... Args, std::size_t... PtrIdxs>
void make_ptrs_null(
    std::tuple<Args...> & args,
    std::uint64_t valid_ptr_bitset,
    std::index_sequence<PtrIdxs...>)
{
    (make_ptr_null_impl<PtrIdxs>(args, valid_ptr_bitset), ...);
}

template <typename... Args, std::size_t... ArgIdxs>
auto unpack_and_call(
    pod5_error_t (*func)(Args...),
    std::tuple<Args...> args,
    std::index_sequence<ArgIdxs...>)
{
    return func(std::get<ArgIdxs>(args)...);
}

}  // namespace detail

template <typename... Args>
void call_with_nulls(pod5_error_t (*func)(Args...), Args... args)
{
    auto const valid_inputs = std::make_tuple(args...);

    constexpr std::size_t num_args = sizeof...(Args);
    static_assert(num_args <= 64, "uint64_t isn't big enough for a bitmask");
    constexpr std::size_t num_pointers = (std::is_pointer_v<Args> + ...);

    constexpr auto ArgIdxs = std::make_index_sequence<num_args>();
    constexpr auto PtrIdxs = std::make_index_sequence<num_pointers>();

    // Try every combination of NULL for the pointers.
    for (std::uint64_t valid_ptr_bitset = 0; std::popcount(valid_ptr_bitset) != num_pointers;
         valid_ptr_bitset++)
    {
        CAPTURE(valid_ptr_bitset);

        // Replace some args with nulls.
        auto inputs = valid_inputs;
        detail::make_ptrs_null(inputs, valid_ptr_bitset, PtrIdxs);

        // Make the call.
        pod5_reset_error();
        pod5_error_t const result = detail::unpack_and_call(func, inputs, ArgIdxs);

        // Check that it was an error.
        // TODO: We could improve this to check that the first invalid arg matches the error that's
        // reported (ie null string, null file, etc...), but this is already overengineered enough.
        //int const first_ptr = std::countr_zero(~valid_ptr_bitset); // codespell:ignore
        CHECK_POD5_NOT_OK(result);
        CHECK_THAT(pod5_get_error_string(), Catch::Matchers::Contains("null"));
    }
}

TEST_CASE("NULL input doesn't crash")
{
    using Catch::Matchers::Contains;

    pod5_init();
    auto cleanup = gsl::finally([] { pod5_terminate(); });

    // Make a temporary file for the read API to use.
    static constexpr char const temporary_filename[] = "./foo_c_api.pod5";
    {
        REQUIRE(remove_file_if_exists(temporary_filename).ok());
        Pod5FileWriter_t * writer = pod5_create_file(temporary_filename, "c_software", nullptr);
        REQUIRE_POD5_OK(pod5_get_error_no());
        REQUIRE(writer);

        std::int16_t pore_type_id{};
        REQUIRE_POD5_OK(pod5_add_pore(&pore_type_id, writer, "pore_type"));

        std::int16_t run_info_id{};
        size_t const num_kv_pairs = 1;
        char const * keys[]{"key"};
        char const * values[]{"value"};
        REQUIRE_POD5_OK(pod5_add_run_info(
            &run_info_id,
            writer,
            "acquisition_id",
            1,
            1,
            -1,
            num_kv_pairs,
            keys,
            values,
            "experiment_name",
            "flow_cell_id",
            "flow_cell_product_code",
            "protocol_name",
            "protocol_run_id",
            1,
            "sample_id",
            1,
            "sequencing_kit",
            "sequencer_position",
            "sequencer_position_type",
            "software",
            "system_name",
            "system_type",
            num_kv_pairs,
            keys,
            values));

        read_id_t const read_id{};
        uint32_t const read_number{};
        uint64_t const start_sample{};
        float const median_before{};
        uint16_t const channel{};
        uint8_t const well{};
        float const calibration_offset{};
        float const calibration_scale{};
        pod5_end_reason_t const end_reason{};
        uint8_t const end_reason_forced{};
        uint64_t const num_minknow_events{};
        float const tracked_scaling_scale{};
        float const tracked_scaling_shift{};
        float const predicted_scaling_scale{};
        float const predicted_scaling_shift{};
        uint32_t const num_reads_since_mux_change{};
        float const time_since_mux_change{};

        ReadBatchRowInfoArrayV3 const row_data{
            &read_id,
            &read_number,
            &start_sample,
            &median_before,
            &channel,
            &well,
            &pore_type_id,
            &calibration_offset,
            &calibration_scale,
            &end_reason,
            &end_reason_forced,
            &run_info_id,
            &num_minknow_events,
            &tracked_scaling_scale,
            &tracked_scaling_shift,
            &predicted_scaling_scale,
            &predicted_scaling_shift,
            &num_reads_since_mux_change,
            &time_since_mux_change,
        };

        int16_t const signal_data[]{1, 2, 3, 4, 5};
        uint32_t const signal_size = std::size(signal_data);
        auto * signal_data_ptr = signal_data;

        REQUIRE_POD5_OK(pod5_add_reads_data(
            writer, 1, READ_BATCH_ROW_INFO_VERSION_3, &row_data, &signal_data_ptr, &signal_size));

        REQUIRE_POD5_OK(pod5_close_and_free_writer(writer));
    }

    SECTION("Reader API")
    {
        {
            INFO("pod5_open_file")

            pod5_reset_error();
            CHECK(pod5_open_file(nullptr) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null string passed"));
        }

        {
            INFO("pod5_open_file_options")

            Pod5ReaderOptions_t options{};

            pod5_reset_error();
            CHECK(pod5_open_file_options(nullptr, nullptr) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null string passed"));

            pod5_reset_error();
            CHECK(pod5_open_file_options(temporary_filename, nullptr) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null passed"));

            pod5_reset_error();
            CHECK(pod5_open_file_options(nullptr, &options) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null string passed"));
        }

        {
            INFO("pod5_close_and_free_reader")

            pod5_reset_error();
            CHECK_POD5_OK(pod5_close_and_free_reader(nullptr));
        }

        // The rest of these functions require a reader.
        Pod5FileReader_t * reader = pod5_open_file(temporary_filename);
        REQUIRE(reader);
        auto close_reader = gsl::finally([&reader] { pod5_close_and_free_reader(reader); });

        {
            INFO("pod5_get_file_info")

            FileInfo file_info{};
            call_with_nulls(pod5_get_file_info, reader, &file_info);
        }

        {
            INFO("pod5_get_file_read_table_location")

            EmbeddedFileData_t file_data{};
            call_with_nulls(pod5_get_file_read_table_location, reader, &file_data);
        }

        {
            INFO("pod5_get_file_signal_table_location")

            EmbeddedFileData_t file_data{};
            call_with_nulls(pod5_get_file_signal_table_location, reader, &file_data);
        }

        {
            INFO("pod5_get_file_run_info_table_location")

            EmbeddedFileData_t file_data{};
            call_with_nulls(pod5_get_file_run_info_table_location, reader, &file_data);
        }

        {
            INFO("pod5_get_read_count")

            size_t count{};
            call_with_nulls(pod5_get_read_count, reader, &count);
        }

        {
            INFO("pod5_get_read_ids")

            std::array<read_id_t, 3> read_ids{};
            call_with_nulls(pod5_get_read_ids, reader, read_ids.size(), read_ids.data());
        }

        {
            INFO("pod5_plan_traversal")

            constexpr std::size_t read_id_count = 1;
            uint8_t const read_id_array[read_id_count * 16]{};
            uint32_t batch_counts{};
            uint32_t batch_rows{};
            size_t find_success_count_out{};
            call_with_nulls(
                pod5_plan_traversal,
                reader,
                read_id_array,
                read_id_count,
                &batch_counts,
                &batch_rows,
                &find_success_count_out);
        }

        {
            INFO("pod5_get_read_batch_count")

            size_t count{};
            call_with_nulls(pod5_get_read_batch_count, &count, reader);
        }

        {
            INFO("pod5_get_read_batch")

            Pod5ReadRecordBatch_t * batch = nullptr;
            size_t index{};
            call_with_nulls(pod5_get_read_batch, &batch, reader, index);
        }

        {
            INFO("pod5_free_read_batch")

            pod5_reset_error();
            CHECK_POD5_OK(pod5_free_read_batch(nullptr));
        }

        // The rest of these functions require a batch.
        Pod5ReadRecordBatch_t * batch = nullptr;
        CHECK_POD5_OK(pod5_get_read_batch(&batch, reader, 0));
        REQUIRE(batch);
        auto free_batch = gsl::finally([&batch] { pod5_free_read_batch(batch); });

        {
            INFO("pod5_get_read_batch_row_count")

            size_t count{};
            call_with_nulls(pod5_get_read_batch_row_count, &count, batch);
        }

        {
            INFO("pod5_get_read_batch_row_info_data")

            ReadBatchRowInfoV3 row_info{};
            size_t row = 0;
            uint16_t struct_version = READ_BATCH_ROW_INFO_VERSION;
            uint16_t read_table_version{};
            call_with_nulls(
                pod5_get_read_batch_row_info_data,
                batch,
                row,
                struct_version,
                static_cast<void *>(&row_info),
                &read_table_version);
        }

        {
            INFO("pod5_get_signal_row_indices")

            size_t row = 0;
            uint64_t indices[1];
            call_with_nulls(
                pod5_get_signal_row_indices,
                batch,
                row,
                static_cast<int64_t>(std::size(indices)),
                indices);
        }

        {
            INFO("pod5_get_calibration_extra_info")

            size_t row = 0;
            CalibrationExtraData_t data{};
            call_with_nulls(pod5_get_calibration_extra_info, batch, row, &data);
        }

        {
            INFO("pod5_get_run_info")

            int16_t index = 0;
            RunInfoDictData_t * data = nullptr;
            call_with_nulls(pod5_get_run_info, batch, index, &data);
        }

        {
            INFO("pod5_get_file_run_info")

            run_info_index_t run_info_index = 0;
            RunInfoDictData_t * run_info_data = nullptr;
            call_with_nulls(pod5_get_file_run_info, reader, run_info_index, &run_info_data);
        }

        {
            INFO("pod5_free_run_info")

            pod5_reset_error();
            CHECK_POD5_OK(pod5_free_run_info(nullptr));
        }

        {
            INFO("pod5_release_run_info")

            pod5_reset_error();
#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
            CHECK_POD5_OK(pod5_release_run_info(nullptr));
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
        }

        {
            INFO("pod5_get_file_run_info_count")

            run_info_index_t count{};
            call_with_nulls(pod5_get_file_run_info_count, reader, &count);
        }

        {
            INFO("pod5_get_end_reason")

            int16_t index = 0;
            pod5_end_reason end_reason{};
            std::array<char, 10> string{};
            size_t string_len = string.size();
            call_with_nulls(
                pod5_get_end_reason, batch, index, &end_reason, string.data(), &string_len);
        }

        {
            INFO("pod5_get_pore_type")

            int16_t index = 0;
            std::array<char, 10> string{};
            size_t string_len = string.size();
            call_with_nulls(pod5_get_pore_type, batch, index, string.data(), &string_len);
        }

        {
            INFO("pod5_get_signal_row_info")

            std::array<uint64_t, 1> const signal_rows{};
            SignalRowInfo * signal_row_info = nullptr;
            call_with_nulls(
                pod5_get_signal_row_info,
                reader,
                signal_rows.size(),
                signal_rows.data(),
                &signal_row_info);
        }

        {
            INFO("pod5_free_signal_row_info")

            pod5_reset_error();
            CHECK_POD5_OK(pod5_free_signal_row_info(0, nullptr));
            CHECK_POD5_NOT_OK(pod5_free_signal_row_info(1, nullptr));
        }

        {
            INFO("pod5_get_signal")

            // We need a signal row info.
            uint64_t const signal_row_index = 0;
            SignalRowInfo_t * signal_row_info = nullptr;
            CHECK_POD5_OK(pod5_get_signal_row_info(reader, 1, &signal_row_index, &signal_row_info));
            REQUIRE(signal_row_info);
            auto free_signal_row_info = gsl::finally(
                [&signal_row_info] { pod5_free_signal_row_info(1, &signal_row_info); });

            std::array<int16_t, 10> samples{};
            call_with_nulls(
                pod5_get_signal, reader, signal_row_info, samples.size(), samples.data());
        }

        {
            INFO("pod5_get_read_complete_sample_count")

            size_t row = 0;
            size_t count{};
            call_with_nulls(pod5_get_read_complete_sample_count, reader, batch, row, &count);
        }

        {
            INFO("pod5_get_read_complete_signal")

            size_t row = 1;
            std::array<int16_t, 10> samples{};
            call_with_nulls(
                pod5_get_read_complete_signal, reader, batch, row, samples.size(), samples.data());
        }
    }

    SECTION("Writer API")
    {
        {
            INFO("pod5_create_file")

            pod5_reset_error();
            CHECK(pod5_create_file(nullptr, nullptr, nullptr) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null string passed"));

            pod5_reset_error();
            CHECK(pod5_create_file(temporary_filename, nullptr, nullptr) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null string passed"));

            pod5_reset_error();
            CHECK(pod5_create_file(nullptr, temporary_filename, nullptr) == nullptr);
            CHECK_THAT(pod5_get_error_string(), Contains("null string passed"));
        }

        {
            INFO("pod5_close_and_free_writer")

            pod5_reset_error();
            CHECK_POD5_OK(pod5_close_and_free_writer(nullptr));
        }

        // The rest of these functions require a writer.
        REQUIRE(remove_file_if_exists(temporary_filename).ok());
        Pod5FileWriter_t * writer = pod5_create_file(temporary_filename, "c_software", nullptr);
        REQUIRE(writer);
        auto close_writer = gsl::finally([&writer] { pod5_close_and_free_writer(writer); });

        {
            INFO("pod5_add_pore")

            int16_t pore_index{};
            char const pore_type[] = "test";
            call_with_nulls(pod5_add_pore, &pore_index, writer, pore_type);
        }

        {
            INFO("pod5_add_run_info")

            int16_t run_info_index{};

            char const dummy_string[] = "test";

            char const * acquisition_id = dummy_string;
            int64_t acquisition_start_time_ms = 1;
            int16_t adc_max = 1;
            int16_t adc_min = -1;
            char const * experiment_name = dummy_string;
            char const * flow_cell_id = dummy_string;
            char const * flow_cell_product_code = dummy_string;
            char const * protocol_name = dummy_string;
            char const * protocol_run_id = dummy_string;
            int64_t protocol_start_time_ms = 1;
            char const * sample_id = dummy_string;
            uint16_t sample_rate = 1;
            char const * sequencing_kit = dummy_string;
            char const * sequencer_position = dummy_string;
            char const * sequencer_position_type = dummy_string;
            char const * software = dummy_string;
            char const * system_name = dummy_string;
            char const * system_type = dummy_string;

            size_t context_tags_count = 1;
            char const * context_tags_keys[]{dummy_string};
            char const * context_tags_values[]{dummy_string};

            size_t tracking_id_count = 1;
            char const * tracking_id_keys[]{dummy_string};
            char const * tracking_id_values[]{dummy_string};

            call_with_nulls(
                pod5_add_run_info,
                &run_info_index,
                writer,
                acquisition_id,
                acquisition_start_time_ms,
                adc_max,
                adc_min,
                context_tags_count,
                context_tags_keys,
                context_tags_values,
                experiment_name,
                flow_cell_id,
                flow_cell_product_code,
                protocol_name,
                protocol_run_id,
                protocol_start_time_ms,
                sample_id,
                sample_rate,
                sequencing_kit,
                sequencer_position,
                sequencer_position_type,
                software,
                system_name,
                system_type,
                tracking_id_count,
                tracking_id_keys,
                tracking_id_values);
        }

        {
            INFO("pod5_add_reads_data")

            uint32_t count = 1;
            uint16_t version = READ_BATCH_ROW_INFO_VERSION;
            ReadBatchRowInfoArray_t row_info{};
            int16_t const signal[]{1, 2, 3, 4, 5};
            int16_t const * signals[]{signal};
            uint32_t const signal_size = std::size(signal);

            call_with_nulls(
                pod5_add_reads_data,
                writer,
                count,
                version,
                static_cast<void const *>(&row_info),
                signals,
                &signal_size);
        }

        {
            INFO("pod5_add_reads_data_pre_compressed")

            uint32_t count = 1;
            uint16_t version = READ_BATCH_ROW_INFO_VERSION;
            ReadBatchRowInfoArray_t row_info{};

            char const read0_compressed_signal_chunk0[]{1, 2, 3, 4, 5};
            char const * read0_compressed_signal[]{read0_compressed_signal_chunk0};
            size_t const read0_compressed_signal_sizes[]{std::size(read0_compressed_signal_chunk0)};
            uint32_t const read0_sample_counts[]{3};
            size_t const read0_signal_chunk_count = std::size(read0_compressed_signal);

            char const ** compressed_signals[]{read0_compressed_signal};
            size_t const * compressed_signal_sizes[]{read0_compressed_signal_sizes};
            uint32_t const * sample_counts[]{read0_sample_counts};
            size_t const signal_chunk_counts[]{read0_signal_chunk_count};

            call_with_nulls(
                pod5_add_reads_data_pre_compressed,
                writer,
                count,
                version,
                static_cast<void const *>(&row_info),
                compressed_signals,
                compressed_signal_sizes,
                sample_counts,
                signal_chunk_counts);
        }
    }

    SECTION("VBZ API")
    {
        {
            INFO("pod5_vbz_compress_signal")

            std::array<int16_t, 10> const signal{};
            std::array<char, 10> compressed{};
            size_t compressed_size = compressed.size();
            call_with_nulls(
                pod5_vbz_compress_signal,
                signal.data(),
                signal.size(),
                compressed.data(),
                &compressed_size);
        }

        {
            INFO("pod5_vbz_decompress_signal")

            std::array<char, 10> const compressed{};
            std::array<int16_t, 10> signal{};
            call_with_nulls(
                pod5_vbz_decompress_signal,
                compressed.data(),
                compressed.size(),
                signal.size(),
                signal.data());
        }
    }

    SECTION("Misc API")
    {
        {
            INFO("pod5_format_read_id")

            read_id_t const read_id{};
            char * string = nullptr;
            call_with_nulls(pod5_format_read_id, read_id, string);
        }
    }
}

}  // namespace
