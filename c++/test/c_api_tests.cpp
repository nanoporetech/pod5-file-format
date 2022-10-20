#include "pod5_format/c_api.h"

#include "pod5_format/file_reader.h"
#include "pod5_format/schema_metadata.h"
#include "pod5_format/version.h"
#include "utils.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>

#include <iostream>
#include <numeric>

SCENARIO("C API") {
    static constexpr char const* combined_filename = "./foo_c_api.pod5";

    pod5_init();
    auto fin = gsl::finally([] { pod5_terminate(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto input_read_id = uuid_gen();
    std::vector<int16_t> signal_1(10);
    std::iota(signal_1.begin(), signal_1.end(), -20000);

    std::vector<int16_t> signal_2(20);
    std::iota(signal_2.begin(), signal_2.end(), 0);

    std::size_t read_count = 0;

    std::int16_t adc_min = -4096;
    std::int16_t adc_max = 4095;

    float calibration_offset = 54.0f;
    float calibration_scale = 100.0f;

    float predicted_scale = 2.3f;
    float predicted_shift = 10.0f;
    float tracked_scale = 4.3f;
    float tracked_shift = 15.0f;
    std::uint32_t num_reads_since_mux_change = 1234;
    float time_since_mux_change = 2.4f;
    std::uint64_t num_minknow_events = 104;

    // Write the file:
    {
        CHECK(pod5_get_error_no() == POD5_OK);
        CHECK(!pod5_create_combined_file(NULL, "c_software", NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);
        CHECK(!pod5_create_combined_file("", "c_software", NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);
        CHECK(!pod5_create_combined_file("", NULL, NULL));
        CHECK(pod5_get_error_no() == POD5_ERROR_INVALID);

        REQUIRE(remove_file_if_exists(combined_filename).ok());

        auto combined_file = pod5_create_combined_file(combined_filename, "c_software", NULL);
        REQUIRE(combined_file);
        CHECK(pod5_get_error_no() == POD5_OK);

        std::int16_t pore_type_id = -1;
        CHECK(pod5_add_pore(&pore_type_id, combined_file, "pore_type") == POD5_OK);
        CHECK(pore_type_id == 0);

        std::vector<char const*> context_tags_keys{"thing", "foo"};
        std::vector<char const*> context_tags_values{"thing_val", "foo_val"};
        std::vector<char const*> tracking_id_keys{"baz", "other"};
        std::vector<char const*> tracking_id_values{"baz_val", "other_val"};

        std::int16_t run_info_id = -1;
        CHECK(pod5_add_run_info(&run_info_id, combined_file, "acquisition_id", 15400, adc_max,
                                adc_min, context_tags_keys.size(), context_tags_keys.data(),
                                context_tags_values.data(), "experiment_name", "flow_cell_id",
                                "flow_cell_product_code", "protocol_name", "protocol_run_id",
                                200000, "sample_id", 4000, "sequencing_kit", "sequencer_position",
                                "sequencer_position_type", "software", "system_name", "system_type",
                                tracking_id_keys.size(), tracking_id_keys.data(),
                                tracking_id_values.data()) == POD5_OK);
        CHECK(run_info_id == 0);

        std::uint32_t read_number = 12;
        std::uint64_t start_sample = 10245;
        float median_before = 200.0f;
        std::uint16_t channel = 43;
        std::uint8_t well = 4;
        pod5_end_reason_t end_reason = POD5_END_REASON_MUX_CHANGE;
        uint8_t end_reason_forced = false;
        auto read_id_array = (read_id_t const*)input_read_id.begin();

        ReadBatchRowInfoArrayV3 row_data{read_id_array,
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
                                         &tracked_scale,
                                         &tracked_shift,
                                         &predicted_scale,
                                         &predicted_shift,
                                         &num_reads_since_mux_change,
                                         &time_since_mux_change};

        {
            std::int16_t const* signal_arr[] = {signal_1.data()};
            std::uint32_t signal_size[] = {(std::uint32_t)signal_1.size()};

            CHECK(pod5_add_reads_data(combined_file, 1, READ_BATCH_ROW_INFO_VERSION_3, &row_data,
                                      signal_arr, signal_size) == POD5_OK);
            read_count += 1;
        }

        {
            auto compressed_read_max_size = pod5_vbz_compressed_signal_max_size(signal_2.size());
            std::vector<char> compressed_signal(compressed_read_max_size);
            char const* compressed_data[] = {compressed_signal.data()};
            char const** compressed_data_ptr = compressed_data;
            std::size_t compressed_size[] = {compressed_signal.size()};
            std::size_t const* compressed_size_ptr = compressed_size;
            std::uint32_t signal_size[] = {(std::uint32_t)signal_2.size()};
            std::uint32_t const* signal_size_ptr = signal_size;
            pod5_vbz_compress_signal(signal_2.data(), signal_2.size(), compressed_signal.data(),
                                     compressed_size);

            std::size_t signal_counts = 1;

            CHECK(pod5_add_reads_data_pre_compressed(combined_file, 1,
                                                     READ_BATCH_ROW_INFO_VERSION_3, &row_data,
                                                     &compressed_data_ptr, &compressed_size_ptr,
                                                     &signal_size_ptr, &signal_counts) == POD5_OK);
            read_count += 1;
        }

        CHECK(pod5_close_and_free_writer(combined_file) == POD5_OK);
        CHECK(pod5_get_error_no() == POD5_OK);
    }

    // Read the file back:
    {
        CHECK(pod5_get_error_no() == POD5_OK);
        CHECK(!pod5_open_combined_file(NULL));
        auto combined_file = pod5_open_combined_file(combined_filename);
        CHECK(pod5_get_error_no() == POD5_OK);
        CHECK(!!combined_file);

        FileInfo_t file_info;
        CHECK(pod5_get_file_info(combined_file, &file_info) == POD5_OK);
        CHECK(file_info.version.major == pod5::Pod5MajorVersion);
        CHECK(file_info.version.minor == pod5::Pod5MinorVersion);
        CHECK(file_info.version.revision == pod5::Pod5RevVersion);
        {
            auto reader = pod5::open_combined_file_reader(combined_filename);
            boost::uuids::uuid file_identifier;
            std::copy(file_info.file_identifier,
                      file_info.file_identifier + sizeof(file_info.file_identifier),
                      file_identifier.begin());
            CHECK(file_identifier == (*reader)->schema_metadata().file_identifier);
        }

        std::size_t batch_count = 0;
        CHECK(pod5_get_read_batch_count(&batch_count, combined_file) == POD5_OK);
        REQUIRE(batch_count == 1);

        Pod5ReadRecordBatch* batch_0 = nullptr;
        CHECK(pod5_get_read_batch(&batch_0, combined_file, 0) == POD5_OK);
        REQUIRE(!!batch_0);

        for (std::size_t row = 0; row < read_count; ++row) {
            auto signal = signal_1;
            if (row == 1) {
                signal = signal_2;
            }

            static_assert(std::is_same<ReadBatchRowInfoV3, ReadBatchRowInfo_t>::value,
                          "Update this if new structs added");

            ReadBatchRowInfoV3 v3_struct;
            uint16_t input_version = 0;
            CHECK(pod5_get_read_batch_row_info_data(batch_0, row, READ_BATCH_ROW_INFO_VERSION,
                                                    &v3_struct, &input_version) == POD5_OK);
            CHECK(input_version == 3);

            std::string formatted_uuid(36, '\0');
            CHECK(pod5_format_read_id(v3_struct.read_id, &formatted_uuid[0]) == POD5_OK);
            CHECK(formatted_uuid.size() ==
                  boost::uuids::to_string(*(boost::uuids::uuid*)v3_struct.read_id).size());
            CHECK(formatted_uuid ==
                  boost::uuids::to_string(*(boost::uuids::uuid*)v3_struct.read_id));

            CHECK(v3_struct.read_number == 12);
            CHECK(v3_struct.start_sample == 10245);
            CHECK(v3_struct.median_before == 200.0f);
            CHECK(v3_struct.channel == 43);
            CHECK(v3_struct.well == 4);
            CHECK(v3_struct.pore_type == 0);
            CHECK(v3_struct.calibration_offset == calibration_offset);
            CHECK(v3_struct.calibration_scale == calibration_scale);
            CHECK(v3_struct.end_reason == 1);
            CHECK(v3_struct.end_reason_forced == false);
            CHECK(v3_struct.run_info == 0);
            CHECK(v3_struct.num_minknow_events == num_minknow_events);
            CHECK(v3_struct.tracked_scaling_scale == tracked_scale);
            CHECK(v3_struct.tracked_scaling_shift == tracked_shift);
            CHECK(v3_struct.predicted_scaling_scale == predicted_scale);
            CHECK(v3_struct.predicted_scaling_shift == predicted_shift);
            CHECK(v3_struct.num_reads_since_mux_change == num_reads_since_mux_change);
            CHECK(v3_struct.time_since_mux_change == time_since_mux_change);
            CHECK(v3_struct.signal_row_count == 1);
            CHECK(v3_struct.num_samples == signal.size());

            std::vector<uint64_t> signal_row_indices(v3_struct.signal_row_count);
            CHECK(pod5_get_signal_row_indices(batch_0, row, signal_row_indices.size(),
                                              signal_row_indices.data()) == POD5_OK);

            std::vector<SignalRowInfo*> signal_row_info(v3_struct.signal_row_count);
            CHECK(pod5_get_signal_row_info(combined_file, signal_row_indices.size(),
                                           signal_row_indices.data(),
                                           signal_row_info.data()) == POD5_OK);

            std::vector<int16_t> read_signal(signal_row_info.front()->stored_sample_count);
            REQUIRE(signal_row_info.front()->stored_sample_count == signal.size());
            CHECK(pod5_get_signal(combined_file, signal_row_info.front(),
                                  signal_row_info.front()->stored_sample_count,
                                  read_signal.data()) == POD5_OK);
            CHECK(read_signal == signal);

            std::size_t sample_count = 0;
            CHECK(pod5_get_read_complete_sample_count(combined_file, batch_0, row, &sample_count) ==
                  POD5_OK);
            CHECK(sample_count == signal_row_info.front()->stored_sample_count);
            CHECK(pod5_get_read_complete_signal(combined_file, batch_0, row, sample_count,
                                                read_signal.data()) == POD5_OK);
            CHECK(read_signal == signal);

            CHECK(pod5_free_signal_row_info(signal_row_indices.size(), signal_row_info.data()) ==
                  POD5_OK);

            std::array<char, 256> char_buffer{};
            std::size_t returned_size = char_buffer.size();
            CHECK(pod5_get_pore_type(batch_0, v3_struct.pore_type, char_buffer.data(),
                                     &returned_size) == POD5_OK);
            std::string expected_pore_type{"pore_type"};
            CHECK(returned_size == expected_pore_type.size() + 1);
            CHECK(std::string{char_buffer.data()} == expected_pore_type);

            returned_size = char_buffer.size();
            pod5_end_reason end_reason = POD5_END_REASON_UNKNOWN;
            CHECK(pod5_get_end_reason(batch_0, v3_struct.end_reason, &end_reason,
                                      char_buffer.data(), &returned_size) == POD5_OK);
            std::string expected_end_reason{"mux_change"};
            CHECK(returned_size == expected_end_reason.size() + 1);
            CHECK(end_reason == POD5_END_REASON_MUX_CHANGE);
            CHECK(std::string{char_buffer.data()} == expected_end_reason);

            CalibrationExtraData calibration_extra_data{};
            CHECK(pod5_get_calibration_extra_info(batch_0, row, &calibration_extra_data) ==
                  POD5_OK);
            CHECK(calibration_extra_data.digitisation == adc_max - adc_min + 1);
            CHECK(calibration_extra_data.range == 8192 * calibration_scale);
        }

        RunInfoDictData* run_info_data_out = nullptr;
        CHECK(pod5_get_run_info(batch_0, 0, &run_info_data_out) == POD5_OK);
        REQUIRE(!!run_info_data_out);
        CHECK(run_info_data_out->tracking_id.size == 2);
        CHECK(run_info_data_out->tracking_id.keys[0] == std::string("baz"));
        CHECK(run_info_data_out->tracking_id.keys[1] == std::string("other"));
        CHECK(run_info_data_out->tracking_id.values[0] == std::string("baz_val"));
        CHECK(run_info_data_out->tracking_id.values[1] == std::string("other_val"));
        CHECK(run_info_data_out->context_tags.size == 2);
        CHECK(run_info_data_out->context_tags.keys[0] == std::string("thing"));
        CHECK(run_info_data_out->context_tags.keys[1] == std::string("foo"));
        CHECK(run_info_data_out->context_tags.values[0] == std::string("thing_val"));
        CHECK(run_info_data_out->context_tags.values[1] == std::string("foo_val"));
        pod5_release_run_info(run_info_data_out);

        pod5_free_read_batch(batch_0);

        pod5_close_and_free_reader(combined_file);
        CHECK(pod5_get_error_no() == POD5_OK);
    }
}
