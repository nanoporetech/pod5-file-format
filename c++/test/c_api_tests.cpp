#include "mkr_format/c_api.h"

#include <boost/uuid/random_generator.hpp>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>

#include <numeric>

SCENARIO("C API") {
    static constexpr char const* combined_filename = "./foo_c_api.mkr";

    mkr_init();
    auto fin = gsl::finally([] { mkr_terminate(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto read_id = uuid_gen();
    std::vector<int16_t> signal(20'000);
    std::iota(signal.begin(), signal.end(), 0);

    std::size_t read_count = 0;

    // Write the file:
    {
        CHECK(mkr_get_error_no() == MKR_OK);
        CHECK(!mkr_create_combined_file(NULL, "c_software", NULL));
        CHECK(mkr_get_error_no() == MKR_ERROR_INVALID);
        CHECK(!mkr_create_combined_file("", "c_software", NULL));
        CHECK(mkr_get_error_no() == MKR_ERROR_INVALID);
        CHECK(!mkr_create_combined_file("", NULL, NULL));
        CHECK(mkr_get_error_no() == MKR_ERROR_INVALID);

        auto combined_file = mkr_create_combined_file(combined_filename, "c_software", NULL);
        REQUIRE(combined_file);
        CHECK(mkr_get_error_no() == MKR_OK);

        std::int16_t pore_id = -1;
        CHECK(mkr_add_pore(&pore_id, combined_file, 43, 2, "pore_type") == MKR_OK);
        CHECK(pore_id == 0);

        std::int16_t end_reason_id = -1;
        CHECK(mkr_add_end_reason(&end_reason_id, combined_file, MKR_END_REASON_MUX_CHANGE, false) ==
              MKR_OK);
        CHECK(end_reason_id == 0);

        std::int16_t calibration_id = -1;
        CHECK(mkr_add_calibration(&calibration_id, combined_file, 54.0f, 100.0f) == MKR_OK);
        CHECK(calibration_id == 0);

        std::vector<char const*> context_tags_keys{"thing", "foo"};
        std::vector<char const*> context_tags_values{"thing_val", "foo_val"};
        std::vector<char const*> tracking_id_keys{"baz", "other"};
        std::vector<char const*> tracking_id_values{"baz_val", "other_val"};

        std::int16_t run_info_id = -1;
        CHECK(mkr_add_run_info(&run_info_id, combined_file, "acquisition_id", 15400, 1024, 0,
                               context_tags_keys.size(), context_tags_keys.data(),
                               context_tags_values.data(), "experiment_name", "flow_cell_id",
                               "flow_cell_product_code", "protocol_name", "protocol_run_id", 200000,
                               "sample_id", 4000, "sequencing_kit", "sequencer_position",
                               "sequencer_position_type", "software", "system_name", "system_type",
                               tracking_id_keys.size(), tracking_id_keys.data(),
                               tracking_id_values.data()) == MKR_OK);
        CHECK(run_info_id == 0);

        std::uint32_t read_number = 12;
        std::uint64_t start_sample = 10245;
        float median_before = 200.0f;
        auto read_id_array = (read_id_t const*)read_id.begin();

        std::int16_t const* signal_arr[] = {signal.data()};
        std::uint32_t signal_size[] = {(std::uint32_t)signal.size()};

        CHECK(mkr_add_reads(combined_file, 1, read_id_array, &pore_id, &calibration_id,
                            &read_number, &start_sample, &median_before, &end_reason_id,
                            &run_info_id, signal_arr, signal_size) == MKR_OK);
        read_count += 1;

        auto compressed_read_max_size = mkr_vbz_compressed_signal_max_size(signal.size());
        std::vector<char> compressed_signal(compressed_read_max_size);
        char const* compressed_data[] = {compressed_signal.data()};
        char const** compressed_data_ptr = compressed_data;
        std::size_t compressed_size[] = {compressed_signal.size()};
        std::size_t const* compressed_size_ptr = compressed_size;
        std::uint32_t const* signal_size_ptr = signal_size;
        mkr_vbz_compress_signal(signal.data(), signal.size(), compressed_signal.data(),
                                compressed_size);

        std::size_t signal_counts = 1;

        CHECK(mkr_add_reads_pre_compressed(combined_file, 1, read_id_array, &pore_id,
                                           &calibration_id, &read_number, &start_sample,
                                           &median_before, &end_reason_id, &run_info_id,
                                           &compressed_data_ptr, &compressed_size_ptr,
                                           &signal_size_ptr, &signal_counts) == MKR_OK);
        read_count += 1;

        CHECK(mkr_close_and_free_writer(combined_file) == MKR_OK);
        CHECK(mkr_get_error_no() == MKR_OK);
    }

    // Read the file back:
    {
        CHECK(mkr_get_error_no() == MKR_OK);
        CHECK(!mkr_open_combined_file(NULL));
        auto combined_file = mkr_open_combined_file(combined_filename);
        CHECK(mkr_get_error_no() == MKR_OK);
        CAPTURE(mkr_get_error_string());
        CHECK(!!combined_file);

        std::size_t batch_count = 0;
        CHECK(mkr_get_read_batch_count(&batch_count, combined_file) == MKR_OK);
        REQUIRE(batch_count == 1);

        MkrReadRecordBatch* batch_0 = nullptr;
        CHECK(mkr_get_read_batch(&batch_0, combined_file, 0) == MKR_OK);
        REQUIRE(!!batch_0);

        for (std::size_t row = 0; row < read_count; ++row) {
            boost::uuids::uuid read_id;
            int16_t pore = 0;
            int16_t calibration = 0;
            uint32_t read_number = 0;
            uint64_t start_sample = 0;
            float median_before = 0.0f;
            int16_t end_reason = 0;
            int16_t run_info = 0;
            int64_t signal_row_count = 0;

            CHECK(mkr_get_read_batch_row_info(batch_0, row, (uint8_t*)read_id.begin(), &pore,
                                              &calibration, &read_number, &start_sample,
                                              &median_before, &end_reason, &run_info,
                                              &signal_row_count) == MKR_OK);

            CHECK(read_number == 12);
            CHECK(start_sample == 10245);
            CHECK(median_before == 200.0f);
            CHECK(pore == 0);
            CHECK(calibration == 0);
            CHECK(end_reason == 0);
            CHECK(run_info == 0);
            CHECK(signal_row_count == 1);

            std::vector<uint64_t> signal_row_indices(signal_row_count);
            CHECK(mkr_get_signal_row_indices(batch_0, row, signal_row_indices.size(),
                                             signal_row_indices.data()) == MKR_OK);

            std::vector<SignalRowInfo> signal_row_info(signal_row_count);
            CHECK(mkr_get_signal_row_info(combined_file, signal_row_indices.size(),
                                          signal_row_indices.data(),
                                          signal_row_info.data()) == MKR_OK);

            std::vector<int16_t> read_signal(signal_row_info.front().stored_sample_count);
            CHECK(mkr_get_signal(combined_file, signal_row_info.front().batch_index,
                                 signal_row_info.front().batch_row_index,
                                 signal_row_info.front().stored_sample_count,
                                 read_signal.data()) == MKR_OK);

            CHECK(read_signal == signal);
        }

        RunInfoDictData* run_info_data_out = nullptr;
        CHECK(mkr_get_run_info(batch_0, 0, &run_info_data_out) == MKR_OK);
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
        mkr_release_run_info(run_info_data_out);

        mkr_free_read_batch(batch_0);

        mkr_close_and_free_reader(combined_file);
        CHECK(mkr_get_error_no() == MKR_OK);
    }
}