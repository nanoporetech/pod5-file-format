#include "mkr_format/c_api.h"

#include <boost/uuid/random_generator.hpp>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>

SCENARIO("C API") {
    static constexpr char const* combined_filename = "./foo_c_api.mkr";

    mkr_init();
    auto fin = gsl::finally([] { mkr_terminate(); });

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto read_id = uuid_gen();
    std::vector<int16_t> signal(200'000);
    std::iota(signal.begin(), signal.end(), 0);

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

        CHECK(mkr_add_read(combined_file, (uint8_t const*)read_id.begin(), pore_id, calibration_id,
                           12, 10245, 200.0f, end_reason_id, run_info_id, signal.data(),
                           signal.size()) == MKR_OK);

        mkr_close_and_free_writer(combined_file);
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