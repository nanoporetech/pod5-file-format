#include "pod5_format/async_signal_loader.h"
#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_table_reader.h"
#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_primitive.h>
#include <arrow/memory_pool.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <catch2/catch.hpp>

#include <iostream>
#include <numeric>

void run_file_reader_writer_tests()
{
    static constexpr char const * file = "./foo.pod5";
    REQUIRE_ARROW_STATUS_OK(remove_file_if_exists(file));
    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto const run_info_data = get_test_run_info_data("_run_info");

    auto uuid_gen = boost::uuids::random_generator_mt19937();
    auto read_id_1 = uuid_gen();

    std::uint16_t channel = 25;
    std::uint8_t well = 3;
    std::uint32_t read_number = 1234;
    std::uint64_t start_sample = 12340;
    std::uint64_t num_minknow_events = 27;
    float median_before = 224.0f;
    float calib_offset = 22.5f;
    float calib_scale = 1.2f;
    float tracked_scaling_scale = 2.3f;
    float tracked_scaling_shift = 100.0f;
    float predicted_scaling_scale = 1.5f;
    float predicted_scaling_shift = 50.0f;
    std::uint32_t num_reads_since_mux_change = 3;
    float time_since_mux_change = 200.0f;

    std::vector<std::int16_t> signal_1(100'000);
    std::iota(signal_1.begin(), signal_1.end(), 0);

    // Write a file:
    {
        pod5::FileWriterOptions options;
        options.set_max_signal_chunk_size(20'480);
        options.set_read_table_batch_size(1);
        options.set_signal_table_batch_size(5);

        auto writer = pod5::create_file_writer(file, "test_software", options);
        REQUIRE_ARROW_STATUS_OK(writer);

        auto run_info = (*writer)->add_run_info(run_info_data);
        auto end_reason = (*writer)->lookup_end_reason(pod5::ReadEndReason::signal_negative);
        bool end_reason_forced = true;
        auto pore_type = (*writer)->add_pore_type("Pore_type");

        for (std::size_t i = 0; i < 10; ++i) {
            CHECK_ARROW_STATUS_OK((*writer)->add_complete_read(
                {read_id_1,
                 read_number,
                 start_sample,
                 channel,
                 well,
                 *pore_type,
                 calib_offset,
                 calib_scale,
                 median_before,
                 *end_reason,
                 end_reason_forced,
                 *run_info,
                 num_minknow_events,
                 tracked_scaling_scale,
                 tracked_scaling_shift,
                 predicted_scaling_scale,
                 predicted_scaling_shift,
                 num_reads_since_mux_change,
                 time_since_mux_change},
                gsl::make_span(signal_1)));
        }
    }

    // Open the file for reading:
    // Write a file:
    {
        auto reader = pod5::open_file_reader(file, {});
        REQUIRE_ARROW_STATUS_OK(reader);

        REQUIRE((*reader)->num_read_record_batches() == 10);
        for (std::size_t i = 0; i < 10; ++i) {
            auto read_batch = (*reader)->read_read_record_batch(i);
            REQUIRE_ARROW_STATUS_OK(read_batch);

            auto read_id_array = read_batch->read_id_column();
            CHECK(read_id_array->length() == 1);
            CHECK(read_id_array->Value(0) == read_id_1);

            auto columns = *read_batch->columns();
            auto const run_info_dict_index =
                std::dynamic_pointer_cast<arrow::Int16Array>(columns.run_info->indices())->Value(0);
            CHECK(run_info_dict_index == 0);
            auto const run_info_id = read_batch->get_run_info(run_info_dict_index);
            CHECK(*run_info_id == run_info_data.acquisition_id);
            auto const run_info = (*reader)->find_run_info(*run_info_id);
            CHECK(**run_info == run_info_data);

            REQUIRE((*reader)->num_signal_record_batches() == 10);
            auto signal_batch = (*reader)->read_signal_record_batch(i);
            REQUIRE_ARROW_STATUS_OK(signal_batch);

            auto signal_read_id_array = signal_batch->read_id_column();
            CHECK(signal_read_id_array->length() == 5);
            CHECK(signal_read_id_array->Value(0) == read_id_1);
            CHECK(signal_read_id_array->Value(1) == read_id_1);
            CHECK(signal_read_id_array->Value(2) == read_id_1);
            CHECK(signal_read_id_array->Value(3) == read_id_1);
            CHECK(signal_read_id_array->Value(4) == read_id_1);

            auto vbz_signal_array = signal_batch->vbz_signal_column();
            CHECK(vbz_signal_array->length() == 5);

            auto samples_array = signal_batch->samples_column();
            CHECK(samples_array->Value(0) == 20'480);
            CHECK(samples_array->Value(1) == 20'480);
            CHECK(samples_array->Value(2) == 20'480);
            CHECK(samples_array->Value(3) == 20'480);
            CHECK(samples_array->Value(4) == 18'080);
        }

        auto const samples_mode = GENERATE(
            pod5::AsyncSignalLoader::SamplesMode::NoSamples,
            pod5::AsyncSignalLoader::SamplesMode::Samples);

        pod5::AsyncSignalLoader async_no_samples_loader(
            *reader,
            samples_mode,
            {},  // Read all the batches
            {}   // No specific rows within batches)
        );

        for (std::size_t i = 0; i < 10; ++i) {
            CAPTURE(i);
            auto first_batch_res = async_no_samples_loader.release_next_batch();
            REQUIRE_ARROW_STATUS_OK(first_batch_res);
            auto first_batch = std::move(*first_batch_res);
            CHECK(first_batch->batch_index() == i);

            CHECK(first_batch->sample_count().size() == 1);
            CHECK(first_batch->sample_count()[0] == signal_1.size());

            CHECK(first_batch->samples().size() == 1);
            if (samples_mode == pod5::AsyncSignalLoader::SamplesMode::Samples) {
                CHECK(first_batch->samples()[0] == signal_1);
            } else {
                CHECK(first_batch->samples()[0].size() == 0);
            }
        }
    }
}

SCENARIO("File Reader Writer Tests") { run_file_reader_writer_tests(); }

SCENARIO("Opening older files")
{
    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto uuid_from_string = [](char const * val) -> boost::uuids::uuid {
        return boost::lexical_cast<boost::uuids::uuid>(val);
    };

    struct ReadData {
        boost::uuids::uuid read_id;
        std::uint32_t read_number;
        float calibration_offset;
        float calibration_scale;
        std::string end_reason;
        std::string pore_type;
        std::string run_info_id;
    };

    std::vector<ReadData> test_read_data{
        {{uuid_from_string("0000173c-bf67-44e7-9a9c-1ad0bc728e74")},
         1093,
         21.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("002fde30-9e23-4125-9eae-d112c18a81a7")},
         75,
         4.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("006d1319-2877-4b34-85df-34de7250a47b")},
         1053,
         6.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("00728efb-2120-4224-87d8-580fbb0bd4b2")},
         657,
         2.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("007cc97e-6de2-4ff6-a0fd-1c1eca816425")},
         1625,
         23.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("008468c3-e477-46c4-a6e2-7d021a4ebf0b")},
         411,
         4.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("008ed3dc-86c2-452f-b107-6877a473d177")},
         513,
         5.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("00919556-e519-4960-8aa5-c2dfa020980c")},
         56,
         2.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("00925f34-6baf-47fc-b40c-22591e27fb5c")},
         930,
         37.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
        {{uuid_from_string("009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e")},
         195,
         14.0f,
         0.1755f,
         "unknown",
         "not_set",
         "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
    };

    auto repo_root =
        ::arrow::internal::PlatformFilename::FromString(__FILE__)->Parent().Parent().Parent();
    auto path = GENERATE_COPY(
        *repo_root.Join("test_data/multi_fast5_zip_v0.pod5"),
        *repo_root.Join("test_data/multi_fast5_zip_v1.pod5"),
        *repo_root.Join("test_data/multi_fast5_zip_v2.pod5"),
        *repo_root.Join("test_data/multi_fast5_zip_v3.pod5"));
    auto reader = pod5::open_file_reader(path.ToString(), {});
    CHECK_ARROW_STATUS_OK(reader);

    auto metadata = (*reader)->schema_metadata();
    CHECK(metadata.writing_software == "Python API");

    std::size_t abs_row = 0;

    for (std::size_t i = 0; i < (*reader)->num_read_record_batches(); ++i) {
        auto batch = (*reader)->read_read_record_batch(i);

        auto columns = batch->columns();
        REQUIRE_ARROW_STATUS_OK(columns);

        for (std::size_t row = 0; row < batch->num_rows(); ++row) {
            CAPTURE(abs_row);
            auto read_data = test_read_data[row];
            CHECK(columns->read_id->Value(row) == read_data.read_id);
            CHECK(columns->read_number->Value(row) == read_data.read_number);
            CHECK(columns->calibration_offset->Value(row) == read_data.calibration_offset);
            CHECK(columns->calibration_scale->Value(row) == Approx(read_data.calibration_scale));
            auto end_reason = *batch->get_end_reason(columns->end_reason->GetValueIndex(row));
            CHECK(end_reason.first == pod5::end_reason_from_string(read_data.end_reason));
            CHECK(end_reason.second == read_data.end_reason);
            auto pore_type = batch->get_pore_type(columns->pore_type->GetValueIndex(row));
            CHECK(*pore_type == read_data.pore_type);
            auto run_info_id = batch->get_run_info(columns->run_info->GetValueIndex(row));
            CHECK(*run_info_id == read_data.run_info_id);

            ++abs_row;
        }
    }
    CHECK(abs_row == test_read_data.size());

    auto run_info = (*reader)->find_run_info(test_read_data[0].run_info_id);
    REQUIRE_ARROW_STATUS_OK(run_info);
    CHECK((*run_info)->acquisition_id == test_read_data[0].run_info_id);
    CHECK((*run_info)->adc_min == -4096);
    CHECK((*run_info)->adc_max == 4095);
    CHECK((*run_info)->protocol_run_id == "df049455-3552-438c-8176-d4a5b1dd9fc5");
    CHECK((*run_info)->software == "python-pod5-converter");
    CHECK(
        (*run_info)->tracking_id
        == pod5::RunInfoData::MapType{
            {"asic_id", "131070"},
            {"asic_id_eeprom", "0"},
            {"asic_temp", "35.043102"},
            {"asic_version", "IA02C"},
            {"auto_update", "0"},
            {"auto_update_source", "https://mirror.oxfordnanoportal.com/software/MinKNOW/"},
            {"bream_is_standard", "0"},
            {"device_id", "MS00000"},
            {"device_type", "minion"},
            {"distribution_status", "modified"},
            {"distribution_version", "unknown"},
            {"exp_script_name", "c449127e3461a521e0865fe6a88716f6f6b0b30c"},
            {"exp_script_purpose", "sequencing_run"},
            {"exp_start_time", "2019-05-13T11:11:43Z"},
            {"flow_cell_id", ""},
            {"guppy_version", "3.0.3+7e7b7d0"},
            {"heatsink_temp", "35.000000"},
            {"hostname", "happy_fish"},
            {"installation_type", "prod"},
            {"local_firmware_file", "1"},
            {"operating_system", "ubuntu 16.04"},
            {"protocol_group_id", "TEST_EXPERIMENT"},
            {"protocol_run_id", "df049455-3552-438c-8176-d4a5b1dd9fc5"},
            {"protocols_version", "4.0.6"},
            {"run_id", "a08e850aaa44c8b56765eee10b386fc3e516a62b"},
            {"sample_id", "TEST_SAMPLE"},
            {"usb_config", "MinION_fx3_1.1.1_ONT#MinION_fpga_1.1.0#ctrl#Auto"},
            {"version", "3.4.0-rc3"},
        });
}
