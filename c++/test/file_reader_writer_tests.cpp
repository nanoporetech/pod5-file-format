#include "pod5_format/async_signal_loader.h"
#include "pod5_format/file_reader.h"
#include "pod5_format/file_writer.h"
#include "pod5_format/internal/combined_file_utils.h"
#include "pod5_format/read_table_reader.h"
#include "pod5_format/signal_table_reader.h"
#include "pod5_format/thread_pool.h"
#include "pod5_format/uuid.h"
#include "TemporaryDirectory.h"
#include "test_utils.h"
#include "utils.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_primitive.h>
#include <arrow/io/file.h>
#include <arrow/memory_pool.h>
#include <catch2/catch.hpp>

#include <chrono>
#include <fstream>
#include <numeric>
#include <string>
#include <thread>

void run_file_reader_writer_tests(
    char const * file,
    pod5::FileWriterOptions const & extra_options = {})
{
    REQUIRE_ARROW_STATUS_OK(remove_file_if_exists(file));
    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto const run_info_data = get_test_run_info_data("_run_info");

    std::mt19937 gen{Catch::rngSeed()};
    auto uuid_gen = pod5::UuidRandomGenerator{gen};
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
    float open_pore_level = 150.0f;

    std::vector<std::int16_t> signal_1(100'000);
    std::iota(signal_1.begin(), signal_1.end(), 0);

    // Write a file:
    {
        pod5::FileWriterOptions options = extra_options;
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
                 time_since_mux_change,
                 open_pore_level},
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

SCENARIO("File Reader Writer Tests") { run_file_reader_writer_tests("./foo.pod5"); }

#ifdef __linux__
TEST_CASE("Additional make_file_stream() tests")
{
    // When the user filesystem doesn't support direct-io, but it is requested then
    // make_file_stream() should fallback to a "regular" FileOutputStream

    // This because of the disk mounting, this test can only be run by someone or something that
    // is effectively a root user.
    if (::geteuid() != 0) {
        WARN("SKIPPING TEST: Need root privileges to mount a test drive.");
        return;
    }

    std::filesystem::path const dir_path = "./ramdisk_" + std::to_string(std::time(nullptr));

    auto const umount_cmd = std::string{"umount "} + dir_path.string();
    // Create and mount tmpfs drive.
    auto remove_directory = gsl::finally([&] { std::filesystem::remove(dir_path); });
    auto remove_mount = gsl::finally([&] { std::ignore = std::system(umount_cmd.c_str()); });
    try {
        std::filesystem::create_directory(dir_path);
        auto const mount_cmd = std::string{"mount -o size=500M -t tmpfs none "} + dir_path.string();
        auto const mount_return = std::system(mount_cmd.c_str());
        REQUIRE(mount_return == 0);
    } catch (std::exception const & e) {
        FAIL("Failed to create and mount a tmpfs drive: " << e.what());
    }

    pod5::FileWriterOptions options_for_direct_io;
    options_for_direct_io.set_use_directio(true);
    options_for_direct_io.set_use_sync_io(true);
    options_for_direct_io.set_write_chunk_size(524288);

    try {
        auto const test_file_path = dir_path / "bar.pod5";
        run_file_reader_writer_tests(test_file_path.c_str(), options_for_direct_io);
    } catch (std::exception const & e) {
        FAIL("Failed to run file reader/writer tests: " << e.what());
    }
}
#endif

SCENARIO("Opening older files")
{
    (void)pod5::register_extension_types();
    auto fin = gsl::finally([] { (void)pod5::unregister_extension_types(); });

    auto uuid_from_string = [](char const * val) -> pod5::Uuid {
        auto result = pod5::Uuid::from_string(val);
        REQUIRE(result);
        return *result;
    };

    struct ReadData {
        pod5::Uuid read_id;
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
        *repo_root.Join("test_data/multi_fast5_zip_v3.pod5"),
        *repo_root.Join("test_data/multi_fast5_zip_v4.pod5"));
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

/// Create empty file at \p path.
static void touch(std::filesystem::path const & path) { std::ofstream const ofs(path); }

/// Create file containing bytes of value zero at \p path.
static void write_zeros(std::filesystem::path const & path)
{
    std::ofstream file_stream(path, std::ios::binary);
    for (int i = 0; i < 1000000; ++i) {
        file_stream.put('\0');
    }
}

/// Returns true iff the file exists and contains non-null data.
static bool file_writing_started(std::filesystem::path const & file_path)
{
    if (!exists(file_path)) {
        return false;
    }
    if (!is_regular_file(file_path)) {
        return false;
    }
    // This should be enough for the check as unwritten files are usually
    // empty or populated with nulls if writing has not been done.
    auto const MINIMUM_BYTES_WRITTEN = 3;
    if (file_size(file_path) < 3) {
        return false;
    }
    std::ifstream file{file_path, std::ios::in | std::ios::binary};
    for (auto byte_index = 0; byte_index < MINIMUM_BYTES_WRITTEN; ++byte_index) {
        std::uint8_t byte;
        file >> byte;
        if (byte == 0) {
            return false;
        }
    }
    return true;
}

static bool files_ready_to_recover(std::filesystem::path const & directory_path)
{
    using directory_iterator = std::filesystem::directory_iterator;
    // The directory should contain 3 files for recovery. A `pod5.tmp` with the signal data
    // a `.tmp-reads` for the reads and a `.tmp-run-info` for the run information.
    return std::count_if(
               directory_iterator(directory_path), directory_iterator{}, file_writing_started)
           >= 3;
}

static void wait_for_files_to_recover(std::filesystem::path const & directory_path)
{
    using clock = std::chrono::steady_clock;
    auto const begin_waiting = clock::now();
    auto const time_waited = [&]() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - begin_waiting);
    };

    while (!files_ready_to_recover(directory_path)) {
        REQUIRE(time_waited() < std::chrono::milliseconds{100000});
        // Give any asynchronous file writing threads a chance to write to disk, before we continue.
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
}

static std::filesystem::path create_files_for_recovery(
    std::string const & file_name,
    pod5::Uuid read_id_1,
    ont::testutils::TemporaryDirectory & recovery_directory)
{
    auto const run_info_data = get_test_run_info_data("_run_info");

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
    float open_pore_level = 150.0f;

    std::vector<std::int16_t> signal_1(100'000);
    std::iota(signal_1.begin(), signal_1.end(), 0);

    ont::testutils::TemporaryDirectory data_writing_directory;
    auto file = data_writing_directory.path() / file_name;

    pod5::FileWriterOptions options;
    options.set_max_signal_chunk_size(20'480);
    options.set_read_table_batch_size(1);
    options.set_signal_table_batch_size(5);
    options.set_use_sync_io(true);
    auto thread_pool = pod5::make_thread_pool(4);
    options.set_thread_pool(thread_pool);

    auto writer_result = pod5::create_file_writer(file.string(), "test_software", options);
    REQUIRE_ARROW_STATUS_OK(writer_result);
    std::unique_ptr<pod5::FileWriter> writer = std::move(*writer_result);

    auto run_info = writer->add_run_info(run_info_data);
    auto end_reason = writer->lookup_end_reason(pod5::ReadEndReason::signal_negative);
    bool end_reason_forced = true;
    auto pore_type = writer->add_pore_type("Pore_type");

    for (std::size_t i = 0; i < 10; ++i) {
        CHECK_ARROW_STATUS_OK(writer->add_complete_read(
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
             time_since_mux_change,
             open_pore_level},
            gsl::make_span(signal_1)));
    }

    wait_for_files_to_recover(data_writing_directory.path());

    // Intermittent failures were seen on Windows, where the file was in the middle of being
    // written when we copied it. This ensures that the file writing threads are done before
    // we take the files.
    thread_pool->wait_for_drain();

    // The files are deliberately copied here before they can be properly finalised
    // by the destructor of the FileWriter.
    std::filesystem::copy(data_writing_directory.path(), recovery_directory.path());

    REQUIRE(files_ready_to_recover(recovery_directory.path()));

    return recovery_directory.path() / file_name;
}

/// This is equivalent to the C++20 `std::string::ends_with` function. It should be replaced with
/// the standard library function once we move to the C++20 standard and drop support for building
/// with GCC 8.
static bool ends_with(std::string const & search_in, std::string const & suffix)
{
    if (suffix.size() > search_in.size()) {
        return false;
    }
    return search_in.compare(search_in.size() - suffix.size(), std::string::npos, suffix) == 0;
}

TEST_CASE("Check custom rolled ends_with works", "[string_utilities]")
{
    CHECK(ends_with("abc", "abc"));
    CHECK(ends_with("abcdef", "def"));
    CHECK_FALSE(ends_with("abcdef", "abc"));
    CHECK_FALSE(ends_with("def", "abcdef"));
    CHECK_FALSE(ends_with("abc", "def"));
}

static std::string escape_for_regex(std::string const & input)
{
    std::string output;
    for (auto const & character : input) {
        switch (character) {
        case '\\':
        case '/':
        case '.':
        case '[':
        case ']':
        case '(':
        case ')':
            output += std::string("\\");
        default:;
        }
        output += character;
    }
    return output;
}

TEST_CASE("Recovering .pod5.tmp files", "[recovery]")
{
    std::string const file_name = "foo.pod5.tmp";
    ont::testutils::TemporaryDirectory recovery_directory;
    auto const registration_status = pod5::register_extension_types();
    REQUIRE(registration_status.ok());
    auto const unregister = [] { (void)pod5::unregister_extension_types(); };
    auto fin = std::make_unique<gsl::final_action<decltype(unregister)>>(unregister);
    std::mt19937 gen{Catch::rngSeed()};
    auto uuid_gen = pod5::UuidRandomGenerator{gen};
    std::filesystem::path const path_to_recover =
        create_files_for_recovery(file_name, uuid_gen(), recovery_directory);

    REQUIRE(exists(path_to_recover));
    std::filesystem::path reads_path, run_path;
    for (auto const & directory_entry :
         std::filesystem::directory_iterator{recovery_directory.path()})
    {
        if (!directory_entry.is_regular_file()) {
            continue;
        }
        if (ends_with(directory_entry.path().filename().string(), (".tmp-reads"))) {
            reads_path = directory_entry.path();
        }
        if (ends_with(directory_entry.path().filename().string(), (".tmp-run-info"))) {
            run_path = directory_entry.path();
        }
    }
    REQUIRE(exists(reads_path));
    REQUIRE(exists(run_path));
    auto const recovered_file_path = recovery_directory.path() / (file_name + "-recovered.pod5");
    // Confirm that no recovered file is left over from previous test runs.
    REQUIRE_FALSE(exists(recovered_file_path));

    // Paths are implicitly convertible to the kind of strings used for paths
    // on the current platform. On Windows this is an `std::wstring`, but the
    // recover_file_writer takes a `std::string`, so we need the explicit
    // conversion to make the build work on that platform.
    // `generic_string()` is used rather than `native()` because Arrow paths
    // always use `/` as a separator, even on Windows.
    std::string const to_recover = path_to_recover.generic_string();
    std::string const recovered = recovered_file_path.generic_string();

    bool const cleanup = GENERATE(true, false);
    pod5::RecoverFileOptions const options{.cleanup = cleanup};

    CAPTURE(to_recover, recovered, cleanup);

    SECTION("Recovering basic set of .tmp files.")
    {
        auto const recovery_details = pod5::recover_file(to_recover, recovered, options);
        REQUIRE_ARROW_STATUS_OK(recovery_details);
        CHECK(exists(recovered_file_path));
        CHECK(recovery_details->row_counts.run_info == 1);
        CHECK(recovery_details->row_counts.signal == 50);
        CHECK(recovery_details->row_counts.reads == 10);
        CHECK(recovery_details->cleanup_errors.empty());
        if (cleanup) {
            CHECK_FALSE(exists(path_to_recover));
            CHECK_FALSE(exists(reads_path));
            CHECK_FALSE(exists(run_path));
        } else {
            CHECK(exists(path_to_recover));
            CHECK(exists(reads_path));
            CHECK(exists(run_path));
        }
    }

    SECTION("Recovering whilst extensions are not registered.")
    {
        fin = {};
        auto recover_result2 = pod5::recover_file(to_recover, recovered, options);
        REQUIRE_FALSE(recover_result2.ok());
        REQUIRE(
            recover_result2.status().ToString()
            == "Invalid: POD5 library is not correctly initialised.");
        CHECK(exists(path_to_recover));
        CHECK(exists(reads_path));
        CHECK(exists(run_path));
    }

    SECTION("Recovering without run information.")
    {
        remove(run_path);
        std::string const run_info_string = run_path.generic_string();
        CAPTURE(run_info_string);

        SECTION("Recovering set of .tmp files with run info file missing.")
        {
            auto recover_result3 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result3.ok());
            auto const result_message3 = recover_result3.status().ToString();
            auto const expected_regex3 =
                "IOError: Failed whilst attempting to recover run information from file - "
                + escape_for_regex(run_info_string) + R"(\. Detail: \[(errno|Windows error) 2\] )"
                + R"((No such file or directory|The system cannot find the file specified)[.\n\r]*)";
            REQUIRE_THAT(result_message3, Catch::Matchers::Matches(expected_regex3));
            if (cleanup) {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK_FALSE(exists(run_path));
                CHECK_FALSE(exists(recovered_file_path));
            } else {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
            }
        }

        SECTION("Recovering set of .tmp files with run info file empty.")
        {
            touch(run_path);
            auto recover_result4 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result4.ok());
            REQUIRE(
                recover_result4.status().ToString()
                == "Invalid: Failed whilst attempting to recover run information from file - "
                       + run_info_string + ". Detail: File is empty/zero bytes long.");
            if (cleanup) {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK_FALSE(exists(run_path));
                CHECK_FALSE(exists(recovered_file_path));
            } else {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK(exists(run_path));
            }
        }

        SECTION("Recovering set of .tmp files with run info file zeroed.")
        {
            write_zeros(run_path);
            auto recover_result5 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result5.ok());
            REQUIRE(
                recover_result5.status().ToString()
                == "Invalid: Failed whilst attempting to recover run information from file - "
                       + run_info_string + ". Detail: Not an Arrow file");
            if (cleanup) {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK_FALSE(exists(run_path));
                CHECK_FALSE(exists(recovered_file_path));
            } else {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK(exists(run_path));
            }
        }
    }

    SECTION("Recovering without read information.")
    {
        remove(reads_path);
        std::string const reads_string = reads_path.generic_string();
        CAPTURE(reads_string);

        SECTION("Recovering set of .tmp files with reads file missing.")
        {
            auto recover_result6 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result6.ok());
            auto const result_message6 = recover_result6.status().ToString();
            auto const expected_regex6 =
                "IOError: Failed whilst attempting to recover reads from file - "
                + escape_for_regex(reads_string) + R"(\. Detail: \[(errno|Windows error) 2\] )"
                + R"((No such file or directory|The system cannot find the file specified)[.\n\r]*)";
            REQUIRE_THAT(result_message6, Catch::Matchers::Matches(expected_regex6));
            if (cleanup) {
                CHECK(exists(path_to_recover));
                CHECK_FALSE(exists(reads_path));
                CHECK(exists(run_path));
                CHECK_FALSE(exists(recovered_file_path));
            } else {
                CHECK(exists(path_to_recover));
                CHECK(exists(run_path));
            }
        }

        SECTION("Recovering set of .tmp files with reads file empty.")
        {
            touch(reads_path);
            auto recover_result7 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result7.ok());
            REQUIRE(
                recover_result7.status().ToString()
                == "Invalid: Failed whilst attempting to recover reads from file - " + reads_string
                       + ". Detail: File is empty/zero bytes long.");
            if (cleanup) {
                CHECK(exists(path_to_recover));
                CHECK_FALSE(exists(reads_path));
                CHECK(exists(run_path));
                CHECK_FALSE(exists(recovered_file_path));
            } else {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK(exists(run_path));
            }
        }

        SECTION("Recovering set of .tmp files with reads file zeroed.")
        {
            write_zeros(reads_path);
            auto recover_result7 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result7.ok());
            REQUIRE(
                recover_result7.status().ToString()
                == "Invalid: Failed whilst attempting to recover reads from file - " + reads_string
                       + ". Detail: Not an Arrow file");
            if (cleanup) {
                CHECK(exists(path_to_recover));
                CHECK_FALSE(exists(reads_path));
                CHECK(exists(run_path));
                CHECK_FALSE(exists(recovered_file_path));
            } else {
                CHECK(exists(path_to_recover));
                CHECK(exists(reads_path));
                CHECK(exists(run_path));
            }
        }
    }

    SECTION("Error messages for problems with combined .pod5.tmp file.")
    {
        remove(path_to_recover);

        SECTION("Recovering set of .tmp files with .pod5.tmp file missing.")
        {
            auto recover_result8 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result8.ok());
            auto const result_message = recover_result8.status().ToString();
            auto const expected_regex =
                "IOError: Failed to open local file '" + escape_for_regex(to_recover)
                + R"('\. Detail: \[(errno|Windows error) 2\] )"
                + R"((No such file or directory|The system cannot find the file specified)[.\n\r]*)";
            CAPTURE(result_message, expected_regex);
            REQUIRE_THAT(result_message, Catch::Matchers::Matches(expected_regex));
            if (cleanup) {
                CHECK_FALSE(exists(recovered_file_path));
            }
            CHECK_FALSE(exists(path_to_recover));
            CHECK(exists(reads_path));
            CHECK(exists(run_path));
        }

        SECTION("Recovering set of .tmp files with .pod5.tmp file empty.")
        {
            touch(path_to_recover);
            auto recover_result9 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result9.ok());
            REQUIRE(recover_result9.status().ToString() == "IOError: Invalid signature in file");
            if (cleanup) {
                CHECK_FALSE(exists(recovered_file_path));
                CHECK_FALSE(exists(path_to_recover));
            } else {
                CHECK(exists(path_to_recover));
            }
            CHECK(exists(reads_path));
            CHECK(exists(run_path));
        }

        SECTION("Recovering set of .tmp files with .pod5.tmp file zeroed.")
        {
            write_zeros(path_to_recover);
            auto recover_result10 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result10.ok());
            REQUIRE(recover_result10.status().ToString() == "IOError: Invalid signature in file");
            if (cleanup) {
                CHECK_FALSE(exists(recovered_file_path));
                CHECK_FALSE(exists(path_to_recover));
            } else {
                CHECK(exists(path_to_recover));
            }
            CHECK(exists(reads_path));
            CHECK(exists(run_path));
        }

        arrow::Result<std::shared_ptr<arrow::io::FileOutputStream>> result_tmp_file =
            arrow::io::FileOutputStream::Open(to_recover, false);
        REQUIRE_ARROW_STATUS_OK(result_tmp_file);
        std::shared_ptr<arrow::io::FileOutputStream> tmp_file = std::move(*result_tmp_file);
        REQUIRE_ARROW_STATUS_OK(pod5::combined_file_utils::write_file_signature(tmp_file));

        SECTION("Recover .pod5.tmp missing section marker after signature.")
        {
            tmp_file = {};
            auto recover_result11 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result11.ok());
            REQUIRE(recover_result11.status().ToString() == "IOError: Invalid offset into SubFile");
            if (cleanup) {
                CHECK_FALSE(exists(recovered_file_path));
            }
            CHECK(exists(path_to_recover));
            CHECK(exists(reads_path));
            CHECK(exists(run_path));
        }

        SECTION("Recover .pod5.tmp missing signal sub file.")
        {
            pod5::Uuid section_id = uuid_gen();
            REQUIRE_ARROW_STATUS_OK(tmp_file->Write(section_id.data(), section_id.size()));
            tmp_file = {};
            auto recover_result12 = pod5::recover_file(to_recover, recovered, options);
            REQUIRE_FALSE(recover_result12.ok());
            REQUIRE(
                recover_result12.status().ToString()
                == "Invalid: Failed whilst attempting to recover signal data sub file from file - "
                       + to_recover + ". Detail: Not an Arrow file");
            if (cleanup) {
                CHECK_FALSE(exists(recovered_file_path));
            }
            CHECK(exists(path_to_recover));
            CHECK(exists(reads_path));
            CHECK(exists(run_path));
        }
    }
}
