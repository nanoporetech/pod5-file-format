#include <pod5_format/c_api.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#else
#include <process.h>

static int setenv(char const * name, char const * value, int) { return _putenv_s(name, value); }
#endif

// No access to arrow in shared lib builds.
#if !BUILD_SHARED_LIB
#include <arrow/memory_pool.h>
#endif

#ifdef NDEBUG
#error "asserts aren't enabled"
#endif

namespace {
// Global state
std::string s_file_name;

// Write out data to the temp file.
char const * write_data(void const * data, size_t size)
{
    FILE * f = fopen(s_file_name.c_str(), "w");
    assert(f != nullptr);
    fwrite(data, 1, size, f);
    fclose(f);
    return s_file_name.c_str();
}

// Check the result of a POD5 call.
void check_pod5_ok(pod5_error_t err, char const * msg)
{
    if (err != POD5_OK) {
        printf("Assertion failed: %s - %i - %s\n", msg, err, pod5_get_error_string());
        assert(false);
    }
}

// Check that the return value of a function always matches pod5_get_error_no().
void check_pod5_consistency(pod5_error_t err, char const * msg)
{
    if (err != pod5_get_error_no()) {
        printf("POD5 inconsistency: %s - %i != %i\n", msg, err, pod5_get_error_no());
        assert(false);
    }
}

#define CHECK_POD5_SUCCESS(func)             \
    do {                                     \
        auto _res = func;                    \
        check_pod5_ok(_res, #func);          \
        check_pod5_consistency(_res, #func); \
    } while (false)

#define CHECK_POD5_MAY_FAIL(func) check_pod5_consistency(func, #func)

// Helper to stop the optimiser from removing results.
template <typename T>
void keep_result(T && t)
{
    auto volatile v = t;
    (void)v;
}

// Make sure that a string is NUL-terminated.
void validate_string(char const * ptr)
{
    std::string str = ptr;
    keep_result(str);
}

// File handle wrapper
struct POD5FileCloser {
    void operator()(Pod5FileReader_t * file)
    {
        if (file != nullptr) {
            CHECK_POD5_SUCCESS(pod5_close_and_free_reader(file));
        }
    }
};

using POD5File = std::unique_ptr<Pod5FileReader_t, POD5FileCloser>;
}  // namespace

extern "C" int LLVMFuzzerInitialize(int * argc, char *** argv)
{
    // Make sure arrow uses the system allocator
    setenv("ARROW_DEFAULT_MEMORY_POOL", "system", 1);
#if !BUILD_SHARED_LIB
    assert(arrow::system_memory_pool()->backend_name() == "system");
#endif

    // Init POD5
    CHECK_POD5_SUCCESS(pod5_init());

    // Setup state shared for all runs
    s_file_name = "./fuzz_tmp_" + std::to_string(getpid());
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(uint8_t const * data, size_t size)
{
    // Write the input to a file
    char const * file_path = write_data(data, size);

    // Try and open it
    POD5File file(pod5_open_file(file_path));
    if (file == nullptr) {
        return 0;
    }

    // Check that we can query info about the file.
    FileInfo_t file_info{};
    CHECK_POD5_SUCCESS(pod5_get_file_info(file.get(), &file_info));

    // If we need any more randomness, use the file's ID as a seed.
    std::seed_seq seed(std::begin(file_info.file_identifier), std::end(file_info.file_identifier));
    std::mt19937_64 rng(seed);

    // See what IDs there are
    std::size_t batch_count = 0;
    CHECK_POD5_SUCCESS(pod5_get_read_batch_count(&batch_count, file.get()));

    std::size_t total_read_count = 0;
    for (std::size_t batch_index = 0; batch_index < batch_count; ++batch_index) {
        Pod5ReadRecordBatch_t * batch = nullptr;
        CHECK_POD5_MAY_FAIL(pod5_get_read_batch(&batch, file.get(), batch_index));
        if (batch == nullptr) {
            continue;
        }

        std::size_t batch_row_count = 0;
        CHECK_POD5_SUCCESS(pod5_get_read_batch_row_count(&batch_row_count, batch));
        total_read_count += batch_row_count;

        for (std::size_t row = 0; row < batch_row_count; ++row) {
            uint16_t read_table_version = 0;
            ReadBatchRowInfo_t read_data;
            CHECK_POD5_SUCCESS(pod5_get_read_batch_row_info_data(
                batch, row, READ_BATCH_ROW_INFO_VERSION, &read_data, &read_table_version));

            // Check read formatter.
            std::array<char, 37> formatted_read_id;
            CHECK_POD5_SUCCESS(pod5_format_read_id(read_data.read_id, formatted_read_id.data()));
            validate_string(formatted_read_id.data());

            // Check signal indices.
            assert(read_data.signal_row_count >= 0);
            if (read_data.signal_row_count > 0 && read_data.signal_row_count < 1'000'000) {
                std::vector<uint64_t> indices(read_data.signal_row_count);
                CHECK_POD5_SUCCESS(
                    pod5_get_signal_row_indices(batch, row, indices.size(), indices.data()));
            }

            // Check signal extraction.
            std::size_t sample_count = 0;
            CHECK_POD5_MAY_FAIL(
                pod5_get_read_complete_sample_count(file.get(), batch, row, &sample_count));
            if (sample_count < 1'000'000) {
                std::vector<int16_t> samples(sample_count);
                CHECK_POD5_MAY_FAIL(pod5_get_read_complete_signal(
                    file.get(), batch, row, samples.size(), samples.data()));
            }

            // Check calibration data.
            CalibrationExtraData_t calib_data;
            CHECK_POD5_MAY_FAIL(pod5_get_calibration_extra_info(batch, row, &calib_data));

            // Check run info.
            RunInfoDictData_t * run_info = nullptr;
            CHECK_POD5_MAY_FAIL(pod5_get_run_info(batch, read_data.run_info, &run_info));
            // We'll do a proper check of the run info later.
            if (run_info != nullptr) {
                CHECK_POD5_SUCCESS(pod5_free_run_info(run_info));
            }
        }

        // Check run info.
        run_info_index_t run_info_count = 0;
        CHECK_POD5_MAY_FAIL(pod5_get_file_run_info_count(file.get(), &run_info_count));
        for (run_info_index_t run_info_idx = 0; run_info_idx < run_info_count; run_info_idx++) {
            RunInfoDictData_t * run_info_data = nullptr;
            CHECK_POD5_SUCCESS(pod5_get_file_run_info(file.get(), run_info_idx, &run_info_data));
            assert(run_info_data != nullptr);

            validate_string(run_info_data->acquisition_id);
            validate_string(run_info_data->experiment_name);
            validate_string(run_info_data->flow_cell_id);
            validate_string(run_info_data->flow_cell_product_code);
            validate_string(run_info_data->protocol_name);
            validate_string(run_info_data->protocol_run_id);
            validate_string(run_info_data->sample_id);
            validate_string(run_info_data->sequencing_kit);
            validate_string(run_info_data->sequencer_position);
            validate_string(run_info_data->sequencer_position_type);
            validate_string(run_info_data->software);
            validate_string(run_info_data->system_name);
            validate_string(run_info_data->system_type);
            for (std::size_t i = 0; i < run_info_data->context_tags.size; i++) {
                validate_string(run_info_data->context_tags.keys[i]);
                validate_string(run_info_data->context_tags.values[i]);
            }
            for (std::size_t i = 0; i < run_info_data->tracking_id.size; i++) {
                validate_string(run_info_data->tracking_id.keys[i]);
                validate_string(run_info_data->tracking_id.values[i]);
            }

            CHECK_POD5_SUCCESS(pod5_free_run_info(run_info_data));
        }

        // Cleanup.
        CHECK_POD5_SUCCESS(pod5_free_read_batch(batch));
    }

    {
        // Check total read count matches.
        std::size_t read_count = 0;
        CHECK_POD5_MAY_FAIL(pod5_get_read_count(file.get(), &read_count));
        if (pod5_get_error_no() == POD5_OK) {
            assert(read_count == total_read_count);
        } else {
            read_count = 0;
        }

        if (read_count > 0) {
            // Query all the reads IDs.
            std::vector<uint8_t> read_ids(read_count * sizeof(read_id_t));
            CHECK_POD5_SUCCESS(pod5_get_read_ids(
                file.get(), read_count, reinterpret_cast<read_id_t *>(read_ids.data())));

            // Randomise the order of the read IDs and then try and plan a path through them.
            std::shuffle(read_ids.begin(), read_ids.end(), rng);
            std::vector<std::uint32_t> batch_counts(read_count);
            std::vector<std::uint32_t> batch_rows(read_count);
            std::size_t find_success_count = 0;
            CHECK_POD5_MAY_FAIL(pod5_plan_traversal(
                file.get(),
                reinterpret_cast<uint8_t const *>(read_ids.data()),
                read_count,
                batch_counts.data(),
                batch_rows.data(),
                &find_success_count));
            assert(find_success_count <= read_count);
        }
    }

    // Check embedded files.
    {
        for (auto * pod5_get_embedded_file : {
                 pod5_get_file_read_table_location,
                 pod5_get_file_signal_table_location,
                 pod5_get_file_run_info_table_location,
             })
        {
            EmbeddedFileData_t file_data{};
            CHECK_POD5_SUCCESS(pod5_get_embedded_file(file.get(), &file_data));
            validate_string(file_data.file_name);
            assert(file_data.offset <= size);
            assert(file_data.length <= size - file_data.offset);
        }
    }

    return 0;
}
