#include <arrow/memory_pool.h>
#include <pod5_format/c_api.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#ifdef NDEBUG
#error "asserts aren't enabled"
#endif

namespace {
// Global state
std::string s_file_name;

// Write out data to the temp file
void WriteData(void const * data, size_t size)
{
    FILE * f = fopen(s_file_name.c_str(), "w");
    assert(f != nullptr);
    fwrite(data, 1, size, f);
    fclose(f);
}

// Check the result of a POD5 call
void CheckPod5(pod5_error_t err, char const * msg)
{
    if (err != POD5_OK) {
        printf("Assertion failed: %s - %i - %s\n", msg, err, pod5_get_error_string());
        assert(false);
    }
    if (err != pod5_get_error_no()) {
        printf("POD5 inconsistency: %s - %i != %i\n", msg, err, pod5_get_error_no());
        assert(false);
    }
}

#define CHECK_POD5(x) CheckPod5(x, #x)

// File handle wrapper
struct POD5FileCloser {
    void operator()(Pod5FileReader_t * file)
    {
        if (file != nullptr) {
            CHECK_POD5(pod5_close_and_free_reader(file));
        }
    }
};

using POD5File = std::unique_ptr<Pod5FileReader_t, POD5FileCloser>;
}  // namespace

extern "C" int LLVMFuzzerInitialize(int * argc, char *** argv)
{
    // Make sure arrow uses the system allocator
    setenv("ARROW_DEFAULT_MEMORY_POOL", "system", 1);
    assert(arrow::system_memory_pool()->backend_name() == "system");

    // Init POD5
    CHECK_POD5(pod5_init());

    // Setup state shared for all runs
    s_file_name = "./fuzz_tmp_" + std::to_string(getpid());
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(uint8_t const * data, size_t size)
{
    // Write the input to a file
    WriteData(data, size);

    // Try and open it
    POD5File file(pod5_open_file(s_file_name.c_str()));
    if (file == nullptr) {
        return 0;
    }

    // See what IDs there are
    std::size_t batch_count = 0;
    CHECK_POD5(pod5_get_read_batch_count(&batch_count, file.get()));

    for (std::size_t batch_index = 0; batch_index < batch_count; ++batch_index) {
        Pod5ReadRecordBatch_t * batch = nullptr;
        // TODO: commented out check since it can fail
        /*CHECK_POD5*/ (pod5_get_read_batch(&batch, file.get(), batch_index));
        if (batch == nullptr) {
            continue;
        }

        std::size_t batch_row_count = 0;
        CHECK_POD5(pod5_get_read_batch_row_count(&batch_row_count, batch));

        for (std::size_t row = 0; row < batch_row_count; ++row) {
            uint16_t read_table_version = 0;
            ReadBatchRowInfo_t read_data;
            CHECK_POD5(pod5_get_read_batch_row_info_data(
                batch, row, READ_BATCH_ROW_INFO_VERSION, &read_data, &read_table_version));

            std::array<char, 37> formatted_read_id;
            CHECK_POD5(pod5_format_read_id(read_data.read_id, formatted_read_id.data()));
        }

        CHECK_POD5(pod5_free_read_batch(batch));
    }

    return 0;
}
