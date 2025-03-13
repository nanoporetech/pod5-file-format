#include <pod5_format/c_api.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

// No access to arrow in shared lib builds.
#if !BUILD_SHARED_LIB
#include <arrow/memory_pool.h>
#endif

#ifdef NDEBUG
#error "asserts aren't enabled"
#endif

namespace {
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
}  // namespace

extern "C" int LLVMFuzzerInitialize(int * argc, char *** argv)
{
    // Make sure arrow uses the system allocator
    setenv("ARROW_DEFAULT_MEMORY_POOL", "system", 1);
#if !BUILD_SHARED_LIB
    assert(arrow::system_memory_pool()->backend_name() == "system");
#endif

    // Init pod5
    CHECK_POD5(pod5_init());
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(uint8_t const * data, size_t size)
{
    // POD5 requires non-empty input
    if (size < sizeof(int16_t)) {
        return 0;
    }

    // Copy to a new buffer of the "right" type so that we get bounds checking even if the length wasn't even
    std::vector<int16_t> input(size / sizeof(int16_t));
    std::memcpy(input.data(), data, input.size() * sizeof(int16_t));

    // Compress it
    size_t const max_compressed_size = pod5_vbz_compressed_signal_max_size(input.size());
    std::vector<char> compressed_data(max_compressed_size);
    size_t compressed_size = compressed_data.size();
    CHECK_POD5(pod5_vbz_compress_signal(
        input.data(), input.size(), compressed_data.data(), &compressed_size));
    assert(compressed_size <= max_compressed_size);

    // Update size for bounds checking when decompressing
    compressed_data =
        std::vector<char>(compressed_data.begin(), compressed_data.begin() + compressed_size);

    // Decompress it
    std::vector<int16_t> output(input.size());
    CHECK_POD5(pod5_vbz_decompress_signal(
        compressed_data.data(), compressed_data.size(), output.size(), output.data()));

    // Check it decompressed correctly
    assert(input == output);

    // See how it handles random input
    std::vector<int16_t> temp(pod5_vbz_compressed_signal_max_size(size));
    pod5_vbz_decompress_signal(
        reinterpret_cast<char const *>(data), size, temp.size(), temp.data());

    return 0;
}
