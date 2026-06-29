#include "pod5_format/pdz_compression.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#else
#include <process.h>

static int setenv(char const * name, char const * value, int) { return _putenv_s(name, value); }
#endif

// This fuzzer drives the PDZ C++ adapter directly, which takes an
// arrow::MemoryPool; arrow is only linked into static builds, so the body is
// guarded on !BUILD_SHARED_LIB and is a no-op in shared-library builds.
#if !BUILD_SHARED_LIB
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <gsl/gsl-lite.hpp>
#endif

#ifdef NDEBUG
#error "asserts aren't enabled"
#endif

extern "C" int LLVMFuzzerInitialize(int * argc, char *** argv)
{
    static_cast<void>(argc);
    static_cast<void>(argv);
    // Make sure arrow uses the system allocator (so ASan sees every allocation).
    setenv("ARROW_DEFAULT_MEMORY_POOL", "system", 1);
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(uint8_t const * data, size_t size)
{
    // PDZ operates on int16 samples.
    if (size < sizeof(int16_t)) {
        return 0;
    }

#if !BUILD_SHARED_LIB
    // Copy into a correctly typed buffer so we still get bounds checking when the
    // input length is odd.
    std::vector<int16_t> input(size / sizeof(int16_t));
    std::memcpy(input.data(), data, input.size() * sizeof(int16_t));

    auto * const pool = arrow::system_memory_pool();

    // A round trip through the adapter must be lossless.
    auto compressed = pod5::compress_signal_pdz(gsl::make_span(input), pool);
    assert(compressed.ok());
    auto const buffer = *compressed;

    std::vector<int16_t> output(input.size());
    auto const status = pod5::decompress_signal_pdz(
        gsl::make_span(buffer->data(), buffer->size()), pool, gsl::make_span(output));
    assert(status.ok());
    assert(input == output);

    // Treating the raw fuzz bytes as a compressed block must never crash or write
    // out of bounds — the safety net is expected to reject it.
    std::vector<int16_t> scratch(input.size());
    static_cast<void>(pod5::decompress_signal_pdz(
        gsl::make_span(data, size), pool, gsl::make_span(scratch)));
#else
    static_cast<void>(data);
#endif

    return 0;
}
