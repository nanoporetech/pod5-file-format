#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace pod5 { namespace pdz {

// Allocate `n` bytes rounded up to a multiple of `alignment_bytes`, aligned to
// `alignment_bytes`. The 32-byte alignment used by PDZ satisfies both SSE and
// NEON load/store requirements. `aligned_alloc` (C11) is unavailable on MSVC,
// so dispatch to the matching platform allocation/free pair.
inline uint8_t * padded_aligned_malloc(size_t n, size_t alignment_bytes)
{
    if (n % alignment_bytes != 0) {
        n = (n / alignment_bytes) * alignment_bytes + alignment_bytes;
    }
#ifdef _MSC_VER
    return static_cast<uint8_t *>(_aligned_malloc(n, alignment_bytes));
#else
    return static_cast<uint8_t *>(aligned_alloc(alignment_bytes, n));
#endif
}

inline void padded_aligned_free(void * x)
{
#ifdef _MSC_VER
    _aligned_free(x);
#else
    free(x);
#endif
}

template <typename T = uint8_t>
inline T * padded_aligned_malloc_t(size_t n, size_t alignment_bytes)
{
    return static_cast<T *>(
        static_cast<void *>(padded_aligned_malloc(n * sizeof(T), alignment_bytes)));
}

}}  // namespace pod5::pdz
