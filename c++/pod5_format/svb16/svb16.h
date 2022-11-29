#ifndef SVB16_H
#define SVB16_H

#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/// Get the number of key bytes required to encode a given number of 16-bit integers.
inline uint32_t svb16_key_length(uint32_t count)
{
    // ceil(count / 8.0), without overflowing or using fp arithmetic
    return (count >> 3) + (((count & 7) + 7) >> 3);
}

/// Get the maximum number of bytes required to encode a given number of 16-bit integers.
inline uint32_t svb16_max_encoded_length(uint32_t count)
{
    return svb16_key_length(count) + (2 * count);
}

#if defined(__cplusplus)
};
#endif

#endif  // SVB16_H
