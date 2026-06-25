#ifndef SVB16_H
#define SVB16_H

#include <stdint.h>

namespace svb16 {

/// Get the number of key bytes required to encode a given number of 16-bit integers.
inline uint32_t key_length(uint32_t count)
{
    // ceil(count / 8.0), without overflowing or using fp arithmetic
    return (count >> 3) + (((count & 7) + 7) >> 3);
}

/// Get the maximum number of bytes required to encode a given number of 16-bit integers.
inline uint32_t max_encoded_length(uint32_t count) { return key_length(count) + (2 * count); }

}  // namespace svb16

#endif  // SVB16_H
