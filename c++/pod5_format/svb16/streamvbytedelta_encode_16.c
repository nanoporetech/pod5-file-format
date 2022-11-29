#include "streamvbyte_isadetection.h"
#include "streamvbytedelta.h"

#include <stdio.h>
#include <string.h>  // for memcpy

#ifdef STREAMVBYTE_X64
#include "streamvbytedelta_x64_encode_16.c"
#endif

static inline uint16_t _zigzag_encode_16(uint16_t val)
{
    return (val + val) ^ ((int16_t)val >> 15);
}

static uint8_t _encode_data(uint16_t val, uint8_t * __restrict__ * dataPtrPtr)
{
    uint8_t * dataPtr = *dataPtrPtr;
    uint8_t code;

    if (val < (1 << 8)) {  // 1 byte
        *dataPtr = (uint8_t)(val);
        *dataPtrPtr += 1;
        code = 0;
    } else {                       // 2 bytes
        memcpy(dataPtr, &val, 2);  // assumes little endian
        *dataPtrPtr += 2;
        code = 1;
    }

    return code;
}

static uint8_t * svb_encode_scalar_d1_init(
    uint16_t const * in,
    uint8_t * __restrict__ keyPtr,
    uint8_t * __restrict__ dataPtr,
    uint32_t count,
    uint16_t prev)
{
    if (count == 0)
        return dataPtr;  // exit immediately if no data

    uint8_t shift = 0;  // cycles 0 through 7 then resets
    uint8_t key = 0;
    for (uint32_t c = 0; c < count; c++) {
        if (shift == 8) {
            shift = 0;
            *keyPtr++ = key;
            key = 0;
        }
        uint16_t val = _zigzag_encode_16((uint16_t)(in[c] - prev));
        //uint16_t val = in[c] - prev;
        prev = in[c];
        uint8_t code = _encode_data(val, &dataPtr);
        key |= code << shift;
        shift += 1;
    }

    *keyPtr = key;   // write last key (no increment needed)
    return dataPtr;  // pointer to first unused data byte
}

size_t streamvbyte_zigzag_delta_encode_16(
    uint16_t const * in,
    uint32_t count,
    uint8_t * out,
    uint16_t prev)
{
#ifdef STREAMVBYTE_X64
    if (streamvbyte_ssse3()) {
        return streamvbyte_zigzag_delta_encode_SSSE3_d1_init(in, count, out, prev);
    }
#endif
    uint8_t * keyPtr = out;  // keys come at start
    // keyLen = ceil(count / 8), without overflowing (1 bit per input value):
    uint32_t keyLen = (count >> 3) + (((count & 7) + 7) >> 3);
    uint8_t * dataPtr = keyPtr + keyLen;  // variable byte data after all keys
    return svb_encode_scalar_d1_init(in, keyPtr, dataPtr, count, prev) - out;
}
