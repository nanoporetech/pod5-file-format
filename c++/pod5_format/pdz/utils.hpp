#pragma once

#include <cstddef>

namespace pod5 { namespace pdz {

// Ceil(a / b) for unsigned integer types. Used by the buffer-size formulas.
template <typename T>
constexpr T round_up_division(T a, T b)
{
    const auto x = a / b;
    if (a % b != 0) {
        return x + 1;
    } else {
        return x;
    }
}

}}  // namespace pod5::pdz

// Compile-time assertion helper used to reject unsupported bit widths.
#define PDZ_FAIL_COMPILE_IF(expr) static_assert(!(expr))
