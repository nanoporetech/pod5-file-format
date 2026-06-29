#pragma once

namespace pod5 { namespace pdz {

// Selects which implementation of the piecewise split runs. `Native` picks the
// best backend compiled in for the current architecture (NEON on ARM, SSE on
// x86, scalar elsewhere); `Scalar` forces the portable reference path used as
// the oracle in the cross-architecture tests.
enum class SimdBackend {
    Native,
    Scalar,
    Sse,
    Neon,
};

}}  // namespace pod5::pdz
