#pragma once

namespace pod5 {

using SignalTableRowIndex = std::uint64_t;

enum class SignalType {
    UncompressedSignal,
    VbzSignal,
    // New values must be added at the end: the existing values are exposed to
    // Python as a py::arithmetic() enum and must stay stable.
    PdzSignal,
};

}  // namespace pod5
