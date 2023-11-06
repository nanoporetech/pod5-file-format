#pragma once

namespace pod5 {

using SignalTableRowIndex = std::uint64_t;

enum class SignalType {
    UncompressedSignal,
    VbzSignal,
};

}  // namespace pod5
