#pragma once

#include "pod5_format/result.h"

inline void raise_error(arrow::Status const & status)
{
    throw std::runtime_error(status.ToString());
}

template <typename T>
inline void raise_error(arrow::Result<T> const & result)
{
    throw std::runtime_error(result.status().ToString());
}

#define POD5_PYTHON_RETURN_NOT_OK(result) \
    if (!result.ok()) {                   \
        raise_error(result);              \
    }

#define POD5_PYTHON_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr) \
    auto && result_name = (rexpr);                                \
    if (!(result_name).ok()) {                                    \
        raise_error(result_name);                                 \
    }                                                             \
    lhs = std::move(result_name).ValueUnsafe();

#define POD5_PYTHON_ASSIGN_OR_RAISE(lhs, rexpr) \
    POD5_PYTHON_ASSIGN_OR_RAISE_IMPL(           \
        ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

inline void throw_on_error(pod5::Status const & s)
{
    if (!s.ok()) {
        throw std::runtime_error(s.ToString());
    }
}

template <typename T>
inline T throw_on_error(pod5::Result<T> const & s)
{
    if (!s.ok()) {
        throw std::runtime_error(s.status().ToString());
    }
    return *s;
}
