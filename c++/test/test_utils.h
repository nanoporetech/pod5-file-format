#pragma once

#include <arrow/status.h>
#include <catch2/catch.hpp>

template <bool CheckOk>
class IsStatusOk : public Catch::MatcherBase<arrow::Status> {
public:
    IsStatusOk() = default;

    bool match(arrow::Status const & status) const override { return status.ok() == CheckOk; }

    virtual std::string describe() const override { return "== arrow::Status::OK()"; }
};

template <bool CheckOk, typename T>
class IsResultOk : public Catch::MatcherBase<arrow::Result<T>> {
public:
    IsResultOk() = default;

    bool match(arrow::Result<T> const & status) const override { return status.ok() == CheckOk; }

    virtual std::string describe() const override { return "== arrow::Status::OK()"; }
};

template <typename T>
inline IsResultOk<true, T> _is_arrow_ok(arrow::Result<T> const &)
{
    return IsResultOk<true, T>();
}

inline IsStatusOk<true> _is_arrow_ok(arrow::Status const &) { return IsStatusOk<true>(); }

template <typename T>
inline IsResultOk<false, T> _is_arrow_not_ok(arrow::Result<T> const &)
{
    return IsResultOk<false, T>();
}

inline IsStatusOk<false> _is_arrow_not_ok(arrow::Status const &) { return IsStatusOk<false>(); }

#define CHECK_ARROW_STATUS_OK(statement)      \
    {                                         \
        auto const & _res = (statement);      \
        CHECK_THAT(_res, _is_arrow_ok(_res)); \
    }
#define REQUIRE_ARROW_STATUS_OK(statement)      \
    {                                           \
        auto const & _res = (statement);        \
        REQUIRE_THAT(_res, _is_arrow_ok(_res)); \
    }
#define CHECK_ARROW_STATUS_NOT_OK(statement)      \
    {                                             \
        auto const & _res = (statement);          \
        CHECK_THAT(_res, _is_arrow_not_ok(_res)); \
    }
#define REQUIRE_ARROW_STATUS_NOT_OK(statement)      \
    {                                               \
        auto const & _res = (statement);            \
        REQUIRE_THAT(_res, _is_arrow_not_ok(_res)); \
    }
