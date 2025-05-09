#pragma once

#include "pod5_format/c_api.h"

#include <catch2/catch.hpp>

#include <string>

#define CHECK_POD5_OK(statement)                                                    \
    {                                                                               \
        auto const & _res = (statement);                                            \
        CHECK_THAT(testutils::Pod5C_Result::capture(_res), testutils::IsPod5COk()); \
    }

#define REQUIRE_POD5_OK(statement)                                                    \
    {                                                                                 \
        auto const & _res = (statement);                                              \
        REQUIRE_THAT(testutils::Pod5C_Result::capture(_res), testutils::IsPod5COk()); \
    }

#define CHECK_POD5_NOT_OK(statement)                                                 \
    {                                                                                \
        auto const & _res = (statement);                                             \
        CHECK_THAT(testutils::Pod5C_Result::capture(_res), !testutils::IsPod5COk()); \
    }

#define REQUIRE_POD5_NOT_OK(statement)                                                 \
    {                                                                                  \
        auto const & _res = (statement);                                               \
        REQUIRE_THAT(testutils::Pod5C_Result::capture(_res), !testutils::IsPod5COk()); \
    }

namespace testutils {

struct Pod5C_Result {
    static Pod5C_Result capture(pod5_error_t err_num)
    {
        return Pod5C_Result{err_num, pod5_get_error_string()};
    }

    pod5_error_t error_code;
    std::string error_string;
};

class IsPod5COk : public Catch::MatcherBase<Pod5C_Result> {
public:
    IsPod5COk() = default;

    bool match(Pod5C_Result const & result) const override { return result.error_code == POD5_OK; }

    virtual std::string describe() const override { return "== POD5_OK"; }
};

}  // namespace testutils

template <>
struct Catch::StringMaker<testutils::Pod5C_Result> {
    static std::string convert(testutils::Pod5C_Result const & value)
    {
        return "{ code: " + std::to_string(value.error_code) + "| " + value.error_string + " }";
    }
};
