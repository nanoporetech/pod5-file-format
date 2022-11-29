#pragma once

#include <tuple>
#include <utility>

namespace pod5 { namespace detail {

template <typename T, typename F, int... Is>
void for_each(T && t, F f, std::integer_sequence<int, Is...>)
{
    auto l = {(f(std::get<Is>(t), Is), 0)...};
    (void)l;
}

template <typename... Ts, typename F>
void for_each_in_tuple(std::tuple<Ts...> & t, F f)
{
    detail::for_each(t, f, std::make_integer_sequence<int, sizeof...(Ts)>());
}

template <typename T1, typename T2, typename F, int... Is>
void for_each_zipped(T1 && t1, T2 && t2, F f, std::integer_sequence<int, Is...>)
{
    auto l = {(f(std::get<Is>(t1), std::get<Is>(t2), Is), 0)...};
    (void)l;
}

template <typename T1, typename T2, typename F>
void for_each_in_tuple_zipped(T1 & t1, T2 & t2, F f)
{
    static_assert(
        std::tuple_size<T1>::value == std::tuple_size<T2>::value, "Tuples must be same size");
    detail::for_each_zipped(
        t1, t2, f, std::make_integer_sequence<int, std::tuple_size<T1>::value>());
}

}}  // namespace pod5::detail
