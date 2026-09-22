#pragma once

#include <cstdint>
#include <iterator>

namespace pod5::utils {

// Minimal iterator to repeat the same value N times.
// TODO: replace with std::views::repeat in C++23
template <typename T>
class RepeatIter {
public:
    // These are necessary to make |std::distance| and |std::copy| fast.
    using iterator_category [[maybe_unused]] = std::random_access_iterator_tag;
    using value_type = T const;
    using difference_type = std::int64_t;
    using pointer [[maybe_unused]] = value_type *;
    using reference = value_type &;

    explicit RepeatIter(std::size_t count, T value) : m_idx(count), m_value(value) {}

    constexpr RepeatIter & operator++()
    {
        m_idx++;
        return *this;
    }

    constexpr RepeatIter operator++(int)
    {
        RepeatIter retval = *this;
        m_idx++;
        return retval;
    }

    constexpr bool operator==(RepeatIter const & other) const { return m_idx == other.m_idx; }

    constexpr bool operator!=(RepeatIter const & other) const { return !operator==(other); }

    constexpr difference_type operator-(RepeatIter const & other) const
    {
        return static_cast<difference_type>(m_idx) - static_cast<difference_type>(other.m_idx);
    }

    constexpr reference operator*() const { return m_value; }

private:
    std::size_t m_idx;
    value_type m_value;
};

}  // namespace pod5::utils
