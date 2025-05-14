#pragma once

// This file contains code from https://github.com/mariusbancila/stduuid/ which has the following
// license:
//
//   MIT License
//
//   Copyright (c) 2017
//
//   Permission is hereby granted, free of charge, to any person obtaining a copy
//   of this software and associated documentation files (the "Software"), to deal
//   in the Software without restriction, including without limitation the rights
//   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//   copies of the Software, and to permit persons to whom the Software is
//   furnished to do so, subject to the following conditions:
//
//   The above copyright notice and this permission notice shall be included in all
//   copies or substantial portions of the Software.
//
//   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//   SOFTWARE.

#include <array>
#include <cstdint>
#include <iosfwd>
#include <optional>
#include <random>
#include <string>
#include <string_view>

namespace pod5 {

namespace uuid_detail {

template <typename TChar>
[[nodiscard]] constexpr inline unsigned char hex2char(TChar const ch) noexcept
{
    if (ch >= static_cast<TChar>('0') && ch <= static_cast<TChar>('9')) {
        return static_cast<unsigned char>(ch - static_cast<TChar>('0'));
    }
    if (ch >= static_cast<TChar>('a') && ch <= static_cast<TChar>('f')) {
        return static_cast<unsigned char>(10 + ch - static_cast<TChar>('a'));
    }
    if (ch >= static_cast<TChar>('A') && ch <= static_cast<TChar>('F')) {
        return static_cast<unsigned char>(10 + ch - static_cast<TChar>('A'));
    }
    return 0;
}

template <typename TChar>
[[nodiscard]] constexpr inline bool is_hex(TChar const ch) noexcept
{
    return (ch >= static_cast<TChar>('0') && ch <= static_cast<TChar>('9'))
           || (ch >= static_cast<TChar>('a') && ch <= static_cast<TChar>('f'))
           || (ch >= static_cast<TChar>('A') && ch <= static_cast<TChar>('F'));
}

template <typename TChar>
[[nodiscard]] constexpr std::basic_string_view<TChar> to_string_view(TChar const * str) noexcept
{
    if (str) {
        return str;
    }
    return {};
}

template <typename StringType>
[[nodiscard]] constexpr std::
    basic_string_view<typename StringType::value_type, typename StringType::traits_type>
    to_string_view(StringType const & str) noexcept
{
    return str;
}

template <typename CharT>
inline constexpr CharT empty_guid[37] = "00000000-0000-0000-0000-000000000000";

template <>
inline constexpr wchar_t empty_guid<wchar_t>[37] = L"00000000-0000-0000-0000-000000000000";

template <typename CharT>
inline constexpr CharT guid_encoder[17] = "0123456789abcdef";

template <>
inline constexpr wchar_t guid_encoder<wchar_t>[17] = L"0123456789abcdef";

}  // namespace uuid_detail

// Forward declare uuid & to_string so that we can declare to_string as a friend later.
class Uuid;
template <
    class CharT = char,
    class Traits = std::char_traits<CharT>,
    class Allocator = std::allocator<CharT>>
std::basic_string<CharT, Traits, Allocator> to_string(Uuid const & id);

/// A representation of a Universally Unique IDentifier.
///
/// This code implements part of RFC 4122. It does not aim to be a complete implementation of UUIDs,
/// but provides enough for the uses in the POD5 library.
class Uuid {
public:
    using value_type = uint8_t;

    constexpr Uuid() noexcept = default;

    Uuid(value_type const (&arr)[16]) noexcept
    {
        std::copy(std::cbegin(arr), std::cend(arr), std::begin(m_data));
    }

    constexpr Uuid(std::array<value_type, 16> const & arr) noexcept : m_data{arr} {}

    template <typename ForwardIterator>
    explicit Uuid(ForwardIterator first, ForwardIterator last)
    {
        if (std::distance(first, last) == 16) {
            std::copy(first, last, std::begin(m_data));
        }
    }

    [[nodiscard]] constexpr bool is_nil() const noexcept
    {
        for (size_t i = 0; i < m_data.size(); ++i) {
            if (m_data[i] != 0) {
                return false;
            }
        }
        return true;
    }

    void swap(Uuid & other) noexcept { m_data.swap(other.m_data); }

    uint8_t const * data() const noexcept { return m_data.data(); }

    size_t size() const noexcept { return m_data.size(); }

    void to_c_array(value_type (&arr)[16]) const noexcept
    {
        std::copy(std::cbegin(m_data), std::cend(m_data), std::begin(arr));
    }

    // Note: uustr must be at least 36 characters
    template <typename CharT>
    void write_to(CharT * uustr) const noexcept
    {
        for (size_t i = 0, index = 0; i < 36; ++i) {
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                uustr[i] = uuid_detail::empty_guid<CharT>[i];
                continue;
            }
            uustr[i] = uuid_detail::guid_encoder<CharT>[m_data[index] >> 4 & 0x0f];
            uustr[++i] = uuid_detail::guid_encoder<CharT>[m_data[index] & 0x0f];
            index++;
        }
    }

    template <typename StringType>
    [[nodiscard]] constexpr static std::optional<Uuid> from_string(
        StringType const & in_str) noexcept
    {
        auto str = uuid_detail::to_string_view(in_str);
        bool firstDigit = true;
        size_t hasBraces = 0;
        size_t index = 0;

        std::array<uint8_t, 16> data{{0}};

        if (str.empty()) {
            return {};
        }

        if (str.front() == '{') {
            hasBraces = 1;
        }
        if (hasBraces && str.back() != '}') {
            return {};
        }

        for (size_t i = hasBraces; i < str.size() - hasBraces; ++i) {
            if (str[i] == '-') {
                continue;
            }

            if (index >= 16 || !uuid_detail::is_hex(str[i])) {
                return {};
            }

            if (firstDigit) {
                data[index] = static_cast<uint8_t>(uuid_detail::hex2char(str[i]) << 4);
                firstDigit = false;
            } else {
                data[index] = static_cast<uint8_t>(data[index] | uuid_detail::hex2char(str[i]));
                index++;
                firstDigit = true;
            }
        }

        if (index < 16) {
            return {};
        }

        return Uuid{data};
    }

private:
    std::array<value_type, 16> m_data{{0}};

    friend bool operator==(Uuid const & lhs, Uuid const & rhs) noexcept;
    friend bool operator<(Uuid const & lhs, Uuid const & rhs) noexcept;

    template <class Elem, class Traits>
    friend std::basic_ostream<Elem, Traits> & operator<<(
        std::basic_ostream<Elem, Traits> & s,
        Uuid const & id);

    template <class CharT, class Traits, class Allocator>
    friend std::basic_string<CharT, Traits, Allocator> to_string(Uuid const & id);

    friend std::hash<Uuid>;
};

[[nodiscard]] inline bool operator==(Uuid const & lhs, Uuid const & rhs) noexcept
{
    return lhs.m_data == rhs.m_data;
}

[[nodiscard]] inline bool operator!=(Uuid const & lhs, Uuid const & rhs) noexcept
{
    return !(lhs == rhs);
}

[[nodiscard]] inline bool operator<(Uuid const & lhs, Uuid const & rhs) noexcept
{
    return lhs.m_data < rhs.m_data;
}

template <class CharT, class Traits, class Allocator>
[[nodiscard]] inline std::basic_string<CharT, Traits, Allocator> to_string(Uuid const & id)
{
    std::basic_string<CharT, Traits, Allocator> uustr{uuid_detail::empty_guid<CharT>};
    id.write_to(uustr.data());
    return uustr;
}

template <class Elem, class Traits>
std::basic_ostream<Elem, Traits> & operator<<(std::basic_ostream<Elem, Traits> & s, Uuid const & id)
{
    s << to_string(id);
    return s;
}

inline void swap(Uuid & lhs, Uuid & rhs) noexcept { lhs.swap(rhs); }

template <typename UniformRandomNumberGenerator>
class BasicUuidRandomGenerator {
public:
    using engine_type = UniformRandomNumberGenerator;

    explicit BasicUuidRandomGenerator(engine_type & gen) : generator(&gen) {}

    explicit BasicUuidRandomGenerator(engine_type * gen) : generator(gen) {}

    [[nodiscard]] Uuid operator()()
    {
        alignas(uint32_t) uint8_t bytes[16];
        for (int i = 0; i < 16; i += 4) {
            *reinterpret_cast<uint32_t *>(bytes + i) = distribution(*generator);
        }

        // variant must be 10xxxxxx
        bytes[8] &= 0xBF;
        bytes[8] |= 0x80;

        // version must be 0100xxxx
        bytes[6] &= 0x4F;
        bytes[6] |= 0x40;

        return Uuid{std::begin(bytes), std::end(bytes)};
    }

private:
    std::uniform_int_distribution<uint32_t> distribution;
    UniformRandomNumberGenerator * generator;
};

using UuidRandomGenerator = BasicUuidRandomGenerator<std::mt19937>;

}  // namespace pod5

namespace std {
template <>
struct hash<pod5::Uuid> {
    using argument_type = pod5::Uuid;
    using result_type = std::size_t;

    [[nodiscard]] result_type operator()(argument_type const & uuid) const
    {
        uint64_t l = static_cast<uint64_t>(uuid.m_data[0]) << 56
                     | static_cast<uint64_t>(uuid.m_data[1]) << 48
                     | static_cast<uint64_t>(uuid.m_data[2]) << 40
                     | static_cast<uint64_t>(uuid.m_data[3]) << 32
                     | static_cast<uint64_t>(uuid.m_data[4]) << 24
                     | static_cast<uint64_t>(uuid.m_data[5]) << 16
                     | static_cast<uint64_t>(uuid.m_data[6]) << 8
                     | static_cast<uint64_t>(uuid.m_data[7]);
        uint64_t h = static_cast<uint64_t>(uuid.m_data[8]) << 56
                     | static_cast<uint64_t>(uuid.m_data[9]) << 48
                     | static_cast<uint64_t>(uuid.m_data[10]) << 40
                     | static_cast<uint64_t>(uuid.m_data[11]) << 32
                     | static_cast<uint64_t>(uuid.m_data[12]) << 24
                     | static_cast<uint64_t>(uuid.m_data[13]) << 16
                     | static_cast<uint64_t>(uuid.m_data[14]) << 8
                     | static_cast<uint64_t>(uuid.m_data[15]);

        if constexpr (sizeof(result_type) > 4) {
            return result_type(l ^ h);
        } else {
            uint64_t hash64 = l ^ h;
            return result_type(uint32_t(hash64 >> 32) ^ uint32_t(hash64));
        }
    }
};
}  // namespace std
