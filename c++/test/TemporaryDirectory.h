#pragma once

#include "pod5_format/uuid.h"

#include <filesystem>

namespace ont { namespace testutils {

static std::string make_unique_name()
{
    std::random_device gen;
    auto uuid_gen = pod5::BasicUuidRandomGenerator<std::random_device>{gen};
    return to_string(uuid_gen());
}

/// A scoped directory with a fixed name.
class TemporaryDirectory {
public:
    /// Where to create the directory.
    enum class Location { CurrentDir, TempDir };
    enum class DeleteBehaviour { AfterOnly, BeforeAndAfter };

    /// Creates a random temporary directory
    TemporaryDirectory()
    : TemporaryDirectory(make_unique_name(), Location::TempDir, DeleteBehaviour::AfterOnly)
    {
    }

    /// Create a directory.
    explicit TemporaryDirectory(std::filesystem::path path, DeleteBehaviour delete_behaviour)
    : TemporaryDirectory(std::move(path), Location::CurrentDir, delete_behaviour)
    {
    }

    /// Create a directory.
    explicit TemporaryDirectory(
        std::filesystem::path path,
        Location location = Location::CurrentDir,
        DeleteBehaviour delete_behaviour = DeleteBehaviour::AfterOnly)
    {
        if (!path.is_absolute()) {
            if (location == Location::CurrentDir) {
                path = std::filesystem::absolute(path);
            } else {
                path = std::filesystem::temp_directory_path() / path;
            }
        }
        if (delete_behaviour == DeleteBehaviour::BeforeAndAfter) {
            std::filesystem::remove_all(m_path);
        }
        std::filesystem::create_directories(path);
        m_path = path;
    }

    TemporaryDirectory(TemporaryDirectory const &) = delete;
    TemporaryDirectory & operator=(TemporaryDirectory const &) = delete;

    TemporaryDirectory(TemporaryDirectory &&) = default;
    TemporaryDirectory & operator=(TemporaryDirectory &&) = default;

    /// Remove the referenced directory.
    ///
    /// Does nothing if this is not a valid object.
    ~TemporaryDirectory()
    {
        if (!m_path.empty()) {
            std::error_code error;
            std::filesystem::remove_all(m_path, error);
        }
    }

    /// Path to the directory.
    std::filesystem::path const & path() const { return m_path; }

    explicit operator bool() const { return !m_path.empty(); }

private:
    std::filesystem::path m_path;
};

template <class CharType, class CharTrait>
std::basic_ostream<CharType, CharTrait> & operator<<(
    std::basic_ostream<CharType, CharTrait> & os,
    TemporaryDirectory const & td)
{
    return os << "TemporaryDirectory{ " << td.path() << " }";
}

}}  // namespace ont::testutils
