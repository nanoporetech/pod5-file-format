#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"

#include <boost/uuid/uuid.hpp>

#include <memory>
#include <string>
#include <tuple>

namespace arrow {
class KeyValueMetadata;
}

namespace boost { namespace uuids {
struct uuid;
}}  // namespace boost::uuids

namespace pod5 {

class Version {
public:
    Version() : m_version(0, 0, 0) {}

    Version(std::uint16_t major, std::uint16_t minor, std::uint16_t revision)
    : m_version(major, minor, revision)
    {
    }

    bool operator<(Version const & in) const { return m_version < in.m_version; }

    bool operator>(Version const & in) const { return m_version > in.m_version; }

    bool operator==(Version const & in) const { return m_version == in.m_version; }

    bool operator!=(Version const & in) const { return m_version != in.m_version; }

    std::string to_string() const
    {
        return std::to_string(std::get<0>(m_version)) + "." + std::to_string(std::get<1>(m_version))
               + "." + std::to_string(std::get<2>(m_version));
    }

    std::uint16_t major_version() const { return std::get<0>(m_version); }

    std::uint16_t minor_version() const { return std::get<1>(m_version); }

    std::uint16_t revision_version() const { return std::get<2>(m_version); }

private:
    std::tuple<std::uint16_t, std::uint16_t, std::uint16_t> m_version;
};

POD5_FORMAT_EXPORT Result<Version> parse_version_number(std::string const & ver);
POD5_FORMAT_EXPORT Version current_build_version_number();

struct SchemaMetadataDescription {
    boost::uuids::uuid file_identifier;
    std::string writing_software;
    Version writing_pod5_version;
};

POD5_FORMAT_EXPORT Result<std::shared_ptr<const arrow::KeyValueMetadata>>
make_schema_key_value_metadata(SchemaMetadataDescription const & schema_metadata);

POD5_FORMAT_EXPORT Result<SchemaMetadataDescription> read_schema_key_value_metadata(
    std::shared_ptr<const arrow::KeyValueMetadata> const & key_value_metadata);

}  // namespace pod5
