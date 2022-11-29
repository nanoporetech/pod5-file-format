#include "pod5_format/schema_metadata.h"

#include "pod5_format/version.h"

#include <arrow/util/key_value_metadata.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace pod5 {

Result<Version> parse_version_number(std::string const & ver)
{
    std::uint16_t components[3];
    std::size_t component_index = 0;
    std::size_t last_char_index = 0;
    std::size_t char_index = 0;

    auto parse_component = [&](std::size_t last_char_index, std::size_t char_index) {
        auto const component_str =
            std::string(ver.data() + last_char_index, ver.data() + char_index);

        std::size_t pos = 0;
        int val = std::stoi(component_str, &pos);

        if (pos != (char_index - last_char_index)) {
            throw std::runtime_error("Invalid remaining characters after version number");
        }

        return val;
    };

    try {
        while (char_index < ver.size()) {
            if (ver[char_index] == '.') {
                if (component_index > 3) {
                    return Status::Invalid("Invalid component count");
                }
                components[component_index] = parse_component(last_char_index, char_index);

                last_char_index = char_index + 1;
                component_index += 1;
            }
            char_index += 1;
        }

        // extract the final component
        if (component_index != 2) {
            return Status::Invalid("Invalid component count");
        }
        components[2] = parse_component(last_char_index, char_index);
    } catch (std::exception const & e) {
        return Status::Invalid(e.what());
    }

    return Version{components[0], components[1], components[2]};
}

Version current_build_version_number()
{
    return Version(Pod5MajorVersion, Pod5MinorVersion, Pod5RevVersion);
}

Result<std::shared_ptr<const arrow::KeyValueMetadata>> make_schema_key_value_metadata(
    SchemaMetadataDescription const & schema_metadata)
{
    if (schema_metadata.writing_software.empty()) {
        return Status::Invalid("Expected writing_software to be specified for metadata");
    }

    if (schema_metadata.writing_pod5_version == Version{}) {
        return Status::Invalid("Expected writing_pod5_version to be specified for metadata");
    }

    if (schema_metadata.file_identifier == boost::uuids::uuid{}) {
        return Status::Invalid("Expected file_identifier to be specified for metadata");
    }

    return arrow::KeyValueMetadata::Make(
        {"MINKNOW:file_identifier", "MINKNOW:software", "MINKNOW:pod5_version"},
        {boost::uuids::to_string(schema_metadata.file_identifier),
         schema_metadata.writing_software,
         schema_metadata.writing_pod5_version.to_string()});
}

Result<SchemaMetadataDescription> read_schema_key_value_metadata(
    std::shared_ptr<const arrow::KeyValueMetadata> const & key_value_metadata)
{
    ARROW_ASSIGN_OR_RAISE(
        auto file_identifier_str, key_value_metadata->Get("MINKNOW:file_identifier"));
    ARROW_ASSIGN_OR_RAISE(auto software_str, key_value_metadata->Get("MINKNOW:software"));
    ARROW_ASSIGN_OR_RAISE(auto pod5_version_str, key_value_metadata->Get("MINKNOW:pod5_version"));
    ARROW_ASSIGN_OR_RAISE(auto pod5_version, parse_version_number(pod5_version_str));

    boost::uuids::uuid file_identifier;
    try {
        file_identifier = boost::lexical_cast<boost::uuids::uuid>(file_identifier_str);
    } catch (boost::bad_lexical_cast const &) {
        return Status::IOError(
            "Schema file_identifier metadata not uuid form: '", file_identifier_str, "'");
    }

    return SchemaMetadataDescription{file_identifier, software_str, pod5_version};
}

}  // namespace pod5
