#include "mkr_format/schema_metadata.h"

#include <arrow/util/key_value_metadata.h>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace mkr {

Result<std::shared_ptr<const arrow::KeyValueMetadata>> make_schema_key_value_metadata(
        SchemaMetadataDescription const& schema_metadata) {
    if (schema_metadata.writing_software.empty()) {
        return Status::Invalid("Expected writing_software to be specified for metadata");
    }

    if (schema_metadata.writing_mkr_version.empty()) {
        return Status::Invalid("Expected writing_mkr_version to be specified for metadata");
    }

    if (schema_metadata.file_identifier == boost::uuids::uuid{}) {
        return Status::Invalid("Expected file_identifier to be specified for metadata");
    }

    return arrow::KeyValueMetadata::Make(
            {"MINKNOW:file_identifier", "MINKNOW:software", "MINKNOW:mkr_version"},
            {boost::uuids::to_string(schema_metadata.file_identifier),
             schema_metadata.writing_software, schema_metadata.writing_mkr_version});
}

Result<SchemaMetadataDescription> read_schema_key_value_metadata(
        std::shared_ptr<const arrow::KeyValueMetadata> const& key_value_metadata) {
    ARROW_ASSIGN_OR_RAISE(auto file_identifier_str,
                          key_value_metadata->Get("MINKNOW:file_identifier"));
    ARROW_ASSIGN_OR_RAISE(auto software_str, key_value_metadata->Get("MINKNOW:software"));
    ARROW_ASSIGN_OR_RAISE(auto mkr_version_str, key_value_metadata->Get("MINKNOW:mkr_version"));

    boost::uuids::uuid file_identifier;
    try {
        file_identifier = boost::lexical_cast<boost::uuids::uuid>(file_identifier_str);
    } catch (boost::bad_lexical_cast const&) {
        return Status::IOError("Schema file_identifier metadata not uuid form: '",
                               file_identifier_str, "'");
    }

    return SchemaMetadataDescription{file_identifier, software_str, mkr_version_str};
}

}  // namespace mkr