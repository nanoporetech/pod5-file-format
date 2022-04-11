#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"

#include <boost/uuid/uuid.hpp>

#include <memory>
#include <string>

namespace arrow {
class KeyValueMetadata;
}

namespace boost {
namespace uuids {
class uuid;
}
}  // namespace boost

namespace mkr {

struct SchemaMetadataDescription {
    boost::uuids::uuid file_identifier;
    std::string writing_software;
    std::string writing_mkr_version;
};

MKR_FORMAT_EXPORT Result<std::shared_ptr<const arrow::KeyValueMetadata>>
make_schema_key_value_metadata(SchemaMetadataDescription const& schema_metdata);

MKR_FORMAT_EXPORT Result<SchemaMetadataDescription> read_schema_key_value_metadata(
        std::shared_ptr<const arrow::KeyValueMetadata> const& key_value_metadata);

}  // namespace mkr