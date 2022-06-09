#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"

#include <boost/uuid/uuid.hpp>

#include <memory>
#include <string>

namespace arrow {
class KeyValueMetadata;
}

namespace boost {
namespace uuids {
struct uuid;
}
}  // namespace boost

namespace pod5 {

struct SchemaMetadataDescription {
    boost::uuids::uuid file_identifier;
    std::string writing_software;
    std::string writing_pod5_version;
};

POD5_FORMAT_EXPORT Result<std::shared_ptr<const arrow::KeyValueMetadata>>
make_schema_key_value_metadata(SchemaMetadataDescription const& schema_metdata);

POD5_FORMAT_EXPORT Result<SchemaMetadataDescription> read_schema_key_value_metadata(
        std::shared_ptr<const arrow::KeyValueMetadata> const& key_value_metadata);

}  // namespace pod5