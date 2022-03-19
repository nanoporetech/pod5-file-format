#pragma once

#include "mkr_format/mkr_format_export.h"
#include "mkr_format/result.h"

#include <memory>

namespace arrow {
class KeyValueMetadata;
class Schema;
}  // namespace arrow

namespace mkr {

struct SignalTableSchemaDescription {
    int read_id = 0;
    int signal = 1;
    int samples = 2;
};

/// \brief Make a new schema for a signal table.
/// \param metadata Metadata to be applied to the schema.
/// \param field_locations [optional] The signal table field locations, for use when writing to the table.
/// \returns The schema for a signal table.
MKR_FORMAT_EXPORT std::shared_ptr<arrow::Schema> make_signal_table_schema(
        std::shared_ptr<const arrow::KeyValueMetadata> const& metadata,
        SignalTableSchemaDescription* field_locations);

MKR_FORMAT_EXPORT Result<SignalTableSchemaDescription> read_signal_table_schema(
        std::shared_ptr<arrow::Schema> const&);

}  // namespace mkr