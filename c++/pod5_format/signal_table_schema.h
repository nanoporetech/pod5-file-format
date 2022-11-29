#pragma once

#include "pod5_format/pod5_format_export.h"
#include "pod5_format/result.h"
#include "pod5_format/signal_table_utils.h"

#include <memory>

namespace arrow {
class KeyValueMetadata;
class Schema;
}  // namespace arrow

namespace pod5 {

struct SignalTableSchemaDescription {
    SignalType signal_type;

    int read_id = 0;
    int signal = 1;
    int samples = 2;
};

/// \brief Make a new schema for a signal table.
/// \param signal_type The type of signal to use.
/// \param metadata Metadata to be applied to the schema.
/// \param field_locations [optional] The signal table field locations, for use when writing to the table.
/// \returns The schema for a signal table.
POD5_FORMAT_EXPORT std::shared_ptr<arrow::Schema> make_signal_table_schema(
    SignalType signal_type,
    std::shared_ptr<const arrow::KeyValueMetadata> const & metadata,
    SignalTableSchemaDescription * field_locations);

POD5_FORMAT_EXPORT Result<SignalTableSchemaDescription> read_signal_table_schema(
    std::shared_ptr<arrow::Schema> const &);

}  // namespace pod5
