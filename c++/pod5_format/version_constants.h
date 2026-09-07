#pragma once

#include "pod5_format/schema_metadata.h"

namespace pod5 {

inline Version const kPod5VersionReadTableV1{0, 0, 24};
inline Version const kPod5VersionReadTableV2{0, 0, 32};

// The v2 -> v3 migration writes 0.0.35 into migrated table metadata for
// historical compatibility. However, the flattened v3 physical layout was not
// safely identifiable from release version until 0.0.38: 0.0.37 files are known
// to require v2 -> v3 migration.
inline Version const kPod5VersionReadTableV3Written{0, 0, 35};
inline Version const kPod5VersionReadTableV3MigrationThreshold{0, 0, 38};

inline Version const kPod5VersionReadTableV4{0, 3, 30};
inline Version const kPod5VersionReadTableV5{0, 3, 44};
inline Version const kPod5VersionReadTableV6{0, 3, 46};
inline Version const kPod5VersionReadTableV7{0, 3, 48};

inline Version const kPod5VersionReadTableLatest = kPod5VersionReadTableV7;

}  // namespace pod5
