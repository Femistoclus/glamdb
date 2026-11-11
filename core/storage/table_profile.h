#pragma once

#include "storage/schema.h"

#include <iceberg/result.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace iceberg {
class Schema;
}

namespace NGlamDB::NStorage {

// Manifest string bounds of non-key fields keep at most this many code points
// (`write.metadata.metrics.default`); the key field keeps full bounds.
inline constexpr std::int32_t MetricsTruncateLength = 16;

struct TTableProfile
{
    std::shared_ptr<iceberg::Schema> Schema;
    std::unordered_map<std::string, std::string> Properties;
};

iceberg::Result<TTableProfile> MakeTableProfile(const TSchemaSpec& spec);

// Only profiles produced by MakeTableProfile are supported; schema or property extensions are rejected.
iceberg::Status CheckTableProfile(const TTableProfile& profile);

}
