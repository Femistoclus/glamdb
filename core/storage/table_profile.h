#pragma once

#include "storage/schema.h"

#include <iceberg/result.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace iceberg {
class Schema;
struct TableMetadata;
}

namespace NGlamDB::NStorage {

struct TTableProfile
{
    std::shared_ptr<iceberg::Schema> Schema;
    std::unordered_map<std::string, std::string> Properties;
};

iceberg::Result<TTableProfile> MakeTableProfile(const TSchemaSpec& spec);
iceberg::Result<TTableProfile> MakeKVTableProfile(const TSchemaSpec& spec);
bool IsKVMetadata(const iceberg::TableMetadata& metadata);

// Fixed append and KV profiles are supported. Retention extensions are append-only.
iceberg::Status CheckTableProfile(const TTableProfile& profile);

}
