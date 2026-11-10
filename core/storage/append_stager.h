#pragma once

#include <iceberg/result.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace iceberg {
struct DataFile;
class Table;
class Transaction;
}

namespace NGlamDB::NIO {
class TIOAttempt;
}

namespace NGlamDB::NStorage {

struct TStagedAppend
{
    std::shared_ptr<iceberg::Transaction> Transaction;
    std::int64_t SnapshotID;
    std::optional<std::int64_t> ParentID;
    std::string ManifestList;
    std::vector<std::string> NewManifestPaths;
};

iceberg::Result<TStagedAppend> StageAppend(std::shared_ptr<iceberg::Table> table,
    const std::shared_ptr<iceberg::DataFile>& dataFile,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt);

}
