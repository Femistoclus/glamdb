#pragma once

#include "storage/scan.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iceberg {
class FileIO;
} // namespace iceberg

namespace glamdb::detail {

struct TScanFile
{
    std::string Path;
    int64_t SizeBytes = 0;
    int64_t RecordCount = 0;
};

struct TScanPlan
{
    // nullopt is the initial empty view.
    std::optional<int64_t> SnapshotID;
    std::vector<TScanFile> Files;
};

// Resolves the selector once against the committed root of `base` and lists the
// live data files of that snapshot from its manifests. Warehouse directories
// are never listed, so orphan and unpublished files are not visible.
Result<TScanPlan>
PlanScan(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, const SnapshotSelector& selector);

} // namespace glamdb::detail
