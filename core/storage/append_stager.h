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

struct TBaseState;
struct TStorageLimits;

struct TStagedAppend
{
    std::shared_ptr<iceberg::Transaction> Transaction;
    std::int64_t SnapshotID;
    std::optional<std::int64_t> ParentID;
    std::string ManifestList;
    std::vector<std::string> NewManifestPaths;
};

// The encoded byte target for Iceberg's manifest merge in the next append to
// `base` (adding `newFiles` data files), such that every merged manifest stays
// within the verifier's decoded cap (`limits.ManifestBytes`) by the measured
// decoded/encoded ratios of the current manifests. Merging then produces
// several bounded manifests and the manifest list grows instead. Empty keeps
// Iceberg's default target (it is already small enough, or the current
// manifests were not measured).
std::optional<std::int64_t>
ManifestMergeTarget(const TBaseState& base, std::uint64_t newFiles, const TStorageLimits& limits);

// `mergeTarget` (from ManifestMergeTarget) applies to this append only; the
// staged root's properties are unchanged.
iceberg::Result<TStagedAppend> StageAppend(std::shared_ptr<iceberg::Table> table,
    const std::shared_ptr<iceberg::DataFile>& dataFile,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    std::optional<std::int64_t> mergeTarget = std::nullopt);

iceberg::Result<TStagedAppend> StageBulkAppend(std::shared_ptr<iceberg::Table> table,
    const std::vector<std::shared_ptr<iceberg::DataFile>>& dataFiles,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    std::optional<std::int64_t> mergeTarget = std::nullopt);

iceberg::Result<TStagedAppend> StageKVOverwrite(std::shared_ptr<iceberg::Table> table,
    const std::vector<std::shared_ptr<iceberg::DataFile>>& addedFiles,
    const std::vector<std::shared_ptr<iceberg::DataFile>>& removedFiles,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    bool compact = false);

}
