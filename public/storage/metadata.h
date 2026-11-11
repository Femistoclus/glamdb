#pragma once

#include "storage/scan.h"

namespace glamdb {

enum class BackendKind : uint8_t { Unavailable, Custom, LocalStorage };

// Supported operations, independent of current admission (GetHealth).
// Selecting local storage does not qualify power-loss durability.
struct EngineCapabilities
{
    BackendKind backend = BackendKind::Unavailable;
    bool append = false;
    bool scan = false;
    bool metadata = false;
    bool file_pruning = false;
    bool row_group_pruning = false;
    bool durability_qualified = false;
};

struct SnapshotMetadata
{
    int64_t snapshot_id = 0;
    std::optional<int64_t> parent_snapshot_id{};
    int64_t created_at_us = 0;
    AppendResult receipt{};
};

struct SnapshotHistory
{
    // The confirmed root from which the entire history was read.
    BaseIdentity identity{};
    // Oldest first; initial empty is a view, not a snapshot.
    std::vector<SnapshotMetadata> snapshots{};
};

struct CollectionStatistics
{
    // Root identity and selected snapshot are separate for historical views.
    BaseIdentity identity{};
    SnapshotSelector selector_resolved{};
    Metric rows{};
    Metric data_files{};
    Metric compressed_data_bytes{};
    Metric logical_bytes{};
};

struct ScanExplanation
{
    ScanInfo info{};
    std::vector<std::string> read_fields{};
    Predicate predicate{};
    std::optional<int64_t> limit{};
    int64_t batch_rows = 65536;
    bool file_pruning = false;
    bool row_group_pruning = false;
    // All live files of the selected view, before predicate/limit execution.
    CollectionStatistics candidates{};
    Metric estimated_output_rows{};
};

} // namespace glamdb
