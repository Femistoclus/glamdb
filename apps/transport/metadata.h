#pragma once

#include "apps/transport/codec.h"

#include <map>

namespace glamdb::NTransport {
// Native metadata encoders shared by HTTP and offline administration.
TJson OptionalInteger(std::optional<int64_t> value);
TJson Snapshot(SnapshotSelector selector);
TJson Value(const Metric& metric);
std::string MetricKindName(const Metric& metric);
TJson Footprint(const StorageFootprint& footprint);
TJson NativeIdentity(const BaseIdentity& identity, SnapshotSelector selector);
TJson Statistics(const CollectionStatistics& statistics);
SnapshotSelector MetadataSelector(const std::map<std::string, std::string>& query);
TJson StatisticsJson(const Engine& engine,
    const std::string& collection,
    const std::map<std::string, std::string>& query);
TJson SnapshotsJson(const Engine& engine, const std::string& collection, SnapshotSelector selector);
TJson FilesJson(const Engine& engine, const std::string& collection, SnapshotSelector selector);
TJson FileJson(const Engine& engine,
    const std::string& collection,
    const std::string& file,
    const std::map<std::string, std::string>& query);
// Snapshot archive status as decimal strings: hot_snapshots ("0" without an archive),
// root_snapshots, archived_snapshots, expired_snapshots and archive_pending;
// maintenance responses add snapshots_archived.
TJson SnapshotArchiveJson(const SnapshotArchiveStatus& status, bool maintenance);
// A create or update hot_snapshots member: a decimal string ("0" is
// KeepAllSnapshotsInRoot) or null, which equals an omitted member. On create
// null/omitted selects the engine default; on update it stops archiving, like
// "0". The engine checks the range.
std::optional<int64_t> ParseHotSnapshots(const TJson& value);
// {"hot_snapshots": "N"|null}; the member is required.
UpdateSnapshotArchiveRequest ParseSnapshotArchiveUpdate(std::string collectionID, const TJson& body);
} // namespace glamdb::NTransport
