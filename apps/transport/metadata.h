#pragma once

#include "apps/transport/codec.h"

#include <map>

namespace glamdb::NTransport {
// Native metadata encoders shared by HTTP and offline administration.
TJson OptionalInteger(std::optional<int64_t> value);
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
} // namespace glamdb::NTransport
