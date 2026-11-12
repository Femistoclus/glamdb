#pragma once

#include "core/catalog/durable_sqlite_catalog.h"
#include "core/storage/snapshot_archive.h"

#include <memory>
#include <string>

namespace iceberg {
struct Snapshot;
}

namespace glamdb::detail {

// Lossless, canonical encoding of every field of an Iceberg snapshot entry,
// with the summary in key order. Decoding the blob and serializing it with
// the pinned Iceberg writer reproduces the entry the root held.
std::string EncodeArchivedSnapshot(const iceberg::Snapshot& snapshot);

// Decodes a catalog archive row and requires its columns to describe exactly
// the encoded snapshot. Malformed or inconsistent rows are CORRUPTION.
std::shared_ptr<iceberg::Snapshot> DecodeArchivedSnapshot(const ArchivedSnapshotRecord& record);

ArchivedSnapshotRecord MakeArchivedRecord(const NGlamDB::NStorage::TArchivedSnapshot& archived);

NGlamDB::NStorage::TArchivedList ArchivedListIdentity(const ArchivedSnapshotRecord& record);

} // namespace glamdb::detail
