#pragma once

#include "core/scan/scan_cursor.h"
#include "storage/metadata.h"

#include <span>

namespace glamdb::detail {

struct ArchivedHistory;

// Called under the engine's confirmed-root lease. Failures use the same
// classification as scan planning and propagate through read admission.
CollectionStatistics ReadStatistics(const TScanContext& context, const SnapshotSelector& selector);
// `archived` lists the archived snapshots below the root, read from the
// catalog after the root was pinned; a concurrent expiration may have removed
// its oldest entries, never others.
SnapshotHistory ReadHistory(const TScanContext& context,
    std::span<const AppendResult> receipts,
    const ArchivedHistory* archived = nullptr);
ScanExplanation ExplainScan(const TScanContext& context, const ScanRequest& request);

} // namespace glamdb::detail
