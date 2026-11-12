#pragma once

#include "core/storage/base_state.h"
#include "core/storage/snapshot_retention.h"
#include "core/storage/table_profile.h"

#include <optional>
#include <unordered_map>

namespace NGlamDB::NIO {
class TStoreDirs;
}

namespace NGlamDB::NStorage {

enum class EPointerOp : std::uint8_t { InsertTable, UpdateLocation };

struct TPointerChange
{
    EPointerOp Op;
    std::string Namespace;
    std::string Name;
    std::optional<std::string> ExpectedCurrentLocation;
    std::string NewLocation;
};

struct IPublicationGuard
{
    virtual ~IPublicationGuard() = default;
    virtual iceberg::Status BeforePointerChange(const TPointerChange& change) = 0;
};

// The root an append wrote: the in-memory staged metadata and the exact
// bytes serialized from it.
struct TStagedRoot
{
    std::shared_ptr<const iceberg::TableMetadata> Metadata;
    std::shared_ptr<const std::string> Bytes;
};

struct TPublicationContext
{
    std::shared_ptr<NIO::TStoreDirs> Directories;
    std::shared_ptr<iceberg::FileIO> FileIO;
    std::shared_ptr<NIO::TIOAttempt> Attempt;
    std::string Namespace;
    std::string Name;
    TTableProfile Profile;
    std::shared_ptr<const TBaseState> Base{};
    std::unordered_map<std::string, std::string> ExpectedSummary{};
    TStorageUsage StoreUsage{};
    TStorageLimits Limits{};
    // Pre-production warehouse measurement of this append. With it the guard
    // adds the attempt's durable files instead of walking the warehouse
    // again; without it (creation, or no admitted measurement) it walks.
    std::optional<TWarehouseBaseline> Warehouse{};
    // With a staged root the guard requires the durable root to equal its
    // bytes and validates the staged metadata instead of parsing the file.
    // That is the same metadata only because parsing those bytes yields it
    // again; VerifyStagedRoot additionally parses the file and requires
    // structural equality and byte-identical reserialization.
    std::optional<TStagedRoot> StagedRoot{};
    bool VerifyStagedRoot = false;
    std::optional<TRetentionPolicy> RetentionPolicy{};
    std::size_t ExpirationPrefix = 0;
    // Per-collection list validation remains independent of aggregate admission.
    std::optional<TStorageLimits> StoreLimits{};
    // Archived snapshots an expiration removes (oldest first) before any root
    // snapshot, and the oldest archived snapshot that remains.
    std::vector<TArchivedList> ExpiredArchive{};
    std::optional<TArchivedList> RemainingArchive{};
    // Oldest root snapshots an append or archive maintenance moves to the
    // catalog archive. Their metadata files stay; the root drops them.
    std::size_t ArchivePrefix = 0;
    // Maintenance that only moves snapshots to the archive: every other root
    // field, including the retention properties, stays unchanged.
    bool ArchiveMaintenance = false;
};

class TStoragePublicationGuard final : public IPublicationGuard
{
public:
    explicit TStoragePublicationGuard(TPublicationContext context);
    iceberg::Status BeforePointerChange(const TPointerChange& change) override;
    // Install this tentative state only after the caller confirms its outer COMMIT.
    std::shared_ptr<const TBaseState> GetCandidate() const;
    // The verified description of ArchivePrefix that the same outer
    // transaction must record in the catalog archive.
    std::shared_ptr<const TArchiveCut> GetArchiveCut() const;

private:
    const TPublicationContext Context_;
    std::shared_ptr<const TBaseState> Candidate_;
    std::shared_ptr<const TArchiveCut> ArchiveCut_;
    bool Invoked_ = false;

    iceberg::Status CheckContext(const TPointerChange& change) const;
    iceberg::Status CheckCandidate(const TPointerChange& change);
};

}
