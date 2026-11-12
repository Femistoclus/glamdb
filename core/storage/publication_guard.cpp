#include "core/storage/publication_guard.h"
#include "core/io/io_attempt.h"
#include "core/io/store_dirs.h"
#include "core/io/store_uri.h"
#include "core/storage/format_verifier.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/metadata_validation.h"

#include <iceberg/file_io.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <limits>

namespace NGlamDB::NStorage {
using iceberg::Error;

TStoragePublicationGuard::TStoragePublicationGuard(TPublicationContext context)
    : Context_(std::move(context))
{ }

std::shared_ptr<const TBaseState> TStoragePublicationGuard::GetCandidate() const
{
    return Candidate_;
}

iceberg::Status TStoragePublicationGuard::CheckContext(const TPointerChange& change) const
{
    if (!Context_.Directories || !Context_.FileIO || !Context_.Attempt ||
        Context_.Attempt->Latch() != Context_.Directories->Latch() || change.Namespace != Context_.Namespace ||
        change.Name != Context_.Name ||
        Context_.Limits.RootBytes > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    {
        return iceberg::InvalidArgument("Invalid publication context.");
    }
    const auto isAppend = Context_.Attempt->Kind() != NIO::EAttemptKind::CreateCollection;
    if (isAppend != static_cast<bool>(Context_.Base) ||
        (isAppend &&
            (change.Op != EPointerOp::UpdateLocation ||
                change.ExpectedCurrentLocation != Context_.Base->MetadataLocation)) ||
        (!isAppend && (change.Op != EPointerOp::InsertTable || change.ExpectedCurrentLocation)))
    {
        return iceberg::ValidationFailed("Publication does not match the prepared base.");
    }
    const auto uri = NIO::TStoreUri::Parse(change.NewLocation, Context_.Attempt->StoreId());
    if (!uri || uri.Value().Kind() != NIO::EFileKind::Metadata ||
        uri.Value().CollectionId() != Context_.Attempt->CollectionId() ||
        (Context_.Base && (change.NewLocation == Context_.Base->MetadataLocation || !Context_.Base->Metadata)))
    {
        return iceberg::ValidationFailed("Invalid candidate metadata location.");
    }
    if (Context_.Attempt->FirstError() || Context_.Directories->Latch()->IsSet()) {
        return iceberg::IOError("Publication attempt has failed.");
    }
    if (const auto error = Context_.Directories->CheckIdentity()) {
        return iceberg::Invalid("CORRUPTION: Store root identity changed: {}.", error.message());
    }
    return {};
}

namespace {

// The pinned Iceberg snapshot equality ignores the summary and the manifest
// list location, so compare them explicitly for every snapshot.
bool SameSnapshotDetails(const iceberg::TableMetadata& left, const iceberg::TableMetadata& right)
{
    if (left.snapshots.size() != right.snapshots.size()) {
        return false;
    }
    for (std::size_t index = 0; index < left.snapshots.size(); ++index) {
        const auto& a = left.snapshots[index];
        const auto& b = right.snapshots[index];
        if (!a || !b || a->summary != b->summary || a->manifest_list != b->manifest_list) {
            return false;
        }
    }
    return true;
}

} // namespace

iceberg::Status TStoragePublicationGuard::CheckCandidate(const TPointerChange& change)
{
    const auto record = Context_.Attempt->Record(change.NewLocation);
    if (!record) {
        return iceberg::ValidationFailed("Candidate metadata has no durable receipt.");
    }
    const auto& entry = record.Value();
    const auto& receipt = entry.Receipt;
    if (!receipt || entry.Kind != NIO::EFileKind::Metadata || entry.State != NIO::EFileState::DirSynced ||
        entry.FirstError || receipt->AttemptId != Context_.Attempt->Id())
    {
        return iceberg::ValidationFailed("Candidate metadata has no durable receipt.");
    }
    if (entry.Length > Context_.Limits.RootBytes) {
        return iceberg::ValidationFailed("CAPACITY: Candidate metadata exceeds its byte limit.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto input,
        Context_.FileIO->NewInputFile(change.NewLocation, static_cast<std::int64_t>(record.Value().Length)));
    ICEBERG_ASSIGN_OR_RAISE(auto rootSize, input->Size());
    if (rootSize < 0 || static_cast<std::uint64_t>(rootSize) != record.Value().Length) {
        return iceberg::ValidationFailed("Candidate metadata length differs from its receipt.");
    }
    TBaseState candidate;
    candidate.MetadataLocation = change.NewLocation;
    const auto maxRootBytes = static_cast<std::int64_t>(Context_.Limits.RootBytes);
    if (const auto& staged = Context_.StagedRoot) {
        if (!staged->Metadata || !staged->Bytes || staged->Bytes->size() != record.Value().Length) {
            return iceberg::ValidationFailed("Candidate metadata differs from its staged root.");
        }
        ICEBERG_RETURN_UNEXPECTED(
            VerifyTableMetadataBytes(*Context_.FileIO, change.NewLocation, *staged->Bytes, maxRootBytes));
        if (Context_.VerifyStagedRoot) {
            ICEBERG_ASSIGN_OR_RAISE(auto parsed, ParseTableMetadata(*staged->Bytes));
            ICEBERG_ASSIGN_OR_RAISE(auto reserialized, SerializeTableMetadata(*parsed));
            ICEBERG_ASSIGN_OR_RAISE(auto stagedBytes, SerializeTableMetadata(*staged->Metadata));
            if (!(*parsed == *staged->Metadata) || !SameSnapshotDetails(*parsed, *staged->Metadata) ||
                reserialized != *staged->Bytes || stagedBytes != *staged->Bytes)
            {
                return iceberg::Invalid("CORRUPTION: Staged metadata does not round-trip through its root bytes.");
            }
        }
        candidate.Metadata = staged->Metadata;
    } else {
        ICEBERG_ASSIGN_OR_RAISE(auto metadata,
            ReadTableMetadata(*Context_.FileIO, change.NewLocation, std::nullopt, maxRootBytes));
        candidate.Metadata = std::shared_ptr<iceberg::TableMetadata>(std::move(metadata));
    }
    if (Context_.Base) {
        if (Context_.RetentionPolicy) {
            auto expected = *Context_.Base->Metadata;
            const auto count = Context_.ExpirationPrefix;
            if (count >= expected.snapshots.size() && count != 0) {
                return iceberg::ValidationFailed("Expiration removes the current snapshot.");
            }
            ICEBERG_ASSIGN_OR_RAISE(auto expiredSequence, ExpiredSequence(expected));
            if (count != 0) {
                expiredSequence = expected.snapshots[count - 1]->sequence_number;
            }
            expected.snapshots.erase(expected.snapshots.begin(),
                expected.snapshots.begin() + static_cast<std::ptrdiff_t>(count));
            expected.snapshot_log.erase(expected.snapshot_log.begin(),
                expected.snapshot_log.begin() + static_cast<std::ptrdiff_t>(count));
            ICEBERG_RETURN_UNEXPECTED(WriteRetentionPolicy(expected, *Context_.RetentionPolicy, expiredSequence));
            if (!(expected == *candidate.Metadata) || !SameSnapshotDetails(expected, *candidate.Metadata)) {
                return iceberg::ValidationFailed("Expiration changes immutable metadata.");
            }
        } else {
            ICEBERG_RETURN_UNEXPECTED(
                CheckAppendMetadata(*Context_.Base->Metadata, *candidate.Metadata, Context_.ExpectedSummary));
            if (std::ranges::binary_search(Context_.Base->ExpiredSnapshotIDs, candidate.Metadata->current_snapshot_id))
            {
                return iceberg::ValidationFailed("Append reuses an expired snapshot identity.");
            }
        }
        candidate.Usage = Context_.Base->Usage;
        candidate.ScanIndex = Context_.Base->ScanIndex;
        candidate.ExpiredSnapshotIDs = Context_.Base->ExpiredSnapshotIDs;
        candidate.RetainedManifestPaths = Context_.Base->RetainedManifestPaths;
        candidate.CurrentCommitMetadataBytes = Context_.Base->CurrentCommitMetadataBytes;
    } else {
        const auto location = "store://" + std::string(Context_.Attempt->StoreId()) + "/warehouse/" +
            std::string(Context_.Attempt->CollectionId());
        ICEBERG_RETURN_UNEXPECTED(CheckCreateMetadata(*candidate.Metadata, Context_.Profile, location));
        candidate.ScanIndex = std::make_shared<const TScanMembership>();
    }
    candidate.Usage.RootBytes = record.Value().Length;
    candidate.Usage.Snapshots = candidate.Metadata->snapshots.size();
    candidate.Usage.SnapshotLogEntries = candidate.Metadata->snapshot_log.size();
    if (candidate.Metadata->metadata_log.size() > Context_.Limits.MetadataLogEntries) {
        return iceberg::ValidationFailed("CAPACITY: Metadata history exceeds storage capacity.");
    }
    if (Context_.Base && !Context_.RetentionPolicy) {
        ICEBERG_ASSIGN_OR_RAISE(candidate,
            CheckAppendClosure(Context_.FileIO,
                std::move(candidate),
                Context_.Base->CurrentManifests,
                Context_.Attempt,
                Context_.Limits));
    }
    if (Context_.RetentionPolicy) {
        // Recovery charges the final root with the latest append's metadata.
        // A policy-only root can grow even when no snapshots are removed.
        if (candidate.CurrentCommitMetadataBytes > Context_.Limits.CommitBytes ||
            candidate.Usage.RootBytes > Context_.Limits.CommitBytes - candidate.CurrentCommitMetadataBytes)
        {
            return iceberg::ValidationFailed("CAPACITY: Maintenance exceeds commit verification bytes.");
        }
        candidate.CurrentManifests = Context_.Base->CurrentManifests;
        auto index = std::make_shared<TScanMembership>(*Context_.Base->ScanIndex);
        if (Context_.ExpirationPrefix != 0) {
            ICEBERG_ASSIGN_OR_RAISE(auto retirement,
                FindExpiredMetadata(Context_.FileIO,
                    *Context_.Base->Metadata,
                    Context_.ExpirationPrefix,
                    Context_.Limits));
            const auto usage = ReplaceUsage(candidate.Usage, {.Previous = retirement.Removed});
            if (!usage) {
                return iceberg::ValidationFailed("Invalid expired metadata usage.");
            }
            candidate.Usage = usage.Value();
            candidate.RetainedManifestPaths = std::move(retirement.RetainedManifests);
        }
        for (std::size_t i = 0; i < Context_.ExpirationPrefix; ++i) {
            const auto& expired = Context_.Base->Metadata->snapshots[i];
            candidate.ExpiredSnapshotIDs.push_back(expired->snapshot_id);
            std::erase_if(index->Snapshots, [&](const auto& boundary) {
                return boundary.SnapshotID == expired->snapshot_id;
            });
        }
        std::ranges::sort(candidate.ExpiredSnapshotIDs);
        candidate.ScanIndex = std::move(index);
    }
    const auto total = ReplaceUsage(Context_.StoreUsage,
        {.Previous = Context_.Base ? Context_.Base->Usage : TStorageUsage{}, .Next = candidate.Usage});
    if (!total || CheckStorageCapacity(total.Value(), Context_.StoreLimits.value_or(Context_.Limits))) {
        return iceberg::ValidationFailed("CAPACITY: Candidate exceeds store capacity.");
    }
    // Admission measured the warehouse, including orphans, before file
    // production. Recheck the physical cap with the staged files before
    // publication: from that measurement plus this attempt's durable files
    // when the backend supplied it, otherwise by walking the warehouse.
    auto warehouse = Context_.Warehouse
        ? EstimateAttemptWarehouse(*Context_.Directories, *Context_.Warehouse, *Context_.Attempt, Context_.Limits)
        : MeasureWarehouse(*Context_.Directories, Context_.Limits);
    if (!warehouse && Context_.Warehouse) {
        warehouse = MeasureWarehouse(*Context_.Directories, Context_.Limits);
    }
    if (!warehouse) {
        if (warehouse.Error() == std::errc::no_space_on_device || warehouse.Error() == std::errc::value_too_large) {
            return iceberg::ValidationFailed("CAPACITY: Candidate exceeds warehouse capacity.");
        }
        return iceberg::IOError("Cannot measure candidate warehouse: {}.", warehouse.Error().message());
    }
    if (const auto error = Context_.Attempt->MarkFormatVerified(change.NewLocation)) {
        return iceberg::IOError("Cannot verify candidate metadata: {}.", error.message());
    }
    if (Context_.Attempt->FirstError() || Context_.Directories->Latch()->IsSet()) {
        return iceberg::IOError("Publication attempt failed during verification.");
    }
    Candidate_ = std::make_shared<const TBaseState>(std::move(candidate));
    return {};
}

iceberg::Status TStoragePublicationGuard::BeforePointerChange(const TPointerChange& change)
{
    auto status = CatchIcebergExceptions([&]() -> iceberg::Status {
        if (Invoked_) {
            return iceberg::ValidationFailed("Publication guard can only be invoked once.");
        }
        Invoked_ = true;
        ICEBERG_RETURN_UNEXPECTED(CheckContext(change));
        if (const auto error = Context_.Attempt->Seal()) {
            return iceberg::IOError("Cannot seal publication attempt: {}.", error.message());
        }
        return CheckCandidate(change);
    });
    if (!status) {
        Candidate_.reset();
        if (Context_.Attempt) {
            Context_.Attempt->FailFile(change.NewLocation, std::make_error_code(std::errc::state_not_recoverable));
        }
    }
    return status;
}

}
