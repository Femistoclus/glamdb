#include "core/resources/metadata_result.h"

#include "core/scan/predicate.h"

#include <arrow/type.h>

#include <limits>

namespace glamdb::detail {
namespace {
class TCapacity
{
public:
    uint64_t Bytes = 4096;
    void Add(uint64_t count, uint64_t width = 1)
    {
        const auto max = std::numeric_limits<uint64_t>::max();
        if ((count != 0 && width > max / count) || count * width > max - Bytes) {
            Bytes = max;
        } else {
            Bytes += count * width;
        }
    }
    void Add(const std::string& text)
    {
        Allocation(text.capacity());
        Add(1);
    }
    void Allocation(uint64_t count, uint64_t width = 1)
    {
        if (count == 0 || width == 0) {
            return;
        }
        Add(count, width);
        // Account for each allocation's headers and small size classes. Large
        // allocations also leave a page for alignment/rounding in this profile.
        Add(128);
        if (count > 4095 / width) {
            Add(4096);
        }
    }
    void Add(const BaseIdentity& identity)
    {
        Add(identity.store_id);
        Add(identity.collection_id);
        Add(identity.table_uuid);
        Add(identity.metadata_location);
    }
    void Add(const std::optional<std::vector<uint8_t>>& bytes)
    {
        if (bytes) {
            Allocation(bytes->capacity());
        }
    }
    void Add(const TDataFileMetadata& file)
    {
        Add(file.FileID);
        Add(file.Format);
        Allocation(file.FieldMetrics.capacity(), sizeof(TFileFieldMetrics));
        for (const auto& field : file.FieldMetrics) {
            Add(field.LowerBound);
            Add(field.UpperBound);
        }
    }
};

struct TSchemaLease
{
    std::shared_ptr<const void> Reservation;
    std::shared_ptr<const arrow::Schema> Schema;
};

Status Attach(const std::shared_ptr<ResourceOwner>& owner, uint64_t bytes, std::shared_ptr<const void>& target)
{
    auto memory = owner->Reserve(ResourceClass::Metadata, bytes);
    if (!memory) {
        return std::unexpected(memory.error());
    }
    target = std::move(*memory);
    return {};
}
} // namespace

Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, TCollectionPage& result)
{
    TCapacity capacity;
    capacity.Add(result.NextPageToken);
    capacity.Allocation(result.Collections.capacity(), sizeof(CollectionInfo));
    for (const auto& collection : result.Collections) {
        capacity.Add(collection.collection_id);
        capacity.Add(collection.name);
        capacity.Add(collection.table_uuid);
        capacity.Allocation(collection.schema.fields.capacity(), sizeof(FieldSpec));
        for (const auto& field : collection.schema.fields) {
            capacity.Add(field.name);
        }
    }
    return Attach(owner, capacity.Bytes, result.ResourceLease);
}

Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, SnapshotHistory& result)
{
    TCapacity capacity;
    capacity.Add(result.NextPageToken);
    capacity.Add(result.identity);
    capacity.Allocation(result.snapshots.capacity(), sizeof(SnapshotMetadata));
    for (const auto& snapshot : result.snapshots) {
        const auto& receipt = snapshot.receipt;
        capacity.Add(receipt.collection_id);
        capacity.Add(receipt.operation_id);
        capacity.Add(receipt.payload_hash);
        capacity.Add(receipt.durability_profile);
    }
    return Attach(owner, capacity.Bytes, result.resource_lease);
}

Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, CollectionStatistics& result)
{
    TCapacity capacity;
    capacity.Add(result.identity);
    return Attach(owner, capacity.Bytes, result.resource_lease);
}

Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, ScanExplanation& result)
{
    TCapacity capacity;
    capacity.Add(result.info.identity);
    capacity.Add(result.candidates.identity);
    capacity.Allocation(result.read_fields.capacity(), sizeof(std::string));
    for (const auto& field : result.read_fields) {
        capacity.Add(field);
    }
    const auto predicateBytes = TPredicateResources::Measure(result.predicate);
    if (!predicateBytes) {
        return std::unexpected(predicateBytes.error());
    }
    capacity.Add(*predicateBytes);
    if (result.info.output_schema) {
        // Fixed flat schema: field/type/metadata objects plus owned names.
        capacity.Add(static_cast<uint64_t>(result.info.output_schema->num_fields()), 1024);
        for (const auto& field : result.info.output_schema->fields()) {
            capacity.Add(field->name());
        }
    }
    auto status = Attach(owner, capacity.Bytes, result.resource_lease);
    if (status) {
        result.predicate = TPredicateResources::Retain(result.predicate, result.resource_lease);
    }
    if (status && result.info.output_schema) {
        auto retained = std::make_shared<TSchemaLease>(result.resource_lease, result.info.output_schema);
        result.info.output_schema = std::shared_ptr<const arrow::Schema>(retained, retained->Schema.get());
    }
    return status;
}

Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, TFileListing& result)
{
    TCapacity capacity;
    capacity.Add(result.NextPageToken);
    capacity.Add(result.Identity);
    capacity.Allocation(result.Files.capacity(), sizeof(TDataFileMetadata));
    for (const auto& file : result.Files) {
        capacity.Add(file);
    }
    return Attach(owner, capacity.Bytes, result.ResourceLease);
}

Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, TFileDetails& result)
{
    TCapacity capacity;
    capacity.Add(result.Identity);
    capacity.Add(result.File);
    capacity.Allocation(result.RowGroups.capacity(), sizeof(TRowGroupMetadata));
    for (const auto& group : result.RowGroups) {
        capacity.Allocation(group.Columns.capacity(), sizeof(TColumnChunkMetadata));
        for (const auto& column : group.Columns) {
            capacity.Add(column.Compression);
            capacity.Add(column.LowerBound);
            capacity.Add(column.UpperBound);
        }
    }
    return Attach(owner, capacity.Bytes, result.ResourceLease);
}
} // namespace glamdb::detail
