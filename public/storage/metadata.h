#pragma once

#include "storage/scan.h"

namespace glamdb {

enum class BackendKind : uint8_t { Unavailable, Custom, LocalStorage };

struct ParquetCodecCapability
{
    ParquetCodec codec = ParquetCodec::Zstd;
    // Accepted level range and the level used when none is given; all absent
    // for codecs without levels.
    std::optional<int32_t> min_level{};
    std::optional<int32_t> max_level{};
    std::optional<int32_t> default_level{};
    bool operator==(const ParquetCodecCapability&) const = default;
};

struct ParquetEncodingCapability
{
    ParquetEncoding encoding = ParquetEncoding::Plain;
    std::vector<FieldType> types{};
    bool operator==(const ParquetEncodingCapability&) const = default;
};

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
    // Collection write settings: the codecs available in this build, in
    // ParquetCodec order, and the compression used when none is configured.
    bool write_settings = false;
    std::vector<ParquetCodecCapability> parquet_codecs{};
    ParquetCompression default_parquet_compression{};
    // Explicit value encodings and the field types each accepts, the types
    // that can use a dictionary, and the dictionary page size limit range.
    std::vector<ParquetEncodingCapability> parquet_encodings{};
    std::vector<FieldType> dictionary_types{};
    int64_t min_dictionary_page_size_limit = 0;
    int64_t max_dictionary_page_size_limit = 0;
    int64_t default_dictionary_page_size_limit = 0;
    bool storage_footprint = false;
};

struct TMetadataPageRequest
{
    int64_t PageSize = 100;
    std::string PageToken{};
    // Trusted adapter-supplied principal and authorization generation/scope.
    std::string AuthorizationContext{};
};

// Whole owned store, observed while the engine excludes its writer. External
// filesystem changes are not an atomic snapshot. File lengths and allocated
// blocks are separate; the shared service registry is outside this scope.
struct StorageFootprint
{
    std::string StoreID;
    uint64_t DatabaseFileBytes = 0;
    uint64_t DatabaseAllocatedBytes = 0;
    uint64_t DataFileBytes = 0;
    uint64_t IcebergMetadataFileBytes = 0;
    uint64_t CatalogFileBytes = 0;
    uint64_t OtherFileBytes = 0;
    uint64_t SharedFileBytes = 0;
    uint64_t CommittedDataFileBytes = 0;
    uint64_t ReferencedMetadataFileBytes = 0;
    uint64_t UnreferencedWarehouseFileBytes = 0;
    uint64_t Files = 0;
    uint64_t Entries = 0;
    uint64_t HardlinkPaths = 0;
};

struct TCollectionPage
{
    // Declared first so payload allocations are freed before the reservation.
    std::shared_ptr<const void> ResourceLease{};
    int64_t CreationHighWaterMark = 0;
    std::vector<CollectionInfo> Collections{};
    std::string NextPageToken{};
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
    // Declared first so payload allocations are freed before the reservation.
    std::shared_ptr<const void> resource_lease{};
    // The confirmed root from which the entire history was read.
    BaseIdentity identity{};
    // Oldest first; initial empty is a view, not a snapshot.
    std::vector<SnapshotMetadata> snapshots{};
    std::string NextPageToken{};
};

struct CollectionStatistics
{
    // Declared first so payload allocations are freed before the reservation.
    std::shared_ptr<const void> resource_lease{};
    // Root identity and selected snapshot are separate for historical views.
    BaseIdentity identity{};
    SnapshotSelector selector_resolved{};
    Metric rows{};
    Metric data_files{};
    Metric compressed_data_bytes{};
    Metric logical_bytes{};
};

struct TFileFieldMetrics
{
    int32_t FieldID = 0;
    std::optional<int64_t> CompressedBytes{};
    std::optional<int64_t> ValueCount{};
    std::optional<int64_t> NullCount{};
    std::optional<int64_t> NanCount{};
    // Iceberg single-value binary encoding; string bounds may be truncated.
    std::optional<std::vector<uint8_t>> LowerBound{};
    std::optional<std::vector<uint8_t>> UpperBound{};
    bool operator==(const TFileFieldMetrics&) const = default;
};

struct TDataFileMetadata
{
    std::string FileID{};
    std::string Format = "parquet";
    int64_t RecordCount = 0;
    int64_t CompressedBytes = 0;
    std::vector<TFileFieldMetrics> FieldMetrics{};
    bool operator==(const TDataFileMetadata&) const = default;
};

struct TFileListing
{
    // Declared first so payload allocations are freed before the reservation.
    std::shared_ptr<const void> ResourceLease{};
    BaseIdentity Identity{};
    SnapshotSelector SelectorResolved{};
    std::vector<TDataFileMetadata> Files{};
    std::string NextPageToken{};
};

struct TPageEncodingCount
{
    // data_page, data_page_v2 or dictionary_page.
    std::string PageType{};
    std::string Encoding{};
    int32_t Count = 0;
    bool operator==(const TPageEncodingCount&) const = default;
};

struct TColumnChunkMetadata
{
    int32_t FieldID = 0;
    int64_t ValueCount = 0;
    int64_t CompressedBytes = 0;
    int64_t UncompressedBytes = 0;
    int64_t DataPageOffset = 0;
    std::optional<int64_t> DictionaryPageOffset{};
    std::string Compression{};
    // Every encoding the chunk uses, including level and dictionary page
    // encodings (plain, rle, rle_dictionary, ...), as the footer records them.
    std::vector<std::string> Encodings{};
    // Pages per page type and encoding; a dictionary that overflowed shows
    // both rle_dictionary and plain data pages. Empty when not recorded.
    std::vector<TPageEncodingCount> EncodingStats{};
    std::optional<int64_t> NullCount{};
    // Parquet plain encoding, independent of the manifest's Iceberg bounds.
    std::optional<std::vector<uint8_t>> LowerBound{};
    std::optional<std::vector<uint8_t>> UpperBound{};
    bool HasColumnIndex = false;
    bool HasOffsetIndex = false;
};

struct TRowGroupMetadata
{
    int32_t Ordinal = 0;
    int64_t RecordCount = 0;
    int64_t CompressedBytes = 0;
    int64_t UncompressedBytes = 0;
    std::vector<TColumnChunkMetadata> Columns{};
};

struct TFileDetails
{
    // Declared first so payload allocations are freed before the reservation.
    std::shared_ptr<const void> ResourceLease{};
    BaseIdentity Identity{};
    SnapshotSelector SelectorResolved{};
    TDataFileMetadata File{};
    std::vector<TRowGroupMetadata> RowGroups{};
    int64_t FooterBytes = 0;
};

struct ScanExplanation
{
    // Declared first so payload allocations are freed before the reservation.
    std::shared_ptr<const void> resource_lease{};
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
