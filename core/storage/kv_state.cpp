#include "core/storage/kv_state.h"

#include "core/io/io_attempt.h"
#include "core/io/store_uri.h"
#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/metadata_validation.h"
#include "core/storage/recovery_visited.h"
#include "core/storage/snapshot_retention.h"
#include "core/storage/table_profile.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/util/utf8.h>
#include <iceberg/constants.h>
#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <limits>
#include <map>
#include <set>
#include <span>
#include <unordered_set>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

iceberg::Status CheckKVPath(const std::string& path, const NIO::TStoreUri& table, NIO::EFileKind kind)
{
    const auto parsed = NIO::TStoreUri::Parse(path, table.StoreId());
    if (!parsed || parsed.Value().CollectionId() != table.CollectionId() || parsed.Value().Kind() != kind) {
        return iceberg::ValidationFailed("Invalid KV reachable file path.");
    }
    return {};
}

iceberg::Status KVReceipt(const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const std::string& path,
    NIO::EFileKind kind,
    std::int64_t length,
    bool format = false)
{
    if (length <= 0) {
        return iceberg::ValidationFailed("Invalid KV reachable file length.");
    }
    if (!attempt) {
        return {};
    }
    const auto record = attempt->Record(path);
    if (!record) {
        return iceberg::ValidationFailed("KV reachable file lacks its verified durable receipt.");
    }
    const auto& verified = record.Value();
    if (verified.Kind != kind || verified.State != NIO::EFileState::DirSynced || verified.FirstError ||
        !verified.Receipt || verified.Receipt->AttemptId != attempt->Id() ||
        verified.Receipt->Length != static_cast<std::uint64_t>(length) || (format && !verified.FormatVerified))
    {
        return iceberg::ValidationFailed("KV reachable file lacks its verified durable receipt.");
    }
    return {};
}

iceberg::Status KVMark(const std::shared_ptr<NIO::TIOAttempt>& attempt, const std::string& path)
{
    if (attempt) {
        if (const auto error = attempt->MarkFormatVerified(path)) {
            return iceberg::IOError("Cannot verify KV file: {}.", error.message());
        }
    }
    return {};
}

iceberg::Result<std::uint64_t> KVAvro(const std::shared_ptr<iceberg::FileIO>& io,
    const std::string& path,
    std::int64_t expectedLength,
    NIO::EFileKind kind,
    std::uint64_t maxBytes,
    std::uint64_t maxRecords,
    const std::shared_ptr<NIO::TIOAttempt>& attempt)
{
    if (maxBytes > INT64_MAX || maxRecords > INT64_MAX) {
        return iceberg::InvalidArgument("Invalid KV verification limits.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto input, io->NewInputFile(path));
    ICEBERG_ASSIGN_OR_RAISE(auto length, input->Size());
    if (length <= 0 || (expectedLength >= 0 && length != expectedLength)) {
        return iceberg::ValidationFailed("KV Avro length differs from its descriptor.");
    }
    ICEBERG_RETURN_UNEXPECTED(KVReceipt(attempt, path, kind, length));
    ICEBERG_RETURN_UNEXPECTED(CheckAvroContainer(*input,
        length,
        {static_cast<std::int64_t>(maxBytes),
            static_cast<std::int64_t>(maxRecords),
            static_cast<std::int64_t>(maxBytes)}));
    return static_cast<std::uint64_t>(length);
}

iceberg::Result<std::int32_t> KeyID(const iceberg::Schema& schema)
{
    for (const auto& field : schema.fields()) {
        if (field.name() == "key" && !field.optional() && field.type()->type_id() == iceberg::TypeId::kString) {
            return field.field_id();
        }
    }
    return iceberg::ValidationFailed("KV schema has no required string key.");
}

iceberg::Result<std::pair<std::string, std::string>> KeyBounds(const iceberg::DataFile& file, std::int32_t keyID)
{
    const auto lower = file.lower_bounds.find(keyID);
    const auto upper = file.upper_bounds.find(keyID);
    const auto values = file.value_counts.find(keyID);
    const auto nulls = file.null_value_counts.find(keyID);
    if (lower == file.lower_bounds.end() || upper == file.upper_bounds.end() || lower->second.size() > 1024 ||
        upper->second.size() > 1024 || values == file.value_counts.end() || values->second != file.record_count ||
        nulls == file.null_value_counts.end() || nulls->second != 0)
    {
        return iceberg::ValidationFailed("KV files require full key bounds and exact key counts.");
    }
    std::string first(lower->second.begin(), lower->second.end());
    std::string last(upper->second.begin(), upper->second.end());
    if (first > last || !arrow::util::ValidateUTF8(first) || !arrow::util::ValidateUTF8(last)) {
        return iceberg::ValidationFailed("Invalid KV key bounds.");
    }
    return std::pair(std::move(first), std::move(last));
}

template <typename TValue>
bool KnownKVMetricFields(const std::map<std::int32_t, TValue>& values, const iceberg::Schema& schema)
{
    return std::ranges::all_of(values, [&](const auto& entry) {
        return std::ranges::any_of(schema.fields(), [&](const auto& field) {
            return field.field_id() == entry.first;
        });
    });
}

iceberg::Result<std::uint64_t> VerifyKVData(const std::shared_ptr<iceberg::FileIO>& io,
    const iceberg::DataFile& file,
    const iceberg::Schema& schema,
    const NIO::TStoreUri& table,
    const std::shared_ptr<NIO::TIOAttempt>& attempt)
{
    if (file.content != iceberg::DataFile::Content::kData || file.file_format != iceberg::FileFormatType::kParquet ||
        file.record_count <= 0 || file.record_count > 100000 || file.file_size_in_bytes <= 0 ||
        file.file_size_in_bytes > (32LL << 20) || file.partition.num_fields() != 0 || !file.key_metadata.empty() ||
        !file.equality_ids.empty() || file.first_row_id || file.referenced_data_file || file.content_offset ||
        file.content_size_in_bytes || (file.sort_order_id && *file.sort_order_id != 0))
    {
        return iceberg::ValidationFailed("Unsupported KV data file.");
    }
    if (!KnownKVMetricFields(file.column_sizes, schema) || !KnownKVMetricFields(file.value_counts, schema) ||
        !KnownKVMetricFields(file.null_value_counts, schema) || !KnownKVMetricFields(file.nan_value_counts, schema) ||
        !KnownKVMetricFields(file.lower_bounds, schema) || !KnownKVMetricFields(file.upper_bounds, schema))
    {
        return iceberg::ValidationFailed("KV manifest metrics contain an unknown field ID.");
    }
    ICEBERG_RETURN_UNEXPECTED(CheckKVPath(file.file_path, table, NIO::EFileKind::Data));
    ICEBERG_RETURN_UNEXPECTED(KVReceipt(attempt, file.file_path, NIO::EFileKind::Data, file.file_size_in_bytes, true));
    ICEBERG_ASSIGN_OR_RAISE(auto input, io->NewInputFile(file.file_path, file.file_size_in_bytes));
    ICEBERG_ASSIGN_OR_RAISE(auto footer,
        ReadParquetFooter(*input, schema, {file.file_size_in_bytes, file.record_count}));
    const auto fields = schema.fields();
    std::vector<std::int64_t> columnCompressed(fields.size(), 0);
    for (int groupIndex = 0; groupIndex < footer->num_row_groups(); ++groupIndex) {
        const auto group = footer->RowGroup(groupIndex);
        for (std::size_t column = 0; column < fields.size(); ++column) {
            const auto chunkBytes = group->ColumnChunk(static_cast<int>(column))->total_compressed_size();
            if (chunkBytes < 0 || columnCompressed[column] > file.file_size_in_bytes - chunkBytes) {
                return iceberg::ValidationFailed("KV Parquet column sizes exceed the file size.");
            }
            columnCompressed[column] += chunkBytes;
        }
    }
    for (std::size_t column = 0; column < fields.size(); ++column) {
        if (const auto it = file.column_sizes.find(fields[column].field_id());
            it != file.column_sizes.end() && it->second != columnCompressed[column])
        {
            return iceberg::ValidationFailed("KV manifest column sizes differ from the Parquet footer.");
        }
    }
    ICEBERG_ASSIGN_OR_RAISE(auto keyID, KeyID(schema));
    ICEBERG_ASSIGN_OR_RAISE(auto bounds, KeyBounds(file, keyID));
    // One hard-bounded physical file, and one key page at a time. This read is
    // charged by the originating FileIO attempt during publication.
    ICEBERG_ASSIGN_OR_RAISE(auto bytes,
        io->ReadFile(file.file_path, static_cast<std::size_t>(file.file_size_in_bytes)));
    auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes)));
    arrow::ProxyMemoryPool allocations(arrow::default_memory_pool());
    arrow::CappedMemoryPool decoder(&allocations, 64LL << 20);
    parquet::ReaderProperties properties(&decoder);
    properties.set_thrift_string_size_limit(32 << 20);
    properties.set_thrift_container_size_limit(16384);
    auto parquet = parquet::ParquetFileReader::Open(source, properties);
    const auto keyField = std::ranges::find(schema.fields(), std::string_view("key"), &iceberg::SchemaField::name);
    const auto keyColumn = static_cast<int>(keyField - schema.fields().begin());
    std::string previous;
    std::string first;
    std::int64_t rows = 0;
    for (int group = 0; group < parquet->metadata()->num_row_groups(); ++group) {
        auto column = parquet->RowGroup(group)->Column(keyColumn);
        auto* reader = dynamic_cast<parquet::ByteArrayReader*>(column.get());
        if (!reader) {
            return iceberg::ValidationFailed("KV key is not a Parquet byte array.");
        }
        std::array<parquet::ByteArray, 256> values;
        while (reader->HasNext()) {
            std::int64_t count = 0;
            const auto levels =
                reader->ReadBatch(static_cast<std::int64_t>(values.size()), nullptr, nullptr, values.data(), &count);
            if (levels != count || count <= 0 || count > file.record_count - rows) {
                return iceberg::ValidationFailed("KV key count differs from its file.");
            }
            for (std::int64_t row = 0; row < count; ++row) {
                const auto& value = values[static_cast<std::size_t>(row)];
                if (value.len > 1024) {
                    return iceberg::ValidationFailed("KV key exceeds its byte limit.");
                }
                const std::string_view key(reinterpret_cast<const char*>(value.ptr), value.len);
                if (!arrow::util::ValidateUTF8(key) || (rows > 0 && previous >= key)) {
                    return iceberg::ValidationFailed("KV physical keys are not sorted and unique.");
                }
                if (rows == 0) {
                    first = key;
                }
                previous = key;
                ++rows;
            }
        }
    }
    if (rows != file.record_count || first != bounds.first || previous != bounds.second) {
        return iceberg::ValidationFailed("KV full key bounds differ from the stored keys.");
    }
    return static_cast<std::uint64_t>(footer->size());
}

bool SummaryCount(const iceberg::Snapshot& snapshot, const std::string& key, std::uint64_t count)
{
    const auto found = snapshot.summary.find(key);
    return found == snapshot.summary.end() ? count == 0 : found->second == std::to_string(count);
}

iceberg::Result<std::uint64_t> KVSummaryRows(const iceberg::Snapshot& snapshot, const std::string& key)
{
    const auto field = snapshot.summary.find(key);
    if (field == snapshot.summary.end()) {
        return iceberg::ValidationFailed("KV snapshot omits its logical row counter.");
    }
    std::uint64_t rows = 0;
    const auto& text = field->second;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), rows);
    if (error != std::errc{} || end != text.data() + text.size() || rows > 256ULL * 100000 ||
        text != std::to_string(rows))
    {
        return iceberg::ValidationFailed("Invalid KV logical row counter.");
    }
    return rows;
}

iceberg::Result<NIO::TSha256Digest>
KVRowsDigest(const std::shared_ptr<iceberg::FileIO>& io, std::vector<const TScanDataFile*> files, std::int32_t keyID)
{
    std::ranges::sort(files, [&](const auto* left, const auto* right) {
        return left->DataFile->lower_bounds.at(keyID) < right->DataFile->lower_bounds.at(keyID);
    });
    NIO::TSha256 digest;
    const auto add = [&](std::span<const std::byte> bytes) -> iceberg::Status {
        if (const auto error = digest.Update(bytes)) {
            return iceberg::ValidationFailed("Compaction row digest exceeds its byte limit: {}.", error.message());
        }
        return {};
    };
    for (const auto* file : files) {
        ICEBERG_ASSIGN_OR_RAISE(auto bytes, io->ReadFile(file->Path, static_cast<std::size_t>(file->SizeBytes)));
        auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes)));
        // This pool is local to one file and remains alive until all decoded
        // batches and reader buffers have been released.
        arrow::ProxyMemoryPool allocations(arrow::default_memory_pool());
        arrow::CappedMemoryPool decoder(&allocations, 64LL << 20);
        parquet::ReaderProperties properties(&decoder);
        properties.set_thrift_string_size_limit(32 << 20);
        properties.set_thrift_container_size_limit(16384);
        parquet::arrow::FileReaderBuilder builder;
        const auto opened = builder.Open(source, properties);
        if (!opened.ok()) {
            return iceberg::IOError("Cannot decode compaction rows: {}.", opened.ToString());
        }
        builder.memory_pool(&decoder);
        auto built = builder.Build();
        if (!built.ok()) {
            return iceberg::IOError("Cannot construct compaction decoder: {}.", built.status().ToString());
        }
        auto reader = std::move(*built);
        reader->set_use_threads(false);
        reader->set_batch_size(128);
        auto batches = reader->GetRecordBatchReader();
        if (!batches.ok()) {
            return iceberg::IOError("Cannot stream compaction rows: {}.", batches.status().ToString());
        }
        std::int64_t rowCount = 0;
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            const auto status = (*batches)->ReadNext(&batch);
            if (!status.ok()) {
                return iceberg::IOError("Cannot read compaction rows: {}.", status.ToString());
            }
            if (!batch) {
                break;
            }
            if (batch->num_rows() < 0 || batch->num_rows() > file->RecordCount - rowCount) {
                return iceberg::ValidationFailed("Compaction row count differs from its descriptor.");
            }
            rowCount += batch->num_rows();
            for (std::int64_t row = 0; row < batch->num_rows(); ++row) {
                for (const auto& column : batch->columns()) {
                    const std::byte present{static_cast<unsigned char>(column->IsValid(row))};
                    ICEBERG_RETURN_UNEXPECTED(add({&present, 1}));
                    if (!column->IsValid(row)) {
                        continue;
                    }
                    if (column->type_id() == arrow::Type::STRING) {
                        const auto text = static_cast<const arrow::StringArray&>(*column).GetView(row);
                        if (text.size() > (1U << 20)) {
                            return iceberg::ValidationFailed("Compaction string exceeds its limit.");
                        }
                        std::array<std::byte, 8> length{};
                        for (std::size_t index = 0; index < length.size(); ++index) {
                            length[index] = std::byte((text.size() >> (index * 8)) & 255U);
                        }
                        ICEBERG_RETURN_UNEXPECTED(add(length));
                        ICEBERG_RETURN_UNEXPECTED(add(std::as_bytes(std::span(text.data(), text.size()))));
                    } else if (column->type_id() == arrow::Type::BOOL) {
                        const std::byte value{
                            static_cast<unsigned char>(static_cast<const arrow::BooleanArray&>(*column).Value(row))};
                        ICEBERG_RETURN_UNEXPECTED(add({&value, 1}));
                    } else {
                        const auto* type = dynamic_cast<const arrow::FixedWidthType*>(column->type().get());
                        if (!type || type->bit_width() <= 0 || type->bit_width() > 128 || type->bit_width() % 8 != 0 ||
                            column->data()->buffers.size() < 2 || !column->data()->buffers[1])
                        {
                            return iceberg::ValidationFailed("Unsupported compaction scalar.");
                        }
                        const auto width = static_cast<std::size_t>(type->bit_width() / 8);
                        const auto offset = static_cast<std::size_t>(column->offset() + row) * width;
                        const auto& buffer = column->data()->buffers[1];
                        if (offset > static_cast<std::size_t>(buffer->size()) ||
                            width > static_cast<std::size_t>(buffer->size()) - offset)
                        {
                            return iceberg::ValidationFailed("Invalid compaction scalar buffer.");
                        }
                        ICEBERG_RETURN_UNEXPECTED(add(std::as_bytes(std::span(buffer->data() + offset, width))));
                    }
                }
            }
        }
        if (rowCount != file->RecordCount) {
            return iceberg::ValidationFailed("Incomplete compaction row stream.");
        }
    }
    return digest.Finalize();
}

struct TKVCounts
{
    std::uint64_t AddedFiles = 0;
    std::uint64_t AddedRows = 0;
    std::uint64_t AddedBytes = 0;
    std::uint64_t RemovedFiles = 0;
    std::uint64_t RemovedRows = 0;
    std::uint64_t RemovedBytes = 0;
};

// Recovery owns one mutable proof until its complete closure succeeds. Published
// generations still copy their immutable membership before changing intervals.
struct TKVReplay
{
    std::shared_ptr<TScanMembership> Index;
    std::map<std::string, std::size_t, std::less<>> Known;
    std::map<std::int64_t, std::int64_t> SnapshotBySequence;
    std::set<std::string> Live;
    std::unique_ptr<TRecoveryVisited> Visited;
    std::vector<std::string> RetainedManifestPaths;
};

iceberg::Result<TBaseState> CheckKVTransition(const std::shared_ptr<iceberg::FileIO>& io,
    TBaseState candidate,
    const TBaseState& base,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits,
    bool commitRoot,
    TKVReplay* replay)
{
    if (!io || !candidate.Metadata || !base.Metadata || !base.ScanIndex || !base.ScanIndex->KeyValue ||
        !IsKVMetadata(*candidate.Metadata) || !IsKVMetadata(*base.Metadata) || !base.ExpiredSnapshotIDs.empty() ||
        !candidate.ExpiredSnapshotIDs.empty())
    {
        return iceberg::ValidationFailed("Missing verified KV closure context.");
    }
    ICEBERG_RETURN_UNEXPECTED(ExpiredSequence(*base.Metadata));
    ICEBERG_RETURN_UNEXPECTED(ExpiredSequence(*candidate.Metadata));
    RegisterStorageFormats();
    arrow::util::InitializeUTF8();
    const auto& metadata = *candidate.Metadata;
    const auto table = NIO::TStoreUri::ParseTable(metadata.location,
        attempt ? attempt->StoreId() : std::string_view(metadata.location).substr(8, 36));
    if (!table) {
        return iceberg::ValidationFailed("Invalid KV table location.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, metadata.Snapshot());
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpec());
    ICEBERG_ASSIGN_OR_RAISE(auto keyID, KeyID(*schema));
    ICEBERG_RETURN_UNEXPECTED(CheckKVPath(snapshot->manifest_list, table.Value(), NIO::EFileKind::ManifestList));
    ICEBERG_ASSIGN_OR_RAISE(auto listBytes,
        KVAvro(io,
            snapshot->manifest_list,
            -1,
            NIO::EFileKind::ManifestList,
            limits.ManifestListBytes,
            limits.Store.Manifests,
            attempt));
    ICEBERG_ASSIGN_OR_RAISE(auto list, iceberg::ManifestListReader::Make(snapshot->manifest_list, io));
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, list->Files());
    if (manifests.size() > limits.Store.Manifests) {
        return iceberg::ValidationFailed("CAPACITY: KV manifest list exceeds its limit.");
    }
    TKVReplay local;
    auto& proof = replay ? *replay : local;
    if (!replay) {
        proof.Index = std::make_shared<TScanMembership>(*base.ScanIndex);
        for (const auto& boundary : proof.Index->Snapshots) {
            if (boundary.SequenceNumber <= 0 ||
                !proof.SnapshotBySequence.emplace(boundary.SequenceNumber, boundary.SnapshotID).second)
            {
                return iceberg::ValidationFailed("Invalid KV snapshot membership sequence.");
            }
        }
        for (std::size_t offset = 0; offset < proof.Index->Files.size(); ++offset) {
            const auto& file = proof.Index->Files[offset];
            if (!proof.Known.emplace(file.Path, offset).second) {
                return iceberg::ValidationFailed("Duplicate retained KV file.");
            }
            if (!file.RemovedSequence) {
                proof.Live.insert(file.Path);
            }
        }
    }
    if (!proof.Index || !proof.SnapshotBySequence.emplace(snapshot->sequence_number, snapshot->snapshot_id).second) {
        return iceberg::ValidationFailed("Duplicate KV snapshot sequence.");
    }
    auto& index = *proof.Index;
    auto live = std::move(proof.Live);
    const auto kind = snapshot->summary.find("glamdb.kv-kind");
    const bool compact = kind != snapshot->summary.end() && kind->second == "compact";
    const auto baseLive = compact ? live : std::set<std::string>{};
    const auto firstAdded = index.Files.size();
    std::map<std::string, const iceberg::ManifestFile*, std::less<>> oldManifests;
    for (const auto& manifest : base.CurrentManifests) {
        if (!oldManifests.emplace(manifest.manifest_path, &manifest).second) {
            return iceberg::ValidationFailed("Duplicate retained KV manifest.");
        }
    }
    std::set<std::string> manifestPaths;
    std::vector<const iceberg::ManifestFile*> addedManifests;
    std::vector<const iceberg::ManifestFile*> removedManifests;
    TStorageUsage delta{.ManifestListBytes = listBytes};
    auto workBytes = commitRoot ? candidate.Usage.RootBytes : 0;
    const auto addWork = [&](std::uint64_t bytes) {
        if (bytes > limits.CommitBytes || workBytes > limits.CommitBytes - bytes) {
            return false;
        }
        workBytes += bytes;
        return true;
    };
    if (!addWork(listBytes)) {
        return iceberg::ValidationFailed("CAPACITY: KV commit metadata exceeds its limit.");
    }
    for (const auto& manifest : manifests) {
        if (!manifestPaths.insert(manifest.manifest_path).second ||
            manifest.content != iceberg::ManifestContent::kData ||
            manifest.partition_spec_id != metadata.default_spec_id || !manifest.partitions.empty() ||
            !manifest.key_metadata.empty() || manifest.first_row_id || manifest.manifest_length <= 0 ||
            static_cast<std::uint64_t>(manifest.manifest_length) > limits.ManifestBytes)
        {
            return iceberg::ValidationFailed("Invalid KV manifest descriptor.");
        }
        ICEBERG_RETURN_UNEXPECTED(CheckKVPath(manifest.manifest_path, table.Value(), NIO::EFileKind::Manifest));
        const auto old = oldManifests.find(manifest.manifest_path);
        if (old != oldManifests.end()) {
            if (!EqualManifest(manifest, *old->second)) {
                return iceberg::ValidationFailed("KV changes a retained manifest descriptor.");
            }
            // The prior closure proved every entry of this exact immutable
            // descriptor. Its unchanged live membership is already in `live`.
            continue;
        }
        if (manifest.added_snapshot_id != snapshot->snapshot_id ||
            manifest.sequence_number != snapshot->sequence_number)
        {
            return iceberg::ValidationFailed("KV reuses a historical manifest.");
        }
        if (!replay && std::ranges::binary_search(base.RetainedManifestPaths, manifest.manifest_path)) {
            return iceberg::ValidationFailed("KV reuses a historical manifest.");
        }
        if (proof.Visited) {
            ICEBERG_ASSIGN_OR_RAISE(auto inserted,
                proof.Visited->Insert(NIO::EFileKind::Manifest, manifest.manifest_path));
            if (!inserted) {
                return iceberg::ValidationFailed("KV reuses a historical manifest.");
            }
        }
        const auto bytes = static_cast<std::uint64_t>(manifest.manifest_length);
        if (addedManifests.size() >= limits.NewManifests || !addWork(bytes)) {
            return iceberg::ValidationFailed("CAPACITY: KV new manifests exceed their limit.");
        }
        addedManifests.push_back(&manifest);
        ++delta.Manifests;
        delta.ManifestBytes += bytes;
    }
    for (const auto& [path, manifest] : oldManifests) {
        if (!manifestPaths.contains(path)) {
            if (manifest->manifest_length <= 0 || !addWork(static_cast<std::uint64_t>(manifest->manifest_length))) {
                return iceberg::ValidationFailed("CAPACITY: KV rewritten inputs exceed the metadata limit.");
            }
            removedManifests.push_back(manifest);
        }
    }
    std::set<std::string> rewrittenLive;
    std::set<std::string> deleted;
    TKVCounts counts;
    // Compare every inherited descriptor and both inherited sequence numbers.
    // A tombstone's snapshot is the removal snapshot, while live entries retain
    // their original snapshot, even after several manifest rewrites.
    const auto checkRetained = [&](const iceberg::ManifestEntry& entry, bool rewritten) -> iceberg::Status {
        const auto found = proof.Known.find(entry.data_file->file_path);
        if (found == proof.Known.end()) {
            return iceberg::ValidationFailed("KV rewritten input contains an unknown file.");
        }
        const auto& retained = index.Files[found->second];
        if (!retained.DataFile || *retained.DataFile != *entry.data_file ||
            entry.sequence_number != retained.IntroducedSequence ||
            entry.file_sequence_number != retained.IntroducedSequence)
        {
            return iceberg::ValidationFailed("KV changes a retained data descriptor or sequence.");
        }
        if (entry.status == iceberg::ManifestStatus::kDeleted) {
            const auto deleting = retained.RemovedSequence.value_or(snapshot->sequence_number);
            const auto id = proof.SnapshotBySequence.find(deleting);
            if (id == proof.SnapshotBySequence.end() || entry.snapshot_id != id->second ||
                (rewritten ? retained.RemovedSequence.has_value() : !retained.RemovedSequence))
            {
                return iceberg::ValidationFailed("Invalid KV deletion provenance.");
            }
        } else {
            const auto origin = proof.SnapshotBySequence.find(retained.IntroducedSequence);
            if (retained.RemovedSequence || origin == proof.SnapshotBySequence.end() ||
                entry.snapshot_id != origin->second ||
                (rewritten && entry.status != iceberg::ManifestStatus::kExisting))
            {
                return iceberg::ValidationFailed("KV resurrects a file or changes its origin.");
            }
        }
        return {};
    };
    const auto readManifest = [&](const iceberg::ManifestFile& manifest, bool rewritten) -> iceberg::Status {
        ICEBERG_RETURN_UNEXPECTED(KVAvro(io,
            manifest.manifest_path,
            manifest.manifest_length,
            NIO::EFileKind::Manifest,
            limits.ManifestBytes,
            limits.Store.DataFiles,
            rewritten ? attempt : std::shared_ptr<NIO::TIOAttempt>{}));
        ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestReader::Make(manifest, io, schema, spec));
        ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
        if (entries.empty() || entries.size() > limits.Store.DataFiles) {
            return iceberg::ValidationFailed("Invalid KV manifest entry count.");
        }
        std::array<std::int64_t, 3> files{};
        std::array<std::int64_t, 3> rows{};
        auto minSequence = manifest.sequence_number;
        for (const auto& entry : entries) {
            const auto status = static_cast<std::size_t>(entry.status);
            if (!entry.data_file || status >= files.size()) {
                return iceberg::ValidationFailed("Invalid KV manifest entry.");
            }
            const auto& file = *entry.data_file;
            if (file.record_count <= 0 || file.record_count > 100000 || file.file_size_in_bytes <= 0 ||
                file.file_size_in_bytes > (32LL << 20) || file.content != iceberg::DataFile::Content::kData ||
                file.partition_spec_id != metadata.default_spec_id || !entry.sequence_number ||
                file.data_sequence_number != entry.sequence_number)
            {
                return iceberg::ValidationFailed("Invalid KV data descriptor or inheritance.");
            }
            ICEBERG_RETURN_UNEXPECTED(CheckKVPath(file.file_path, table.Value(), NIO::EFileKind::Data));
            ++files[status];
            rows[status] += file.record_count;
            const auto found = proof.Known.find(file.file_path);
            if (!rewritten) {
                ICEBERG_RETURN_UNEXPECTED(checkRetained(entry, false));
                if (entry.status != iceberg::ManifestStatus::kDeleted &&
                    (!rewrittenLive.insert(file.file_path).second || live.erase(file.file_path) != 1))
                {
                    return iceberg::ValidationFailed("KV rewritten inputs duplicate or lose a live file.");
                }
            } else if (found == proof.Known.end()) {
                if (entry.status != iceberg::ManifestStatus::kAdded || entry.snapshot_id != snapshot->snapshot_id ||
                    entry.sequence_number != snapshot->sequence_number ||
                    entry.file_sequence_number != snapshot->sequence_number || counts.AddedFiles >= 256)
                {
                    return iceberg::ValidationFailed("KV introduces an unowned data file.");
                }
                if (proof.Visited) {
                    ICEBERG_ASSIGN_OR_RAISE(auto inserted, proof.Visited->Insert(NIO::EFileKind::Data, file.file_path));
                    if (!inserted) {
                        return iceberg::ValidationFailed("KV reuses a historical data file.");
                    }
                }
                ICEBERG_ASSIGN_OR_RAISE(auto footer, VerifyKVData(io, file, *schema, table.Value(), attempt));
                const auto total = ReplaceUsage(delta, {.Next = {.DataFiles = 1, .FooterBytes = footer}});
                if (!total) {
                    return iceberg::ValidationFailed("KV data usage overflows.");
                }
                delta = total.Value();
                proof.Known.emplace(file.file_path, index.Files.size());
                index.Files.push_back({file.file_path,
                    file.file_size_in_bytes,
                    file.record_count,
                    entry.data_file,
                    snapshot->sequence_number,
                    {}});
                ++counts.AddedFiles;
                counts.AddedRows += static_cast<std::uint64_t>(file.record_count);
                counts.AddedBytes += static_cast<std::uint64_t>(file.file_size_in_bytes);
            } else {
                ICEBERG_RETURN_UNEXPECTED(checkRetained(entry, true));
                if (!rewrittenLive.contains(file.file_path)) {
                    return iceberg::ValidationFailed("KV rewrites a file from an untouched manifest.");
                }
                if (entry.status == iceberg::ManifestStatus::kDeleted && !deleted.insert(file.file_path).second) {
                    return iceberg::ValidationFailed("Duplicate KV file deletion.");
                }
            }
            if (entry.status != iceberg::ManifestStatus::kDeleted) {
                if (rewritten && !live.insert(file.file_path).second) {
                    return iceberg::ValidationFailed("Duplicate live KV file.");
                }
                minSequence = std::min(minSequence, *entry.sequence_number);
            }
        }
        if (manifest.existing_files_count != files[0] || manifest.added_files_count != files[1] ||
            manifest.deleted_files_count != files[2] || manifest.existing_rows_count != rows[0] ||
            manifest.added_rows_count != rows[1] || manifest.deleted_rows_count != rows[2] ||
            manifest.min_sequence_number != minSequence)
        {
            return iceberg::ValidationFailed("KV manifest counters or minimum sequence differ.");
        }
        return rewritten ? KVMark(attempt, manifest.manifest_path) : iceberg::Status{};
    };
    for (const auto* manifest : removedManifests) {
        ICEBERG_RETURN_UNEXPECTED(readManifest(*manifest, false));
    }
    for (const auto* manifest : addedManifests) {
        ICEBERG_RETURN_UNEXPECTED(readManifest(*manifest, true));
    }
    std::set<std::string> actualRemoved;
    for (const auto& path : rewrittenLive) {
        if (!live.contains(path)) {
            actualRemoved.insert(path);
            auto& file = index.Files[proof.Known.at(path)];
            file.RemovedSequence = snapshot->sequence_number;
            ++counts.RemovedFiles;
            counts.RemovedRows += static_cast<std::uint64_t>(file.RecordCount);
            counts.RemovedBytes += static_cast<std::uint64_t>(file.SizeBytes);
        }
    }
    if (actualRemoved != deleted || (counts.AddedFiles == 0 && counts.RemovedFiles == 0)) {
        return iceberg::ValidationFailed("KV snapshot does not match its file removals.");
    }
    using Fields = iceberg::SnapshotSummaryFields;
    if (!SummaryCount(*snapshot, Fields::kAddedDataFiles, counts.AddedFiles) ||
        !SummaryCount(*snapshot, Fields::kAddedRecords, counts.AddedRows) ||
        !SummaryCount(*snapshot, Fields::kAddedFileSize, counts.AddedBytes) ||
        !SummaryCount(*snapshot, Fields::kDeletedDataFiles, counts.RemovedFiles) ||
        !SummaryCount(*snapshot, Fields::kDeletedRecords, counts.RemovedRows) ||
        !SummaryCount(*snapshot, Fields::kRemovedFileSize, counts.RemovedBytes))
    {
        return iceberg::ValidationFailed("KV snapshot counters differ from its verified file delta.");
    }
    const auto operation = snapshot->summary.find("operation");
    const std::string expectedOperation = compact ? "replace"
        : counts.AddedFiles == 0                  ? "delete"
        : counts.RemovedFiles == 0                ? "append"
                                                  : "overwrite";
    ICEBERG_ASSIGN_OR_RAISE(auto inputRows, KVSummaryRows(*snapshot, "glamdb.kv-input-rows"));
    ICEBERG_ASSIGN_OR_RAISE(auto affectedRows, KVSummaryRows(*snapshot, "glamdb.kv-affected-rows"));
    const auto version = snapshot->summary.find("glamdb.kv-version");
    if (kind == snapshot->summary.end() || operation == snapshot->summary.end() || version == snapshot->summary.end() ||
        version->second != "1" || counts.RemovedFiles > 256 || operation->second != expectedOperation ||
        affectedRows > inputRows ||
        (compact ? inputRows != 0 || affectedRows != 0 || counts.AddedRows != counts.RemovedRows ||
                    counts.AddedFiles == 0 || counts.RemovedFiles < counts.AddedFiles * 4
                 : inputRows == 0 || affectedRows == 0 ||
                    (kind->second == "upsert" ? counts.AddedRows < counts.RemovedRows ||
                                counts.AddedRows - counts.RemovedRows > affectedRows
                                              : kind->second != "delete" || counts.RemovedRows < counts.AddedRows ||
                                counts.RemovedRows - counts.AddedRows != affectedRows)))
    {
        return iceberg::ValidationFailed("KV operation differs from its verified physical or logical delta.");
    }
    if (compact) {
        std::vector<const TScanDataFile*> orderedBase;
        orderedBase.reserve(baseLive.size());
        for (const auto& path : baseLive) {
            orderedBase.push_back(&index.Files[proof.Known.at(path)]);
        }
        std::ranges::sort(orderedBase, [&](const auto* left, const auto* right) {
            return left->DataFile->lower_bounds.at(keyID) < right->DataFile->lower_bounds.at(keyID);
        });
        bool entered = false;
        bool leftGroup = false;
        for (const auto* file : orderedBase) {
            if (actualRemoved.contains(file->Path)) {
                if (leftGroup) {
                    return iceberg::ValidationFailed("Compaction crosses an untouched key range.");
                }
                entered = true;
            } else if (entered) {
                leftGroup = true;
            }
        }
        std::vector<const TScanDataFile*> removedFiles;
        removedFiles.reserve(actualRemoved.size());
        std::vector<const TScanDataFile*> addedFiles;
        for (const auto& path : actualRemoved) {
            removedFiles.push_back(&index.Files[proof.Known.at(path)]);
        }
        for (auto offset = firstAdded; offset < index.Files.size(); ++offset) {
            addedFiles.push_back(&index.Files[offset]);
        }
        ICEBERG_ASSIGN_OR_RAISE(auto before, KVRowsDigest(io, std::move(removedFiles), keyID));
        ICEBERG_ASSIGN_OR_RAISE(auto after, KVRowsDigest(io, std::move(addedFiles), keyID));
        if (before != after) {
            return iceberg::ValidationFailed("Compaction changes the full row contents.");
        }
    }
    std::vector<std::pair<std::string, std::string>> ranges;
    for (const auto& path : live) {
        const auto& file = index.Files[proof.Known.at(path)];
        ICEBERG_ASSIGN_OR_RAISE(auto range, KeyBounds(*file.DataFile, keyID));
        ranges.push_back(std::move(range));
    }
    std::ranges::sort(ranges);
    for (std::size_t position = 1; position < ranges.size(); ++position) {
        if (ranges[position - 1].second >= ranges[position].first) {
            return iceberg::ValidationFailed("KV live key ranges overlap.");
        }
    }
    const auto usage = ReplaceUsage(candidate.Usage, {.Next = delta});
    if (!usage || CheckStorageCapacity(usage.Value(), limits)) {
        return iceberg::ValidationFailed("CAPACITY: KV retained closure exceeds storage capacity.");
    }
    candidate.Usage = usage.Value();
    candidate.CurrentCommitMetadataBytes = workBytes - (commitRoot ? candidate.Usage.RootBytes : 0);
    if (!replay) {
        candidate.RetainedManifestPaths = base.RetainedManifestPaths;
    }
    auto& retained = replay ? proof.RetainedManifestPaths : candidate.RetainedManifestPaths;
    for (const auto* manifest : addedManifests) {
        retained.push_back(manifest->manifest_path);
    }
    if (!replay) {
        std::ranges::sort(retained);
    }
    candidate.CurrentManifests = std::move(manifests);
    if (replay) {
        index.Snapshots.push_back({snapshot->snapshot_id, live.size(), snapshot->sequence_number});
    } else {
        const auto boundary =
            std::ranges::lower_bound(index.Snapshots, snapshot->snapshot_id, {}, &TSnapshotBoundary::SnapshotID);
        if (boundary != index.Snapshots.end() && boundary->SnapshotID == snapshot->snapshot_id) {
            return iceberg::ValidationFailed("Duplicate KV snapshot ID.");
        }
        index.Snapshots.insert(boundary, {snapshot->snapshot_id, live.size(), snapshot->sequence_number});
    }
    proof.Live = std::move(live);
    candidate.ScanIndex = proof.Index;
    ICEBERG_RETURN_UNEXPECTED(KVMark(attempt, snapshot->manifest_list));
    return candidate;
}

} // namespace

iceberg::Result<TBaseState> CheckKVClosure(const std::shared_ptr<iceberg::FileIO>& fileIO,
    TBaseState candidate,
    const TBaseState& base,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits)
{
    return CatchIcebergExceptions([&] {
        return CheckKVTransition(fileIO, std::move(candidate), base, attempt, limits, true, nullptr);
    });
}

iceberg::Result<TBaseState> VerifyKVReachable(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits,
    const std::string& workspaceDirectory)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TBaseState> {
        const auto root = NIO::TStoreUri::Parse(rootLocation, storeID);
        if (!fileIO || !root || root.Value().CollectionId() != collectionID ||
            root.Value().Kind() != NIO::EFileKind::Metadata || limits.RootBytes > INT64_MAX)
        {
            return iceberg::InvalidArgument("Invalid KV recovery root.");
        }
        ICEBERG_ASSIGN_OR_RAISE(auto input, fileIO->NewInputFile(rootLocation));
        ICEBERG_ASSIGN_OR_RAISE(auto rootBytes, input->Size());
        ICEBERG_ASSIGN_OR_RAISE(auto metadata,
            ReadTableMetadata(*fileIO, rootLocation, {}, static_cast<std::int64_t>(limits.RootBytes)));
        if (rootBytes <= 0 || metadata->location != "store://" + storeID + "/warehouse/" + collectionID ||
            !IsKVMetadata(*metadata) || metadata->snapshots.size() != metadata->snapshot_log.size() ||
            metadata->metadata_log.size() > limits.MetadataLogEntries)
        {
            return iceberg::ValidationFailed("Unsupported KV recovery root.");
        }
        auto original = std::shared_ptr<iceberg::TableMetadata>(std::move(metadata));
        auto view = std::make_shared<iceberg::TableMetadata>(*original);
        view->snapshots.clear();
        view->snapshot_log.clear();
        view->refs.clear();
        view->current_snapshot_id = iceberg::kInvalidSnapshotId;
        view->last_sequence_number = 0;
        ICEBERG_ASSIGN_OR_RAISE(auto schema, view->Schema());
        ICEBERG_RETURN_UNEXPECTED(CheckCreateMetadata(*view, {schema, view->properties.configs()}, view->location));
        TBaseState state{rootLocation,
            view,
            {},
            {.RootBytes = static_cast<std::uint64_t>(rootBytes),
                .Snapshots = original->snapshots.size(),
                .SnapshotLogEntries = original->snapshot_log.size()}};
        TKVReplay replay;
        replay.Index = std::make_shared<TScanMembership>();
        replay.Index->KeyValue = true;
        replay.Index->Files.reserve(original->snapshots.size());
        replay.Index->Snapshots.reserve(original->snapshots.size());
        ICEBERG_ASSIGN_OR_RAISE(replay.Visited,
            TRecoveryVisited::Make(limits.RecoveryWorkspaceBytes, workspaceDirectory));
        state.ScanIndex = replay.Index;
        if (CheckStorageCapacity(state.Usage, limits)) {
            return iceberg::ValidationFailed("CAPACITY: KV recovery metadata exceeds capacity.");
        }
        std::unordered_set<std::int64_t> snapshotIDs;
        snapshotIDs.reserve(original->snapshots.size());
        for (std::size_t position = 0; position < original->snapshots.size(); ++position) {
            const auto& snapshot = original->snapshots[position];
            if (!snapshot || !snapshotIDs.insert(snapshot->snapshot_id).second) {
                return iceberg::ValidationFailed("Invalid KV retained snapshot history.");
            }
            ICEBERG_ASSIGN_OR_RAISE(auto inserted,
                replay.Visited->Insert(NIO::EFileKind::ManifestList, snapshot->manifest_list));
            if (!inserted) {
                return iceberg::ValidationFailed("KV reuses a retained manifest list.");
            }
            auto next = std::make_shared<iceberg::TableMetadata>(*view);
            next->snapshots.push_back(snapshot);
            next->snapshot_log.push_back(original->snapshot_log[position]);
            next->current_snapshot_id = snapshot->snapshot_id;
            next->last_sequence_number = snapshot->sequence_number;
            next->refs["main"] =
                std::make_shared<iceberg::SnapshotRef>(iceberg::SnapshotRef{.snapshot_id = snapshot->snapshot_id,
                    .retention = iceberg::SnapshotRef::Branch{}});
            ICEBERG_RETURN_UNEXPECTED(CheckKVMetadata(*view, *next, {}));
            auto candidate = state;
            candidate.Metadata = next;
            ICEBERG_ASSIGN_OR_RAISE(state,
                CheckKVTransition(fileIO,
                    std::move(candidate),
                    state,
                    {},
                    limits,
                    position + 1 == original->snapshots.size(),
                    &replay));
            if (next->snapshots.size() > 1) {
                next->snapshots.erase(next->snapshots.begin());
                next->snapshot_log.erase(next->snapshot_log.begin());
            }
            view = std::move(next);
        }
        view->snapshots = original->snapshots;
        view->snapshot_log = original->snapshot_log;
        std::ranges::sort(replay.Index->Snapshots, {}, &TSnapshotBoundary::SnapshotID);
        if (*view != *original) {
            return iceberg::ValidationFailed("KV recovery root has an inconsistent current view.");
        }
        state.RetainedManifestPaths = std::move(replay.RetainedManifestPaths);
        std::ranges::sort(state.RetainedManifestPaths);
        state.Metadata = std::move(original);
        return state;
    });
}

}
