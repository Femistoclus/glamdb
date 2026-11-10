#include "core/engine/storage_mapping.h"
#include "core/scan/predicate.h"

#if GLAMDB_HAS_STORAGE
#include "core/backend/inert_catalog.h"
#include "core/scan/scan_cursor.h"
#include "core/scan/scan_planner.h"
#include "core/storage/format_registry.h"
#include "core/storage/table_profile.h"
#include <arrow/c/bridge.h>
#include <iceberg/file_io.h>
#include <iceberg/file_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/metrics_config.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>
#endif

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <cstring>
#include <functional>
#include <limits>
#include <map>

namespace glamdb::detail {
namespace {

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"flag", FieldType::Bool, true},
    {"count", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

std::shared_ptr<arrow::RecordBatch> Batch()
{
    const std::vector<Row> rows{
        {std::string("B"), true, int64_t{1}, -0.0, TimestampUs{10}},
        {std::string("a"), false, int64_t{2}, 1.5, TimestampUs{20}},
        {std::string("\xC3\xA9"), std::monostate{}, std::monostate{}, std::monostate{}, std::monostate{}},
        {std::string("a"), true, int64_t{3}, 2.5, TimestampUs{30}},
    };
    auto batch = MakeRecordBatch(Schema, rows);
    EXPECT_TRUE(batch) << batch.error().message;
    return *batch;
}

std::vector<int64_t> Select(const std::string& field, CompareOp op, Literal value)
{
    const auto bound = BindPredicate(Predicate::Compare(field, op, std::move(value)), Schema);
    EXPECT_TRUE(bound) << bound.error().message;
    if (!bound) {
        return {};
    }
    const auto selected = SelectRows(*bound, *Batch()->column(static_cast<int>(bound->FieldIndex)));
    EXPECT_TRUE(selected) << selected.error().message;
    return selected ? *selected : std::vector<int64_t>{};
}

TEST(ScanPredicate, TrueSelectsEveryRow)
{
    const auto bound = BindPredicate(Predicate::True(), Schema);
    ASSERT_TRUE(bound);
    const auto selected = SelectRows(*bound, *Batch()->column(0));
    ASSERT_TRUE(selected);
    EXPECT_EQ(*selected, (std::vector<int64_t>{0, 1, 2, 3}));
}

TEST(ScanPredicate, RejectsUnknownFieldsAndMismatchedLiterals)
{
    const auto rejected = [](const Predicate& predicate) {
        const auto bound = BindPredicate(predicate, Schema);
        return !bound && bound.error().code == "INVALID_ARGUMENT";
    };
    EXPECT_TRUE(rejected(Predicate::Compare("missing", CompareOp::Eq, int64_t{1})));
    EXPECT_TRUE(rejected(Predicate::Compare("count", CompareOp::Eq, 1.0)));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Eq, int64_t{1})));
    EXPECT_TRUE(rejected(Predicate::Compare("key", CompareOp::Eq, false)));
    EXPECT_TRUE(rejected(Predicate::Compare("key", CompareOp::Eq, std::string("\xFF"))));
    EXPECT_TRUE(rejected(Predicate::Compare("score", CompareOp::Lt, std::numeric_limits<double>::quiet_NaN())));
    EXPECT_TRUE(rejected(Predicate::Compare("score", CompareOp::Lt, std::numeric_limits<double>::infinity())));
    EXPECT_TRUE(rejected(Predicate::Compare("at", CompareOp::Lt, TimestampUs{253402300800000000LL})));
}

TEST(ScanPredicate, NullNeverMatchesAnyComparison)
{
    for (const auto op : {CompareOp::Eq, CompareOp::Ne, CompareOp::Lt, CompareOp::Le, CompareOp::Gt, CompareOp::Ge}) {
        for (const auto row : Select("count", op, int64_t{2})) {
            EXPECT_NE(row, 2) << static_cast<int>(op);
        }
    }
    EXPECT_EQ(Select("count", CompareOp::Ne, int64_t{2}), (std::vector<int64_t>{0, 3}));
    EXPECT_EQ(Select("flag", CompareOp::Ne, true), (std::vector<int64_t>{1}));
}

TEST(ScanPredicate, ComparesEveryType)
{
    EXPECT_EQ(Select("count", CompareOp::Ge, int64_t{2}), (std::vector<int64_t>{1, 3}));
    EXPECT_EQ(Select("count", CompareOp::Lt, int64_t{2}), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("flag", CompareOp::Eq, true), (std::vector<int64_t>{0, 3}));
    EXPECT_EQ(Select("score", CompareOp::Eq, 0.0), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("score", CompareOp::Eq, -0.0), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("score", CompareOp::Gt, 1.5), (std::vector<int64_t>{3}));
    EXPECT_EQ(Select("at", CompareOp::Le, TimestampUs{20}), (std::vector<int64_t>{0, 1}));
    EXPECT_EQ(Select("key", CompareOp::Eq, std::string("a")), (std::vector<int64_t>{1, 3}));
}

TEST(ScanPredicate, StringsUseBinaryUtf8Order)
{
    // "B" (0x42) < "a" (0x61) < "\xC3\xA9" (U+00E9) by unsigned bytes.
    EXPECT_EQ(Select("key", CompareOp::Lt, std::string("a")), (std::vector<int64_t>{0}));
    EXPECT_EQ(Select("key", CompareOp::Gt, std::string("a")), (std::vector<int64_t>{2}));
}

TEST(ScanPredicate, TakeRowsPreservesValuesAndNulls)
{
    const auto batch = Batch();
    const std::vector<int64_t> rows{3, 2};
    for (int column = 0; column < batch->num_columns(); ++column) {
        const auto taken = TakeRows(*batch->column(column), rows);
        ASSERT_TRUE(taken) << taken.error().message;
        ASSERT_EQ((*taken)->length(), 2);
        EXPECT_TRUE((*taken)->type()->Equals(*batch->column(column)->type()));
        EXPECT_TRUE((*taken)->RangeEquals(0, 1, 3, batch->column(column)));
        EXPECT_EQ((*taken)->IsNull(1), batch->column(column)->IsNull(2));
    }
    EXPECT_FALSE(TakeRows(*batch->column(0), std::vector<int64_t>{4}));
}

#if GLAMDB_HAS_STORAGE

class TMemoryInputStream final : public iceberg::SeekableInputStream
{
public:
    explicit TMemoryInputStream(std::string bytes)
        : Bytes_(std::move(bytes))
    { }

    iceberg::Result<int64_t> Position() const override
    {
        return Position_;
    }

    iceberg::Status Seek(int64_t position) override
    {
        if (position < 0 || position > static_cast<int64_t>(Bytes_.size())) {
            return iceberg::InvalidArgument("invalid seek position");
        }
        Position_ = position;
        return {};
    }

    iceberg::Result<int64_t> Read(std::span<std::byte> out) override
    {
        const auto available = static_cast<int64_t>(Bytes_.size()) - Position_;
        const auto count = std::min<int64_t>(available, static_cast<int64_t>(out.size()));
        if (count > 0) {
            std::memcpy(out.data(), Bytes_.data() + Position_, static_cast<size_t>(count));
            Position_ += count;
        }
        return count;
    }

    iceberg::Status ReadFully(int64_t position, std::span<std::byte> out) override
    {
        if (position < 0 || position + static_cast<int64_t>(out.size()) > static_cast<int64_t>(Bytes_.size())) {
            return iceberg::Invalid("short read in memory stream");
        }
        std::memcpy(out.data(), Bytes_.data() + position, out.size());
        return {};
    }

    iceberg::Status Close() override
    {
        return {};
    }

private:
    std::string Bytes_;
    int64_t Position_ = 0;
};

class TMemoryOutputStream final : public iceberg::PositionOutputStream
{
public:
    TMemoryOutputStream(std::map<std::string, std::string>* files, std::string location)
        : Files_(files)
        , Location_(std::move(location))
    { }

    iceberg::Result<int64_t> Position() const override
    {
        return static_cast<int64_t>(Buffer_.size());
    }

    iceberg::Status Write(std::span<const std::byte> data) override
    {
        Buffer_.append(reinterpret_cast<const char*>(data.data()), data.size());
        return {};
    }

    iceberg::Status Flush() override
    {
        return {};
    }

    iceberg::Status Close() override
    {
        (*Files_)[Location_] = Buffer_;
        return {};
    }

private:
    std::map<std::string, std::string>* Files_;
    std::string Location_;
    std::string Buffer_;
};

class TMemoryInputFile final : public iceberg::InputFile
{
public:
    TMemoryInputFile(std::string location, std::string bytes)
        : Location_(std::move(location))
        , Bytes_(std::move(bytes))
    { }

    std::string_view location() const override
    {
        return Location_;
    }

    iceberg::Result<int64_t> Size() const override
    {
        return static_cast<int64_t>(Bytes_.size());
    }

    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        return std::make_unique<TMemoryInputStream>(Bytes_);
    }

private:
    std::string Location_;
    std::string Bytes_;
};

class TMemoryOutputFile final : public iceberg::OutputFile
{
public:
    TMemoryOutputFile(std::map<std::string, std::string>* files, std::string location)
        : Files_(files)
        , Location_(std::move(location))
    { }

    std::string_view location() const override
    {
        return Location_;
    }

    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> Create() override
    {
        if (Files_->contains(Location_)) {
            return iceberg::AlreadyExists("memory file already exists");
        }
        return std::make_unique<TMemoryOutputStream>(Files_, Location_);
    }

    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> CreateOrOverwrite() override
    {
        return std::make_unique<TMemoryOutputStream>(Files_, Location_);
    }

private:
    std::map<std::string, std::string>* Files_;
    std::string Location_;
};

class TMemoryFileIO final : public iceberg::FileIO
{
public:
    std::map<std::string, std::string> Files;
    std::function<void(const std::string&)> OnRead;

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location) override
    {
        if (OnRead) {
            OnRead(location);
        }
        const auto it = Files.find(location);
        if (it == Files.end()) {
            return iceberg::NotFound("memory file not found: {}", location);
        }
        return std::make_unique<TMemoryInputFile>(std::move(location), it->second);
    }

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location, size_t) override
    {
        return NewInputFile(std::move(location));
    }

    iceberg::Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string location) override
    {
        return std::make_unique<TMemoryOutputFile>(&Files, std::move(location));
    }

    iceberg::Result<std::string> ReadFile(const std::string& location, std::optional<size_t> length) override
    {
        if (OnRead) {
            OnRead(location);
        }
        const auto it = Files.find(location);
        if (it == Files.end()) {
            return iceberg::NotFound("memory file not found: {}", location);
        }
        if (length && *length < it->second.size()) {
            return it->second.substr(0, *length);
        }
        return it->second;
    }

    iceberg::Status WriteFile(const std::string& location, std::string_view content) override
    {
        Files[location] = std::string(content);
        return {};
    }

    iceberg::Status DeleteFile(const std::string& location) override
    {
        Files.erase(location);
        return {};
    }
};

std::shared_ptr<iceberg::DataFile> WriteMemoryParquet(const std::shared_ptr<TMemoryFileIO>& io,
    const iceberg::TableMetadata& metadata,
    const std::string& path,
    const std::shared_ptr<arrow::RecordBatch>& batch)
{
    NGlamDB::NStorage::RegisterStorageFormats();
    auto schema = metadata.Schema();
    EXPECT_TRUE(schema.has_value());
    iceberg::WriterOptions options;
    options.path = path;
    options.io = io;
    options.schema = *schema;
    auto metricsConfig = iceberg::MetricsConfig::Make(metadata.properties.configs());
    EXPECT_TRUE(metricsConfig.has_value());
    options.metrics_config = *metricsConfig;
    auto writer = iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options);
    EXPECT_TRUE(writer.has_value());

    ArrowArray exported{};
    EXPECT_TRUE(arrow::ExportRecordBatch(*batch, &exported).ok());
    EXPECT_TRUE((*writer)->Write(&exported).has_value());
    if (exported.release != nullptr) {
        exported.release(&exported);
    }
    EXPECT_TRUE((*writer)->Close().has_value());

    auto file = std::make_shared<iceberg::DataFile>();
    file->file_path = path;
    file->file_format = iceberg::FileFormatType::kParquet;
    file->content = iceberg::DataFile::Content::kData;
    file->record_count = batch->num_rows();
    file->file_size_in_bytes = static_cast<int64_t>(io->Files.at(path).size());
    file->partition_spec_id = metadata.default_spec_id;
    file->sort_order_id = metadata.default_sort_order_id;
    return file;
}

TEST(ScanPlannerUnit, PlansAndScansPurelyInMemoryAndEnforcesManifestMembershipCorruptionAndCancel)
{
    constexpr char StoreId[] = "00000000-0000-4000-8000-000000000001";
    constexpr char CollectionId[] = "00000000-0000-4000-8000-000000000011";
    const std::string tableLocation = std::string("store://") + StoreId + "/warehouse/" + CollectionId;

    auto io = std::make_shared<TMemoryFileIO>();
    auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(Schema));
    ASSERT_TRUE(profile.has_value());

    auto meta0 = iceberg::TableMetadata::Make(*profile->Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        tableLocation,
        profile->Properties);
    ASSERT_TRUE(meta0.has_value());
    const std::string root0 = tableLocation + "/metadata/00000-a0.metadata.json";
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root0, **meta0).has_value());

    auto catalog = std::make_shared<TInertCatalog>();
    const iceberg::TableIdentifier ident{.ns = iceberg::Namespace{.levels = {"columnar"}}, .name = "events"};

    // Stage snapshot 1 with file 1.
    const std::string dataPath1 = tableLocation + "/data/op1-a1-00000.parquet";
    const auto file1 = WriteMemoryParquet(io, **meta0, dataPath1, Batch());
    auto table0 = iceberg::Table::Make(ident, std::make_shared<iceberg::TableMetadata>(**meta0), root0, io, catalog);
    ASSERT_TRUE(table0.has_value());
    auto tx1 = iceberg::Transaction::Make(std::move(*table0), iceberg::TransactionKind::kUpdate);
    ASSERT_TRUE(tx1.has_value());
    auto append1 = (*tx1)->NewFastAppend();
    ASSERT_TRUE(append1.has_value());
    (*append1)->AppendFile(file1);
    ASSERT_TRUE((*append1)->Commit().has_value());
    const auto& meta1 = (*tx1)->current();
    const std::string root1 = tableLocation + "/metadata/00001-a1.metadata.json";
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root1, meta1).has_value());
    const int64_t snap1 = meta1.current_snapshot_id;

    // Stage snapshot 2 with file 2, plus an unreferenced orphan file in memory.
    const std::string dataPath2 = tableLocation + "/data/op2-a2-00000.parquet";
    const auto file2 = WriteMemoryParquet(io, meta1, dataPath2, Batch());
    const std::string orphanPath = tableLocation + "/data/orphan-00000.parquet";
    (void)WriteMemoryParquet(io, meta1, orphanPath, Batch());

    auto table1 = iceberg::Table::Make(ident, std::make_shared<iceberg::TableMetadata>(meta1), root1, io, catalog);
    ASSERT_TRUE(table1.has_value());
    auto tx2 = iceberg::Transaction::Make(std::move(*table1), iceberg::TransactionKind::kUpdate);
    ASSERT_TRUE(tx2.has_value());
    auto append2 = (*tx2)->NewFastAppend();
    ASSERT_TRUE(append2.has_value());
    (*append2)->AppendFile(file2);
    ASSERT_TRUE((*append2)->Commit().has_value());
    const auto& meta2 = (*tx2)->current();
    const std::string root2 = tableLocation + "/metadata/00002-a2.metadata.json";
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root2, meta2).has_value());
    const int64_t snap2 = meta2.current_snapshot_id;

    const BaseIdentity base{StoreId, CollectionId, meta2.table_uuid, root2, snap2, 0, 0};
    const CollectionInfo collection{CollectionId, "events", meta2.table_uuid, Schema, 1, snap2};

    const auto emptyPlan = PlanScan(io, base, SnapshotSelector::InitialEmpty());
    ASSERT_TRUE(emptyPlan) << emptyPlan.error().message;
    EXPECT_FALSE(emptyPlan->SnapshotID.has_value());
    EXPECT_TRUE(emptyPlan->Files.empty());

    const auto historicalPlan = PlanScan(io, base, SnapshotSelector::Id(snap1));
    ASSERT_TRUE(historicalPlan) << historicalPlan.error().message;
    ASSERT_EQ(historicalPlan->SnapshotID, std::optional<int64_t>(snap1));
    ASSERT_EQ(historicalPlan->Files.size(), 1U);
    EXPECT_EQ(historicalPlan->Files[0].Path, dataPath1);

    const auto latestPlan = PlanScan(io, base, SnapshotSelector::Latest());
    ASSERT_TRUE(latestPlan) << latestPlan.error().message;
    ASSERT_EQ(latestPlan->SnapshotID, std::optional<int64_t>(snap2));
    ASSERT_EQ(latestPlan->Files.size(), 2U);
    for (const auto& planned : latestPlan->Files) {
        EXPECT_NE(planned.Path, orphanPath);
    }

    // Deterministic Cancel() inside FileIO::ReadFile during Next().
    auto cursorForCancel = OpenScan(
        {.Collection = collection, .Base = base, .FileIO = io, .Closing = nullptr, .Lease = nullptr, .OnError = {}},
        {.collection_id = CollectionId, .selector = SnapshotSelector::Id(snap1)});
    ASSERT_TRUE(cursorForCancel) << cursorForCancel.error().message;
    io->OnRead = [&](const std::string& path) {
        if (path == dataPath1) {
            (*cursorForCancel)->Cancel();
        }
    };
    const auto cancelledNext = (*cursorForCancel)->Next();
    io->OnRead = nullptr;
    ASSERT_FALSE(cancelledNext);
    EXPECT_EQ(cancelledNext.error().code, "CANCELLED");

    // Corrupting the data file in memory triggers CORRUPTION and invokes OnError.
    std::optional<ErrorDetail> reportedError;
    auto cursorForCorrupt = OpenScan({.Collection = collection,
                                         .Base = base,
                                         .FileIO = io,
                                         .Closing = nullptr,
                                         .Lease = nullptr,
                                         .OnError =
                                             [&](const ErrorDetail& detail) {
                                                 reportedError = detail;
                                             }},
        {.collection_id = CollectionId, .selector = SnapshotSelector::Id(snap1)});
    ASSERT_TRUE(cursorForCorrupt) << cursorForCorrupt.error().message;
    io->Files.erase(dataPath1);
    const auto corruptNext = (*cursorForCorrupt)->Next();
    ASSERT_FALSE(corruptNext);
    EXPECT_EQ(corruptNext.error().code, "CORRUPTION");
    ASSERT_TRUE(reportedError.has_value());
    EXPECT_EQ(reportedError->code, "CORRUPTION");
}

#endif

} // namespace
} // namespace glamdb::detail
