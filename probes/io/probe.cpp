#include "probes/io/probe.h"

#include "core/io/durable_file.h"
#include "core/io/store_uri.h"
#include "core/storage/append_stager.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/publication_guard.h"
#include "tests/support/durability_trace.h"

#include <arrow/array/builder_binary.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/file_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/manifest/rolling_manifest_writer.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>

#include <algorithm>
#include <fnmatch.h>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>

namespace NGlamDB::NProbe {
namespace {

using namespace NStorage;
using namespace NIO;
using namespace NTesting;

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
constexpr auto ManifestName = "55555555-5555-4555-8555-555555555555-m0.avro";

void Require(bool condition, const std::string& message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <class T>
T Take(iceberg::Result<T> result)
{
    Require(result.has_value(), result ? "" : result.error().message);
    return std::move(*result);
}

template <class T>
T TakeIO(TResult<T> result)
{
    Require(static_cast<bool>(result), result.Error().message());
    return std::move(result).Value();
}

void Check(iceberg::Status result)
{
    Require(result.has_value(), result ? "" : result.error().message);
}

std::string Outcome(const iceberg::Status& result)
{
    return result ? "OK" : result.error().message;
}

void WriteArtifact(const std::filesystem::path& path, std::string_view bytes)
{
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.exceptions(std::ios::badbit | std::ios::failbit);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
}

std::string KindName(EFileKind kind)
{
    switch (kind) {
        case EFileKind::Data:
            return "Data";
        case EFileKind::Manifest:
            return "Manifest";
        case EFileKind::ManifestList:
            return "ManifestList";
        case EFileKind::Metadata:
            return "Metadata";
        default:
            return "Other";
    }
}

class TMemoryStore final : public iceberg::sql::CatalogStore
{
public:
    std::optional<std::string> Location;
    std::function<iceberg::Status(const TPointerChange&)> Before;
    std::function<void()> After;

    iceberg::Status Initialize() override
    {
        return {};
    }
    iceberg::Result<std::vector<std::string>> ListNamespaceNames() override
    {
        return std::vector<std::string>{};
    }
    iceberg::Result<std::vector<iceberg::sql::NamespaceProperty>> GetNamespaceProperties(std::string_view) override
    {
        return std::vector<iceberg::sql::NamespaceProperty>{};
    }
    iceberg::Status
    InsertNamespaceProperty(std::string_view, std::string_view, std::optional<std::string_view>) override
    {
        return iceberg::NotSupported("Probe only supports the root namespace.");
    }
    iceberg::Status DeleteNamespaceProperty(std::string_view, std::string_view) override
    {
        return iceberg::NotSupported("Probe does not delete namespaces.");
    }
    iceberg::Result<std::int64_t> DeleteNamespace(std::string_view) override
    {
        return iceberg::NotSupported("Probe does not delete namespaces.");
    }
    iceberg::Result<std::vector<std::string>> ListTableNames(std::string_view) override
    {
        return Location ? std::vector<std::string>{"events"} : std::vector<std::string>{};
    }
    iceberg::Result<bool> TableExists(std::string_view, std::string_view) override
    {
        return Location.has_value();
    }
    iceberg::Result<std::optional<std::string>> GetTableMetadataLocation(std::string_view, std::string_view) override
    {
        return Location;
    }
    iceberg::Status InsertTable(std::string_view ns, std::string_view name, std::string_view location) override
    {
        if (Location) {
            return iceberg::AlreadyExists("Probe table already exists.");
        }
        const auto checked =
            Before({EPointerOp::InsertTable, std::string(ns), std::string(name), {}, std::string(location)});
        if (!checked) {
            return checked;
        }
        Location = location;
        After();
        return {};
    }
    iceberg::Result<std::int64_t> UpdateTableMetadataLocation(std::string_view ns,
        std::string_view name,
        std::string_view next,
        std::string_view,
        std::string_view expected) override
    {
        if (!Location || *Location != expected) {
            return 0;
        }
        const auto checked = Before(
            {EPointerOp::UpdateLocation, std::string(ns), std::string(name), std::string(expected), std::string(next)});
        if (!checked) {
            return std::unexpected(checked.error());
        }
        Location = next;
        After();
        return 1;
    }
    iceberg::Result<std::int64_t> DeleteTable(std::string_view, std::string_view) override
    {
        return iceberg::NotSupported("Probe does not delete tables.");
    }
    iceberg::Result<std::int64_t>
    RenameTable(std::string_view, std::string_view, std::string_view, std::string_view) override
    {
        return iceberg::NotSupported("Probe does not rename tables.");
    }
    iceberg::Status RunInTransaction(const std::function<iceberg::Status()>& body) override
    {
        const auto saved = Location;
        auto result = body();
        if (!result) {
            Location = saved;
        }
        return result;
    }
};

struct TFileInfo
{
    EFileKind Kind;
    std::uint64_t Length;
};

class TFlushFaultStream final : public iceberg::PositionOutputStream
{
    std::unique_ptr<iceberg::PositionOutputStream> Inner_;
    std::shared_ptr<TIOAttempt> Attempt_;
    std::string Path_;

public:
    TFlushFaultStream(std::unique_ptr<iceberg::PositionOutputStream> inner,
        std::shared_ptr<TIOAttempt> attempt,
        std::string path)
        : Inner_(std::move(inner))
        , Attempt_(std::move(attempt))
        , Path_(std::move(path))
    { }
    iceberg::Result<std::int64_t> Position() const override
    {
        return Inner_->Position();
    }
    iceberg::Status Write(std::span<const std::byte> bytes) override
    {
        return Inner_->Write(bytes);
    }
    iceberg::Status Flush() override
    {
        Attempt_->FailFile(Path_, std::make_error_code(std::errc::no_space_on_device));
        return Inner_->Flush();
    }
    iceberg::Status Close() override
    {
        return Inner_->Close();
    }
};

class TFixture
{
    const TOptions& Options_;
    std::shared_ptr<TPosixLocalFS> Posix_ = std::make_shared<TPosixLocalFS>();
    std::shared_ptr<TFaultFS> FS_ = std::make_shared<TFaultFS>(*Posix_);
    std::shared_ptr<TWriteLatch> Latch_ = std::make_shared<TWriteLatch>();
    std::shared_ptr<TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> IO_;
    std::shared_ptr<TMemoryStore> Store_ = std::make_shared<TMemoryStore>();
    std::shared_ptr<iceberg::sql::SqlCatalog> Catalog_;
    std::shared_ptr<iceberg::Table> Table_;
    TTableProfile Profile_;
    std::shared_ptr<const TBaseState> Base_;
    std::shared_ptr<const TBaseState> Candidate_;
    std::shared_ptr<TIOAttempt> Attempt_;
    std::vector<std::shared_ptr<TIOAttempt>> Attempts_;
    std::optional<TAttemptScope> Scope_;
    std::vector<std::string> PublishedRoots_;
    std::vector<std::string> References_;
    std::vector<std::pair<std::string, std::uint64_t>> Phases_;
    std::optional<TFaultRule> Rule_;
    std::uint64_t RuleStart_ = 0;
    std::optional<std::string> FirstClose_;
    std::optional<std::string> SecondClose_;
    std::uint64_t InjectionHits_ = 0;
    std::string Actual_ = "rejected";

    static std::string Location()
    {
        return std::string("store://") + StoreID + "/warehouse/" + CollectionID;
    }
    static std::string AttemptID(int index)
    {
        return "33333333-3333-7333-8333-33333333333" + std::to_string(index + 3);
    }
    static std::string OperationID(int index)
    {
        return "44444444-4444-4444-8444-44444444444" + std::to_string(index);
    }
    static std::string DataPath(int index)
    {
        return Location() + "/data/" + OperationID(index) + "-" + AttemptID(index) + "-00000.parquet";
    }
    std::string Physical(std::string_view uri) const
    {
        return (Options_.Root / TakeIO(TStoreUri::Parse(uri, StoreID)).RelativePath()).string();
    }
    void Phase(std::string name)
    {
        Phases_.emplace_back(std::move(name), FS_->Events().size());
    }
    void Arm(TFaultRule rule)
    {
        RuleStart_ = FS_->Events().size();
        Rule_ = rule;
        FS_->AddRule(std::move(rule));
    }
    void Bind(int index)
    {
        Scope_.reset();
        Attempt_ = TakeIO(TIOAttempt::Make({StoreID,
                                               CollectionID,
                                               AttemptID(index),
                                               index ? EAttemptKind::Append : EAttemptKind::CreateCollection,
                                               index ? std::optional(OperationID(index)) : std::nullopt},
            Latch_));
        Attempts_.push_back(Attempt_);
        Scope_.emplace(Take(IO_->BindAttempt(Attempt_)));
        Phase(index ? "append" + std::to_string(index) + "-begin" : "create-begin");
    }
    static std::shared_ptr<arrow::RecordBatch> Batch()
    {
        arrow::StringBuilder builder;
        for (int index = 0; index < 1000; ++index) {
            Require(builder.Append("duplicate").ok(), "Cannot build input.");
        }
        auto array = builder.Finish();
        Require(array.ok(), array.status().ToString());
        return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false)}), 1000, {*array});
    }
    std::map<std::string, TFileInfo> Inventory(const TBaseState& state)
    {
        std::map<std::string, TFileInfo> files;
        const auto add = [&](const std::string& path, EFileKind kind) {
            auto input = Take(IO_->NewInputFile(path));
            const auto size = Take(input->Size());
            Require(size >= 0, "Negative file length.");
            files.emplace(path, TFileInfo{kind, static_cast<std::uint64_t>(size)});
        };
        add(state.MetadataLocation, EFileKind::Metadata);
        for (const auto& snapshot : state.Metadata->snapshots) {
            add(snapshot->manifest_list, EFileKind::ManifestList);
            iceberg::SnapshotReader snapshotReader(snapshot.get());
            for (const auto& manifest : Take(snapshotReader.Manifests(IO_))) {
                if (files.contains(manifest.manifest_path)) {
                    continue;
                }
                add(manifest.manifest_path, EFileKind::Manifest);
                auto reader = Take(iceberg::ManifestReader::Make(manifest,
                    IO_,
                    Take(state.Metadata->Schema()),
                    Take(state.Metadata->PartitionSpec())));
                for (const auto& entry : Take(reader->Entries())) {
                    Require(static_cast<bool>(entry.data_file), "Manifest entry lacks data.");
                    add(entry.data_file->file_path, EFileKind::Data);
                }
            }
        }
        return files;
    }
    iceberg::Status Before(const TPointerChange& change)
    {
        TStoragePublicationGuard guard({.Directories = Dirs_,
            .FileIO = IO_,
            .Attempt = Attempt_,
            .Namespace = "",
            .Name = "events",
            .Profile = Profile_,
            .Base = Base_,
            .StoreUsage = Base_ ? Base_->Usage : TStorageUsage{}});
        auto status = guard.BeforePointerChange(change);
        if (!status) {
            return status;
        }
        Candidate_ = guard.GetCandidate();
        References_.clear();
        for (const auto& [path, info] : Inventory(*Candidate_)) {
            static_cast<void>(info);
            References_.push_back(Physical(path));
        }
        return {};
    }
    void Create()
    {
        Dirs_ = TakeIO(TStoreDirs::Create(FS_, Options_.Root, Latch_));
        TakeIO(Dirs_->CreateCollection(CollectionID));
        IO_ = Take(TDurableLocalFileIO::Make(Dirs_, StoreID));
        Profile_ = Take(MakeTableProfile({{{"key", EFieldType::String}}}));
        Store_->Before = [&](const auto& change) {
            return Before(change);
        };
        Store_->After = [&] {
            FS_->Publish(References_);
            Base_ = Candidate_;
            PublishedRoots_.push_back(Base_->MetadataLocation);
        };
        Catalog_ = Take(iceberg::sql::SqlCatalog::Make({.name = "io-probe-test-double",
                                                           .uri = "memory:",
                                                           .warehouse_location = Location(),
                                                           .max_connections = 1,
                                                           .props = {}},
            IO_,
            Store_));
        Bind(0);
        Table_ = Take(Catalog_->CreateTable({.ns = {}, .name = "events"},
            Profile_.Schema,
            iceberg::PartitionSpec::Unpartitioned(),
            iceberg::SortOrder::Unsorted(),
            Location(),
            Profile_.Properties));
        Scope_.reset();
    }
    void Append(int index)
    {
        Bind(index);
        auto data = Take(WriteDataFile(IO_, Attempt_, *Table_->metadata(), DataPath(index), {Batch()}));
        auto staged = Take(StageAppend(Table_, data.DataFile, {}, Attempt_));
        Table_ = Take(staged.Transaction->Commit());
        Scope_.reset();
        auto recovered =
            Take(VerifyReachable(Take(IO_->ReadOnlyView()), Base_->MetadataLocation, StoreID, CollectionID));
        Require(recovered.Usage == Base_->Usage, "Reopened closure differs from published state.");
    }
    void ArmPipeline()
    {
        const auto& name = Options_.Case;
        std::string pattern;
        std::string operation;
        int directoryOrdinal = 1;
        if (name.starts_with("manifest-list-")) {
            pattern = "*/snap-*.avro";
            operation = name.substr(14);
            directoryOrdinal = 2;
        } else if (name.starts_with("manifest-")) {
            pattern = "*-m*.avro";
            operation = name.substr(9);
        } else if (name.starts_with("metadata-")) {
            pattern = "*/00002-*.metadata.json";
            operation = name.substr(9);
            directoryOrdinal = 3;
        } else {
            pattern = "*.parquet";
            operation = name.starts_with("data-") ? name.substr(5) : name.substr(6);
        }
        TFaultRule rule{EIOOp::Write, pattern, 1, 1, std::make_error_code(std::errc::no_space_on_device), std::nullopt};
        if (operation == "short") {
            rule.Error.clear();
            rule.MaxBytes = 1;
        } else if (operation == "eintr") {
            rule.Error = std::make_error_code(std::errc::interrupted);
            rule.Repeat = 3;
        } else if (operation == "zero") {
            rule.Error.clear();
            rule.MaxBytes = 0;
        } else if (operation == "footer" || operation == "trailer") {
            rule.Occurrence = Options_.WriteOrdinal;
        } else if (operation == "sync-file") {
            rule.Op = EIOOp::SyncFile;
            rule.Error = std::make_error_code(std::errc::io_error);
        } else if (operation == "close") {
            rule.Op = EIOOp::Close;
        } else if (operation == "sync-dir") {
            rule.Op = EIOOp::SyncDir;
            rule.PathGlob =
                (Options_.Root / "warehouse" / CollectionID / (name.starts_with("data-") ? "data" : "metadata"))
                    .string();
            rule.Occurrence = directoryOrdinal;
            rule.Error = std::make_error_code(std::errc::io_error);
        }
        Arm(std::move(rule));
    }
    void RepeatClose(bool parquet)
    {
        Bind(2);
        const auto path = parquet ? DataPath(2) : Location() + "/metadata/" + ManifestName;
        if (parquet) {
            Require(!Attempt_->DeclareDataFile(path), "Cannot declare data path.");
        }
        iceberg::WriterOptions options;
        options.path = path;
        options.io = IO_;
        options.schema = Take(Table_->schema());
        options.properties = iceberg::WriterProperties::FromMap({});
        auto writer = Take(iceberg::WriterFactoryRegistry::Open(parquet ? iceberg::FileFormatType::kParquet
                                                                        : iceberg::FileFormatType::kAvro,
            options));
        ArrowArray exported{};
        Require(arrow::ExportRecordBatch(*Batch(), &exported).ok(), "Cannot export input.");
        const auto written = writer->Write(&exported);
        if (exported.release) {
            exported.release(&exported);
        }
        Check(written);
        Arm({EIOOp::Close, Physical(path), 1, 1, std::make_error_code(std::errc::no_space_on_device), std::nullopt});
        const auto first = writer->Close();
        FirstClose_ = Outcome(first);
        const auto second = writer->Close();
        SecondClose_ = Outcome(second);
        Require(!first && second, "Pinned writer did not expose first-error/repeated-Close behavior.");
        Require(Attempt_->Receipts().empty(), "Failed Close produced a receipt.");
    }
    void DestructorFailure()
    {
        Bind(2);
        const auto path = Location() + "/metadata/" + ManifestName;
        {
            iceberg::RollingManifestWriter writer(
                [&] {
                    return iceberg::ManifestWriter::MakeWriter(2,
                        123,
                        path,
                        IO_,
                        iceberg::PartitionSpec::Unpartitioned(),
                        Profile_.Schema);
                },
                8LL << 20);
            const auto invalid = writer.WriteAddedEntry(nullptr);
            Require(!invalid && invalid.error().kind == iceberg::ErrorKind::kInvalidArgument,
                "Expected a non-IO manifest failure before destruction.");
            Require(!Attempt_->FirstError(), "Stream failed before destructor.");
            Phase("destructor-enter");
            Arm({EIOOp::Write,
                Physical(path),
                1,
                1,
                std::make_error_code(std::errc::no_space_on_device),
                std::nullopt});
        }
        Phase("destructor-exit");
        Require(Attempt_->FirstError() == std::errc::no_space_on_device, "Destructor lost the stream error.");
        Require(Attempt_->Receipts().empty(), "Destructor failure produced a receipt.");
    }
    void Special()
    {
        if (Options_.Case == "parquet-repeat-close" || Options_.Case == "avro-repeat-close") {
            RepeatClose(Options_.Case == "parquet-repeat-close");
        } else if (Options_.Case == "avro-destructor") {
            DestructorFailure();
        } else if (Options_.Case == "outside-attempt") {
            const auto before = FS_->Events().size();
            Require(!IO_->NewOutputFile(DataPath(2)), "Output accepted without an attempt.");
            Require(FS_->Events().size() == before, "Unbound write touched filesystem.");
            InjectionHits_ = 1;
        } else if (Options_.Case == "exclusive-collision") {
            const auto dirs = TakeIO(Dirs_->OpenCollection(CollectionID));
            auto orphan = TakeIO(TDurableFile::Create(FS_, dirs.Data, DataPath(2), Latch_));
            const std::string bytes = "existing-orphan";
            Require(!orphan->Write(std::as_bytes(std::span(bytes))), "Cannot write collision fixture.");
            Require(!orphan->Close(), "Cannot close collision fixture.");
            Bind(2);
            Require(!Attempt_->DeclareDataFile(DataPath(2)), "Cannot declare collision path.");
            auto output = Take(IO_->NewOutputFile(DataPath(2)));
            Require(!output->CreateOrOverwrite(), "CreateOrOverwrite overwrote an orphan.");
            Scope_.reset();
            Require(Take(IO_->ReadFile(DataPath(2), std::nullopt)) == bytes, "Collision changed existing bytes.");
            InjectionHits_ = 1;
        } else if (Options_.Case == "flush-mask") {
            Bind(2);
            const auto path = Location() + "/metadata/" + ManifestName;
            auto output = Take(IO_->NewOutputFile(path));
            TFlushFaultStream stream(Take(output->Create()), Attempt_, path);
            Check(stream.Flush());
            const auto closed = stream.Close();
            FirstClose_ = Outcome(closed);
            Require(!closed && Attempt_->Receipts().empty(), "Flush failure escaped receipt checks.");
            InjectionHits_ = 1;
        } else {
            throw std::runtime_error("Unknown special case.");
        }
    }
    void CountHits()
    {
        if (!Rule_) {
            return;
        }
        std::uint64_t occurrence = 0;
        for (const auto& event : FS_->Events()) {
            if (event.Seq <= RuleStart_ || event.Op != Rule_->Op ||
                fnmatch(Rule_->PathGlob.c_str(), event.Path.c_str(), 0) != 0)
            {
                continue;
            }
            ++occurrence;
            if (occurrence >= Rule_->Occurrence && occurrence - Rule_->Occurrence < Rule_->Repeat &&
                event.Error == Rule_->Error && (!Rule_->MaxBytes || event.Bytes <= *Rule_->MaxBytes))
            {
                ++InjectionHits_;
            }
        }
    }
    void CheckTrace()
    {
        std::vector<std::string> baseline;
        for (auto path = Options_.Root.parent_path();; path = path.parent_path()) {
            baseline.push_back(path.string());
            if (path == path.root_path()) {
                break;
            }
        }
        TDurabilityTrace oracle(std::move(baseline));
        for (const auto& event : FS_->Events()) {
            auto error = oracle.Apply(event);
            Require(!error, error.value_or(""));
        }
    }
    void DumpManifests()
    {
        std::ostringstream output;
        output << "{\"schema_version\":1,\"string_encoding\":\"latin1-bytes\",\"manifests\":[";
        bool firstManifest = true;
        if (Base_) {
            for (const auto& manifest : Base_->CurrentManifests) {
                auto reader = Take(iceberg::ManifestReader::Make(manifest,
                    IO_,
                    Take(Base_->Metadata->Schema()),
                    Take(Base_->Metadata->PartitionSpec())));
                output << (firstManifest ? "" : ",") << "{\"path\":" << Quote(manifest.manifest_path)
                       << ",\"length\":" << manifest.manifest_length
                       << ",\"sequence_number\":" << manifest.sequence_number
                       << ",\"added_files\":" << manifest.added_files_count.value_or(-1) << ",\"entries\":[";
                bool firstEntry = true;
                for (const auto& entry : Take(reader->Entries())) {
                    Require(static_cast<bool>(entry.data_file), "Manifest dump contains no data file.");
                    output << (firstEntry ? "" : ",") << "{\"path\":" << Quote(entry.data_file->file_path)
                           << ",\"rows\":" << entry.data_file->record_count
                           << ",\"length\":" << entry.data_file->file_size_in_bytes
                           << ",\"snapshot_id\":" << entry.snapshot_id.value_or(-1) << '}';
                    firstEntry = false;
                }
                output << "]}";
                firstManifest = false;
            }
        }
        output << "]}\n";
        WriteArtifact(Options_.ReceiptDirectory / "manifest-dump.json", output.str());
    }
    std::string InventoryJson()
    {
        std::map<std::string, TFileInfo> files;
        if (Base_) {
            files = Inventory(*Base_);
        }
        std::map<std::string, TFileRecord> records;
        for (const auto& attempt : Attempts_) {
            for (auto record : attempt->Records()) {
                records.emplace(record.Path, std::move(record));
            }
        }
        std::ostringstream output;
        output << "{\"schema_version\":1,\"string_encoding\":\"latin1-bytes\",\"latest_root\":"
               << (Base_ ? Quote(Base_->MetadataLocation) : "null") << ",\"published_roots\":[";
        for (std::size_t index = 0; index < PublishedRoots_.size(); ++index) {
            output << (index ? "," : "") << Quote(PublishedRoots_[index]);
        }
        output << "],\"files\":[";
        std::set<std::string> reachable;
        bool first = true;
        for (const auto& [path, info] : files) {
            const auto record = records.find(path);
            const bool verified = record != records.end() && record->second.FormatVerified;
            Require(verified && record->second.Receipt.has_value(), "Reachable file lacks verified receipt.");
            reachable.insert(Physical(path));
            const auto bytes = Take(IO_->ReadFile(path, std::nullopt));
            TSha256 hash;
            Require(!hash.Update(std::as_bytes(std::span(bytes))), "Cannot hash file bytes.");
            Require(hash.Finalize() == record->second.Receipt->Sha256, "Receipt hash differs from actual bytes.");
            if (info.Kind == EFileKind::Metadata) {
                WriteArtifact(Options_.ReceiptDirectory / "root.metadata.json", bytes);
            } else if (info.Kind == EFileKind::Manifest || info.Kind == EFileKind::ManifestList) {
                WriteArtifact(Options_.ReceiptDirectory / TakeIO(TStoreUri::Parse(path, StoreID)).Leaf(), bytes);
            }
            output << (first ? "" : ",") << "{\"path\":" << Quote(path)
                   << ",\"physical_path\":" << Quote(Physical(path)) << ",\"kind\":" << Quote(KindName(info.Kind))
                   << ",\"length\":" << info.Length << ",\"attempt_id\":" << Quote(record->second.Receipt->AttemptId)
                   << ",\"format_verified\":true}";
            first = false;
        }
        output << "],\"orphans\":[";
        std::set<std::string> orphans;
        for (const auto& event : FS_->Events()) {
            if (event.Op == EIOOp::CreateExclusive && !event.Error && !reachable.contains(event.Path)) {
                orphans.insert(event.Path);
            }
        }
        first = true;
        for (const auto& path : orphans) {
            output << (first ? "" : ",") << Quote(path);
            first = false;
        }
        output << "]}\n";
        return output.str();
    }

public:
    explicit TFixture(const TOptions& options)
        : Options_(options)
    { }

    int Run()
    {
        std::filesystem::create_directories(Options_.ReceiptDirectory);
        const auto& name = Options_.Case;
        const bool publishes = name == "success" || name.ends_with("-short") || name == "write-eintr";
        std::string failure;
        std::string operationError;
        std::string inventory =
            "{\"schema_version\":1,\"latest_root\":null,\"published_roots\":[],\"files\":[],\"orphans\":[]}\n";
        try {
            if (name == "create-dir-sync") {
                Arm({EIOOp::SyncDir,
                    Options_.Root.parent_path().string(),
                    1,
                    1,
                    std::make_error_code(std::errc::io_error),
                    std::nullopt});
                try {
                    Create();
                } catch (const std::exception& error) {
                    operationError = error.what();
                }
                Require(!operationError.empty() && !Store_->Location && Latch_->IsSet(),
                    "Create fault was not rejected.");
            } else {
                Create();
                Append(1);
                if (name == "success" || name.starts_with("data-") || name.starts_with("manifest-") ||
                    name.starts_with("metadata-") || name.starts_with("write-"))
                {
                    if (name != "success") {
                        ArmPipeline();
                    }
                    try {
                        Append(2);
                        Actual_ = "published";
                    } catch (const std::exception& error) {
                        operationError = error.what();
                    }
                    Require(publishes == operationError.empty(),
                        operationError.empty() ? "Unexpected publication." : operationError);
                    if (!publishes) {
                        Require(static_cast<bool>(Attempt_->FirstError()), "Failed attempt lost first error.");
                    }
                } else {
                    Special();
                }
                Require(PublishedRoots_.size() == (publishes ? 3U : 2U), "Unexpected pointer mutation.");
            }
            Scope_.reset();
            Require(Store_->Location == (Base_ ? std::optional(Base_->MetadataLocation) : std::nullopt),
                "Catalog pointer differs from the last accepted publication.");
            CountHits();
            Require(name == "success" || InjectionHits_ > 0, "Injection did not execute.");
            inventory = InventoryJson();
            DumpManifests();
            CheckTrace();
        } catch (const std::exception& error) {
            failure = error.what();
        }
        Scope_.reset();
        std::filesystem::create_directories(Options_.ReceiptDirectory);
        WriteArtifact(Options_.ReceiptDirectory / "inventory.json", inventory);
        WriteArtifact(Options_.ReceiptDirectory / "io-events.jsonl", FS_->JsonLines());
        std::ostringstream result;
        result << "{\"schema_version\":1,\"string_encoding\":\"latin1-bytes\",\"case\":" << Quote(name)
               << ",\"status\":" << Quote(failure.empty() ? "PASS" : "FAIL")
               << ",\"message\":" << Quote(failure.empty() ? operationError : failure)
               << ",\"catalog\":\"in-memory-test-double\",\"expected_outcome\":"
               << Quote(publishes ? "published" : "rejected") << ",\"actual_outcome\":" << Quote(Actual_)
               << ",\"injection_hits\":" << InjectionHits_
               << ",\"first_error\":" << (Attempt_ ? Attempt_->FirstError().value() : Latch_->Error().value())
               << ",\"first_close\":" << (FirstClose_ ? Quote(*FirstClose_) : "null")
               << ",\"second_close\":" << (SecondClose_ ? Quote(*SecondClose_) : "null") << ",\"published_roots\":[";
        for (std::size_t index = 0; index < PublishedRoots_.size(); ++index) {
            result << (index ? "," : "") << Quote(PublishedRoots_[index]);
        }
        result << "],\"phases\":[";
        for (std::size_t index = 0; index < Phases_.size(); ++index) {
            result << (index ? "," : "") << "{\"name\":" << Quote(Phases_[index].first)
                   << ",\"seq\":" << Phases_[index].second << '}';
        }
        result << "]}\n";
        WriteArtifact(Options_.ReceiptDirectory / "result.json", result.str());
        if (!failure.empty()) {
            std::cerr << name << ": " << failure << '\n';
        }
        return failure.empty() ? 0 : 1;
    }
};

}

std::string Quote(std::string_view value)
{
    constexpr std::string_view hex = "0123456789abcdef";
    std::string result = "\"";
    for (const unsigned char byte : value) {
        if (byte == '"' || byte == '\\') {
            result += '\\';
            result += static_cast<char>(byte);
        } else if (byte < 0x20 || byte >= 0x7f) {
            result += "\\u00";
            result += hex[byte >> 4];
            result += hex[byte & 15];
        } else {
            result += static_cast<char>(byte);
        }
    }
    return result + '"';
}

std::vector<std::string> Cases()
{
    std::vector<std::string> cases{"success"};
    for (const auto* kind : {"data", "manifest", "manifest-list", "metadata"}) {
        for (const auto* op : {"write", "short", "sync-file", "close", "sync-dir"}) {
            cases.emplace_back(std::string(kind) + '-' + op);
        }
    }
    for (const auto* name : {"data-footer",
             "manifest-trailer",
             "manifest-list-trailer",
             "parquet-repeat-close",
             "avro-repeat-close",
             "avro-destructor",
             "outside-attempt",
             "exclusive-collision",
             "write-eintr",
             "write-zero",
             "create-dir-sync",
             "flush-mask"})
    {
        cases.emplace_back(name);
    }
    return cases;
}

int Run(const TOptions& options)
{
    return TFixture(options).Run();
}

}
