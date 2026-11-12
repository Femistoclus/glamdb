#include "core/storage/append_stager.h"
#include "core/storage/format_registry.h"

#include "core/io/io_attempt.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/metadata_validation.h"

#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/merge_append.h>
#include <iceberg/util/macros.h>

#include <set>
#include <span>

namespace NGlamDB::NStorage {
using iceberg::Error;

namespace {

iceberg::Status CheckData(const iceberg::Table& table, const iceberg::DataFile& dataFile, NIO::TIOAttempt& attempt)
{
    if (auto error = attempt.CheckWrite(dataFile.file_path)) {
        return iceberg::IOError("Append attempt failed: {}", error.message());
    }
    const auto record = attempt.Record(dataFile.file_path);
    if (!record) {
        return iceberg::InvalidArgument("Data file is not registered in this attempt");
    }
    const auto& entry = record.Value();
    const auto& receipt = entry.Receipt;
    if (entry.Kind != NIO::EFileKind::Data || !receipt || !entry.FormatVerified ||
        entry.State != NIO::EFileState::DirSynced || receipt->Path != dataFile.file_path ||
        receipt->AttemptId != attempt.Id() || dataFile.file_size_in_bytes <= 0 || dataFile.record_count <= 0 ||
        receipt->Length != static_cast<std::uint64_t>(dataFile.file_size_in_bytes) ||
        dataFile.content != iceberg::DataFile::Content::kData ||
        dataFile.file_format != iceberg::FileFormatType::kParquet)
    {
        return iceberg::InvalidArgument("Append requires a verified data receipt matching its path and length");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto spec, table.spec());
    if (table.metadata()->format_version != 2 || !spec->IsUnpartitioned() ||
        dataFile.partition_spec_id != spec->spec_id() ||
        table.location() != "store://" + attempt.StoreId() + "/warehouse/" + attempt.CollectionId())
    {
        return iceberg::InvalidArgument("Append requires the current unpartitioned Iceberg v2 spec");
    }
    return {};
}

iceberg::Result<TStagedAppend> CollectStage(std::shared_ptr<iceberg::Transaction> transaction,
    const NIO::TIOAttempt& attempt)
{
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, transaction->current().Snapshot());
    ICEBERG_RETURN_UNEXPECTED(CheckSnapshotTimestamp(*snapshot));
    const auto list = attempt.Record(snapshot->manifest_list);
    if (!list || list.Value().Kind != NIO::EFileKind::ManifestList || !list.Value().Receipt ||
        list.Value().State != NIO::EFileState::DirSynced)
    {
        return iceberg::IOError("Staged manifest list has no durable receipt in this attempt");
    }
    TStagedAppend staged{std::move(transaction),
        snapshot->snapshot_id,
        snapshot->parent_snapshot_id,
        snapshot->manifest_list,
        {}};
    for (const auto& record : attempt.Records()) {
        if (record.Kind == NIO::EFileKind::Manifest) {
            if (!record.Receipt || record.State != NIO::EFileState::DirSynced) {
                return iceberg::IOError("Staged manifest has no durable receipt");
            }
            staged.NewManifestPaths.push_back(record.Path);
        }
    }
    if (staged.NewManifestPaths.empty()) {
        return iceberg::IOError("Append produced no durable manifest");
    }
    return staged;
}

iceberg::Result<TStagedAppend> StageFiles(std::shared_ptr<iceberg::Table> table,
    std::span<const std::shared_ptr<iceberg::DataFile>> dataFiles,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    bool bulk)
{
    auto result = CatchIcebergExceptions([&]() -> iceberg::Result<TStagedAppend> {
        if (!table || !attempt || dataFiles.empty() || dataFiles.size() > (bulk ? 256U : 1U)) {
            return iceberg::InvalidArgument("Append requires a table, bounded data files and active attempt");
        }
        const auto marker = summary.find("glamdb.bulk-version");
        if ((bulk && (marker == summary.end() || marker->second != "1")) || (!bulk && marker != summary.end())) {
            return iceberg::InvalidArgument("Append summary does not match its bulk profile");
        }
        std::set<std::string_view> paths;
        for (const auto& dataFile : dataFiles) {
            if (!dataFile || dataFile->record_count > (bulk ? 100000 : 10000) ||
                dataFile->file_size_in_bytes > (32LL << 20) || !paths.insert(dataFile->file_path).second)
            {
                return iceberg::InvalidArgument("Append has invalid, duplicate or oversized data files");
            }
            ICEBERG_RETURN_UNEXPECTED(CheckData(*table, *dataFile, *attempt));
        }
        RegisterStorageFormats();
        ICEBERG_ASSIGN_OR_RAISE(auto transaction,
            iceberg::Transaction::Make(std::move(table), iceberg::TransactionKind::kUpdate));
        ICEBERG_ASSIGN_OR_RAISE(auto append, transaction->NewMergeAppend());
        for (const auto& dataFile : dataFiles) {
            append->AppendFile(dataFile);
        }
        for (const auto& [key, value] : summary) {
            append->Set(key, value);
        }
        const auto committed = append->Commit();
        if (auto error = attempt->FirstError()) {
            return iceberg::IOError("Append attempt failed: {}", error.message());
        }
        ICEBERG_RETURN_UNEXPECTED(committed);
        return CollectStage(std::move(transaction), *attempt);
    });
    if (!result && attempt) {
        attempt->FailFile({},
            std::make_error_code(result.error().kind == iceberg::ErrorKind::kInvalidArgument
                    ? std::errc::invalid_argument
                    : std::errc::io_error));
    }
    return result;
}

}

iceberg::Result<TStagedAppend> StageAppend(std::shared_ptr<iceberg::Table> table,
    const std::shared_ptr<iceberg::DataFile>& dataFile,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt)
{
    return StageFiles(std::move(table), std::span(&dataFile, 1), summary, attempt, false);
}

iceberg::Result<TStagedAppend> StageBulkAppend(std::shared_ptr<iceberg::Table> table,
    const std::vector<std::shared_ptr<iceberg::DataFile>>& dataFiles,
    const std::unordered_map<std::string, std::string>& summary,
    const std::shared_ptr<NIO::TIOAttempt>& attempt)
{
    return StageFiles(std::move(table), dataFiles, summary, attempt, true);
}

}
