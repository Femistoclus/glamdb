#include "apps/server/bulk.h"
#include "apps/transport/bulk_upload.h"
#include "apps/transport/outcome_json.h"

namespace glamdb::NServer {
using namespace NTransport;
namespace {
class TBulkSource final : public BulkAppendSource
{
public:
    TBulkSource(std::shared_ptr<TBulkUpload> upload, SchemaSpec schema)
        : Upload_(std::move(upload))
        , Schema_(std::move(schema))
    { }
    Status Rewind() override
    {
        try {
            Upload_->Rewind();
            Require(Upload_->ReadLine(MaxBulkHeaderBytes).has_value(), "bulk header required");
            return {};
        } catch (const TFailure& error) {
            return std::unexpected(error.Detail);
        }
    }
    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        try {
            const auto line = Upload_->ReadLine();
            if (!line) {
                return nullptr;
            }
            TJsonRows rows;
            const auto body = ParseJson(*line, rows);
            CheckKeys(body, {"rows"});
            Require(rows.Present, "bulk batch requires rows");
            return ParseRows(Schema_, rows);
        } catch (const TFailure& error) {
            return std::unexpected(error.Detail);
        }
    }

private:
    std::shared_ptr<TBulkUpload> Upload_;
    SchemaSpec Schema_;
};
} // namespace

TJson ReadBulkHeader(const TRequest& request)
{
    Require(static_cast<bool>(request.BulkUpload), "bulk upload requires a complete streaming body");
    request.BulkUpload->Rewind();
    const auto line = request.BulkUpload->ReadLine(MaxBulkHeaderBytes);
    if (!line) {
        Fail("bulk header required");
    }
    auto header = ParseJson(*line);
    CheckKeys(header, {"operation_id", "rows", "expected_snapshot"});
    Require(header.contains("operation_id") && header["operation_id"].is_string() && header.contains("rows"),
        "bulk header requires operation_id and declared rows");
    return header;
}

TResponse BulkAppend(Engine& engine, const std::string& collection, const TRequest& request, const TJson& header)
{
    BulkAppendRequest append;
    append.collection_id = collection;
    append.operation_id = header.at("operation_id").get<std::string>();
    append.rows = ParseInt64(header.at("rows"));
    if (header.contains("expected_snapshot")) {
        append.expected_snapshot = header["expected_snapshot"].is_null()
            ? SnapshotPrecondition{SnapshotPreconditionKind::NoSnapshot, 0}
            : SnapshotPrecondition{SnapshotPreconditionKind::Equals, ParseInt64(header["expected_snapshot"])};
    }
    append.source = std::make_shared<TBulkSource>(request.BulkUpload, Unwrap(engine.SchemaForAppend(collection)));
    auto response = Unwrap(json_adapter::ToJson(engine.BulkAppendDurable(append)));
    return {response.status, std::move(response.body)};
}
} // namespace glamdb::NServer
