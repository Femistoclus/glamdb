#include "apps/transport/arrow_ipc.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <openssl/evp.h>

namespace glamdb::NTransport {
namespace {
void WriteStream(arrow::io::OutputStream& sink,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::RecordBatch>& batch)
{
    auto writer = arrow::ipc::MakeStreamWriter(&sink, schema);
    Require(writer.ok(), "cannot start Arrow scan page", "INTERNAL");
    if (batch) {
        Require((*writer)->WriteRecordBatch(*batch).ok(), "cannot write Arrow scan page", "INTERNAL");
    }
    Require((*writer)->Close().ok(), "cannot finish Arrow scan page", "INTERNAL");
}

std::string Checksum(std::string_view bytes)
{
    unsigned char digest[EVP_MAX_MD_SIZE]{};
    unsigned int size = 0;
    Require(EVP_Digest(bytes.data(), bytes.size(), digest, &size, EVP_sha256(), nullptr) == 1 && size == 32,
        "cannot checksum Arrow response",
        "INTERNAL");
    constexpr char Hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (unsigned int i = 0; i < size; ++i) {
        result += Hex[digest[i] >> 4];
        result += Hex[digest[i] & 15];
    }
    return result;
}
} // namespace

TResponse EncodeArrowPage(const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::RecordBatch>& batch,
    SnapshotSelector snapshot,
    size_t byteLimit)
{
    Require(schema != nullptr, "missing Arrow output schema", "INTERNAL");
    Require(snapshot.kind == SnapshotSelectorKind::Id || snapshot.kind == SnapshotSelectorKind::InitialEmpty,
        "Arrow page requires a resolved snapshot",
        "INTERNAL");
    Require(!batch || (batch->schema()->Equals(*schema, true) && batch->Validate().ok()),
        "invalid Arrow output batch",
        "INTERNAL");

    arrow::io::MockOutputStream measure;
    WriteStream(measure, schema, batch);
    const auto size = measure.GetExtentBytesWritten();
    Require(size >= 0 && static_cast<uint64_t>(size) <= std::min(byteLimit, MaxResponseBytes),
        "Arrow response exceeds transport limit",
        "INPUT_TOO_LARGE");

    std::string bytes(static_cast<size_t>(size), '\0');
    auto buffer = std::make_shared<arrow::MutableBuffer>(reinterpret_cast<uint8_t*>(bytes.data()), size);
    arrow::io::FixedSizeBufferWriter sink(buffer);
    WriteStream(sink, schema, batch);
    const auto written = sink.Tell();
    Require(written.ok() && *written == size && sink.Close().ok(), "incomplete Arrow scan page", "INTERNAL");

    TResponse response{200, std::move(bytes), "application/vnd.apache.arrow.stream"};
    response.Headers["X-GlamDB-Snapshot-Id"] =
        snapshot.kind == SnapshotSelectorKind::Id ? std::to_string(snapshot.snapshot_id) : "initial";
    response.Headers["X-Content-SHA256"] = Checksum(response.Body);
    return response;
}
} // namespace glamdb::NTransport
