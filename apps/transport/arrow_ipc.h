#pragma once

#include "apps/transport/http.h"

namespace glamdb::NTransport {
// Caller retains its transport reservation. A null batch emits schema and EOS.
// No response is returned unless the complete stream fits the byte limit.
TResponse EncodeArrowPage(const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::RecordBatch>& batch,
    SnapshotSelector snapshot,
    size_t byteLimit = MaxResponseBytes);
} // namespace glamdb::NTransport
