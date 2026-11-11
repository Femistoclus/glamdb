#pragma once

#include "storage/types.h"

#include <string_view>

namespace glamdb::NTransport {
struct TErrorPolicy
{
    int Status;
    RetryHint Hint;
};
// Both HTTP envelopes and durable outcome serialization use this policy.
// Core-provided retry hints remain authoritative; Hint supplies transport errors.
inline TErrorPolicy ErrorPolicy(std::string_view code)
{
    using enum RetryHint;
    if (code == "INVALID_ARGUMENT" || code == "CONFIG_TOO_SMALL") {
        return {400, FixInput};
    }
    if (code == "UNAUTHORIZED") {
        return {401, FixInput};
    }
    if (code == "NOT_FOUND" || code == "COLLECTION_NOT_FOUND" || code == "SNAPSHOT_NOT_FOUND" ||
        code == "DATABASE_NOT_FOUND" || code == "OPERATION_NOT_FOUND" || code == "ROUTE_NOT_FOUND")
    {
        return {404, FixInput};
    }
    if (code == "METHOD_NOT_ALLOWED") {
        return {405, FixInput};
    }
    if (code == "NOT_ACCEPTABLE") {
        return {406, FixInput};
    }
    if (code == "REQUEST_TIMEOUT") {
        return {408, FixInput};
    }
    if (code == "DATABASE_CLOSED" || code == "STATE_IN_USE" || code == "STORE_INCOMPLETE" || code == "STORE_MISSING" ||
        code == "STORE_IDENTITY_MISMATCH")
    {
        return {409, OperatorAction};
    }
    if (code == "DATABASE_REQUIRED" || code == "ALREADY_EXISTS" || code == "OPERATION_CONFLICT" ||
        code == "SCHEMA_MISMATCH" || code == "STALE_BASE")
    {
        return {409, FixInput};
    }
    if (code == "PRECONDITION_FAILED") {
        return {412, FixInput};
    }
    if (code == "PAYLOAD_TOO_LARGE" || code == "INPUT_TOO_LARGE") {
        return {413, FixInput};
    }
    if (code == "UNSUPPORTED_MEDIA_TYPE") {
        return {415, FixInput};
    }
    if (code == "EXPECTATION_FAILED") {
        return {417, FixInput};
    }
    if (code == "DATABASE_BUSY" || code == "RESOURCE_EXHAUSTED") {
        return {429, SameIdAfterReconcile};
    }
    if (code == "HEADERS_TOO_LARGE") {
        return {431, FixInput};
    }
    if (code == "UNSUPPORTED_OPERATION" || code == "UNSUPPORTED_TABLE_FEATURE" || code == "UNSUPPORTED_STORE_VERSION") {
        return {501, OperatorAction};
    }
    if (code == "RECOVERY_REQUIRED" || code == "OPERATION_IN_PROGRESS" || code == "CANCELLED" ||
        code == "INTERRUPTED" || code == "SERVER_BUSY")
    {
        return {503, SameIdAfterReconcile};
    }
    if (code == "IO_ERROR" || code == "DEPENDENCY_UNAVAILABLE") {
        return {503, OperatorAction};
    }
    if (code == "DEADLINE_EXCEEDED") {
        return {504, SameIdAfterReconcile};
    }
    if (code == "CAPACITY_EXCEEDED") {
        return {507, OperatorAction};
    }
    return {500, OperatorAction};
}
// Core RESOURCE_EXHAUSTED covers both bounded admission (busy writer, pending
// budget), where the same ID may later succeed, and exhausted capacity marked
// OperatorAction (free-space reserves, store/catalog limits, allocation), where
// no retry can. Capacity is 507 like CAPACITY_EXCEEDED, never a 429 retry signal.
inline int ErrorStatus(const ErrorDetail& error)
{
    if (error.code == "RESOURCE_EXHAUSTED" && error.retry_hint == RetryHint::OperatorAction) {
        return 507;
    }
    return ErrorPolicy(error.code).Status;
}
} // namespace glamdb::NTransport
