#include "core/backend/storage_errors.h"

#include "core/storage/iceberg_error_boundary.h"

#include <string>

namespace glamdb::detail {
namespace {

bool IsPersistedDecodeKind(iceberg::ErrorKind kind)
{
    switch (kind) {
        case iceberg::ErrorKind::kInvalid:
        case iceberg::ErrorKind::kInvalidArgument:
        case iceberg::ErrorKind::kNotFound:
        case iceberg::ErrorKind::kValidationFailed:
        case iceberg::ErrorKind::kDecompressError:
        case iceberg::ErrorKind::kUnknownError:
        case iceberg::ErrorKind::kInvalidManifest:
        case iceberg::ErrorKind::kInvalidManifestList:
        case iceberg::ErrorKind::kInvalidSchema:
        case iceberg::ErrorKind::kJsonParseError:
            return true;
        default:
            return false;
    }
}

RetryHint HintFor(std::string_view code)
{
    return code == "INVALID_ARGUMENT" ? RetryHint::FixInput : RetryHint::OperatorAction;
}

} // namespace

std::string_view ClassifyFsError(std::error_code error, EStoragePath path)
{
    if (error == std::errc::operation_canceled) {
        return "CANCELLED";
    }
    if (error == std::errc::timed_out) {
        return "DEADLINE_EXCEEDED";
    }
    if (error == std::errc::state_not_recoverable || error == std::errc::too_many_symbolic_link_levels ||
        error == std::errc::not_a_directory || error == std::errc::cross_device_link ||
        error == std::errc::no_such_file_or_directory)
    {
        return "CORRUPTION";
    }
    if (error == std::errc::invalid_argument) {
        // Preflight: a symlink or special file in the warehouse tree. Read: a
        // committed file that is not a regular file.
        return path == EStoragePath::Write ? "IO_ERROR" : "CORRUPTION";
    }
    if (error == std::errc::file_too_large) {
        return path == EStoragePath::Read ? "CORRUPTION" : "RESOURCE_EXHAUSTED";
    }
    if (error == std::errc::no_space_on_device || error == std::errc::value_too_large ||
        error == std::errc::not_enough_memory)
    {
        return "RESOURCE_EXHAUSTED";
    }
    return "IO_ERROR";
}

ErrorDetail StorageError(std::error_code error, EStoragePath path, std::string_view context)
{
    const auto code = ClassifyFsError(error, path);
    return {std::string(code), BoundedDiagnostic(std::string(context) + ": " + error.message()), HintFor(code)};
}

ErrorDetail
StorageError(const iceberg::Error& error, std::string_view context, EStoragePath path, std::string_view fallback)
{
    const std::string_view message(error.message);
    std::string_view code = fallback;
    const bool capacity = message.starts_with(NGlamDB::NStorage::CapacityPrefix);
    const bool resource = message.starts_with(NGlamDB::NStorage::ResourcePrefix);
    if (message.starts_with(NGlamDB::NStorage::CorruptionPrefix)) {
        code = "CORRUPTION";
    } else if (path == EStoragePath::Read && !resource) {
        // An I/O error reports the environment (EIO, EMFILE, EACCES, ...),
        // not persisted state: it must not close readiness. Retained state
        // that no longer fits its limits cannot be served either: it fails
        // closed like any other invariant violation.
        if (error.kind == iceberg::ErrorKind::kIOError) {
            code = "IO_ERROR";
        } else if (IsPersistedDecodeKind(error.kind) || capacity) {
            code = "CORRUPTION";
        }
    } else if (capacity || resource) {
        code = "RESOURCE_EXHAUSTED";
    }
    return {std::string(code), BoundedDiagnostic(std::string(context) + ": " + error.message), HintFor(code)};
}

} // namespace glamdb::detail
