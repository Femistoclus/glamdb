#include "apps/transport/outcome_json.h"
#include "apps/transport/errors.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/codec.h"

#include <array>

namespace glamdb::json_adapter {
namespace {

using detail::Require;

std::string Quote(std::string_view value)
{
    Require(detail::ValidUtf8(value), "INVALID_ARGUMENT", "JSON string is not valid UTF-8");
    constexpr std::string_view hex = "0123456789abcdef";
    std::string result = "\"";
    for (const auto character : value) {
        const auto byte = static_cast<unsigned char>(character);
        if (byte == '"' || byte == '\\') {
            result.push_back('\\');
            result.push_back(character);
        } else if (byte < 32) {
            result += "\\u00";
            result.push_back(hex[byte >> 4]);
            result.push_back(hex[byte & 15]);
        } else {
            result.push_back(character);
        }
    }
    result.push_back('"');
    return result;
}

struct Object
{
    std::string bytes = "{";
    void Add(std::string_view key, std::string_view value)
    {
        if (bytes.size() > 1) {
            bytes.push_back(',');
        }
        bytes += Quote(key) + ":" + std::string(value);
    }
    void String(std::string_view key, std::string_view value)
    {
        Add(key, Quote(value));
    }
    void Integer(std::string_view key, int64_t value)
    {
        String(key, std::to_string(value));
    }
    void OptionalInteger(std::string_view key, std::optional<int64_t> value)
    {
        Add(key, value ? Quote(std::to_string(*value)) : "null");
    }
    void OptionalString(std::string_view key, const std::optional<std::string>& value)
    {
        Add(key, value ? Quote(*value) : "null");
    }
    std::string Finish() const
    {
        return bytes + "}";
    }
};

std::string_view Disposition(CommitDisposition disposition)
{
    switch (disposition) {
        case CommitDisposition::Committed:
            return "committed";
        case CommitDisposition::DefinitelyNotCommitted:
            return "definitely_not_committed";
        case CommitDisposition::CommitUnknown:
            return "commit_unknown";
    }
    throw detail::Failure({"INVALID_ARGUMENT", "invalid disposition", RetryHint::OperatorAction});
}

std::string_view Phase(CommitPhase phase)
{
    constexpr std::array<std::string_view, 10> phases{"validation",
        "binding",
        "data",
        "metadata",
        "catalog_transaction",
        "catalog_commit",
        "recording_failure",
        "reconciling",
        "complete",
        "invalid"};
    const auto value = static_cast<size_t>(phase);
    Require(value < 9, "INVALID_ARGUMENT", "invalid commit phase");
    return phases[value];
}

std::string Error(const ErrorDetail& error)
{
    constexpr std::array<std::string_view, 4> hints{"SameIdAfterReconcile",
        "NewOperationId",
        "FixInput",
        "OperatorAction"};
    Require(static_cast<size_t>(error.retry_hint) < hints.size(), "INVALID_ARGUMENT", "invalid retry hint");
    Object out;
    out.String("code", error.code);
    out.String("message", error.message);
    out.String("retry_hint", hints[static_cast<size_t>(error.retry_hint)]);
    return out.Finish();
}

std::string Base(const BaseIdentity& base)
{
    Object out;
    out.String("store_id", base.store_id);
    out.String("collection_id", base.collection_id);
    out.String("table_uuid", base.table_uuid);
    out.String("metadata_location", base.metadata_location);
    out.OptionalInteger("current_snapshot_id", base.current_snapshot_id);
    out.Add("schema_id", std::to_string(base.schema_id));
    out.Add("spec_id", std::to_string(base.spec_id));
    return out.Finish();
}

std::string Recovery(const RecoveryContext& context)
{
    Object out;
    out.String("store_id", context.store_id);
    out.String("collection_id", context.collection_id);
    out.String("operation_id", context.operation_id);
    out.String("payload_hash", context.payload_hash);
    out.Add("hash_version", std::to_string(context.hash_version));
    out.Add("base", Base(context.base));
    out.String("attempt_id", context.attempt_id);
    out.OptionalInteger("candidate_snapshot_id", context.candidate_snapshot_id);
    out.OptionalString("candidate_metadata_location", context.candidate_metadata_location);
    out.Add("first_error", Error(context.first_error));
    out.Add("publication_started", context.publication_started ? "true" : "false");
    out.Add("lookup_blocked", context.lookup_blocked ? "true" : "false");
    return out.Finish();
}

std::string Receipt(const AppendResult& result)
{
    Object out;
    out.String("collection_id", result.collection_id);
    out.String("operation_id", result.operation_id);
    out.String("payload_hash", result.payload_hash);
    out.Add("hash_version", std::to_string(result.hash_version));
    out.Integer("snapshot_id", result.snapshot_id);
    out.OptionalInteger("parent_snapshot_id", result.parent_snapshot_id);
    out.Integer("rows_appended", result.rows_appended);
    out.Integer("data_files_added", result.data_files_added);
    out.Integer("compressed_data_bytes_added", result.compressed_data_bytes_added);
    out.Integer("snapshot_created_at_us", result.snapshot_created_at_us);
    out.String("durability_profile", result.durability_profile);
    out.Add("receipt_version", std::to_string(result.receipt_version));
    return out.Finish();
}

std::string Collection(const CollectionInfo& collection)
{
    constexpr std::array<std::string_view, 5> types{"string", "bool", "int64", "float64", "timestamp_us_utc"};
    Object out;
    out.String("collection_id", collection.collection_id);
    out.String("name", collection.name);
    out.String("table_uuid", collection.table_uuid);
    out.String("mode", "append");
    out.Integer("creation_sequence", collection.creation_sequence);
    out.OptionalInteger("current_snapshot_id", collection.current_snapshot_id);
    std::string fields = "[";
    for (size_t i = 0; i < collection.schema.fields.size(); ++i) {
        if (i != 0) {
            fields += ',';
        }
        const auto& field = collection.schema.fields[i];
        const auto type = static_cast<size_t>(field.type);
        Require(type >= 1 && type <= types.size(), "INVALID_ARGUMENT", "invalid field type");
        Object item;
        item.Add("field_id", std::to_string(i + 1));
        item.String("name", field.name);
        item.String("type", types[type - 1]);
        item.Add("nullable", field.nullable ? "true" : "false");
        fields += item.Finish();
    }
    fields += ']';
    out.Add("schema", "{\"fields\":" + fields + "}");
    return out.Finish();
}

} // namespace

Result<Response> ToJson(const AppendOutcome& outcome)
{
    try {
        const auto valid = detail::ValidateOutcome(outcome);
        // Invalid caller IDs still need a structured validation error response.
        Require(valid.has_value() ||
                (outcome.disposition == CommitDisposition::DefinitelyNotCommitted &&
                    outcome.phase == CommitPhase::Validation && outcome.error && !outcome.result && !outcome.recovery),
            "INVALID_ARGUMENT",
            "invalid append outcome");
        if (outcome.disposition == CommitDisposition::DefinitelyNotCommitted && outcome.error &&
            outcome.error->code == "OPERATION_CONFLICT")
        {
            // The conflicting request changed nothing and says nothing about the
            // bound operation, which may be committed or unknown: a plain error,
            // never a terminal outcome of that operation ID.
            Object error;
            error.Add("error", Error(*outcome.error));
            return Response{NTransport::ErrorStatus(*outcome.error), error.Finish()};
        }
        Object out;
        out.String("outcome", Disposition(outcome.disposition));
        out.String("phase", Phase(outcome.phase));
        out.String("operation_id", outcome.operation_id);
        if (outcome.result) {
            out.Add("result", Receipt(*outcome.result));
        }
        if (outcome.error) {
            out.Add("error", Error(*outcome.error));
        }
        if (outcome.recovery) {
            out.Add("recovery_required", "true");
            out.Add("recovery", Recovery(*outcome.recovery));
        }
        const auto status = outcome.disposition == CommitDisposition::Committed ? 200
            : outcome.disposition == CommitDisposition::CommitUnknown           ? 503
                                                                      : NTransport::ErrorStatus(*outcome.error);
        return Response{status, out.Finish()};
    } catch (const detail::Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<Response> ToJson(const CreateOutcome& outcome)
{
    try {
        if (outcome.disposition == CommitDisposition::Committed) {
            if (!outcome.info.has_value()) {
                throw detail::Failure({"INVALID_ARGUMENT", "missing collection", RetryHint::FixInput});
            }
            Require(!outcome.error && !outcome.recovery && outcome.phase == CommitPhase::Complete,
                "INVALID_ARGUMENT",
                "invalid committed create outcome");
            return Response{200, Collection(outcome.info.value())};
        }
        Require(outcome.error && !outcome.info && outcome.phase >= CommitPhase::Validation &&
                outcome.phase < CommitPhase::Complete &&
                ((outcome.disposition == CommitDisposition::CommitUnknown && outcome.recovery.has_value() &&
                     outcome.phase != CommitPhase::Validation && outcome.phase != CommitPhase::Data &&
                     outcome.phase != CommitPhase::Metadata) ||
                    (outcome.disposition == CommitDisposition::DefinitelyNotCommitted && !outcome.recovery)),
            "INVALID_ARGUMENT",
            "invalid create outcome");
        Object out;
        out.String("outcome", Disposition(outcome.disposition));
        out.String("phase", Phase(outcome.phase));
        out.Add("error", Error(*outcome.error));
        if (outcome.recovery) {
            const auto& context = *outcome.recovery;
            Object recovery;
            recovery.String("store_id", context.store_id);
            recovery.String("collection_id", context.collection_id);
            recovery.String("exact_name", context.exact_name);
            recovery.String("schema_hash", context.schema_hash);
            recovery.String("attempt_id", context.attempt_id);
            recovery.OptionalString("candidate_table_uuid", context.candidate_table_uuid);
            recovery.OptionalString("candidate_metadata_location", context.candidate_metadata_location);
            recovery.Add("publication_started", context.publication_started ? "true" : "false");
            out.Add("recovery_required", "true");
            out.Add("recovery", recovery.Finish());
        }
        return Response{
            outcome.disposition == CommitDisposition::CommitUnknown ? 503 : NTransport::ErrorStatus(*outcome.error),
            out.Finish()};
    } catch (const detail::Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<std::string> CollectionJson(const CollectionInfo& collection)
{
    try {
        return Collection(collection);
    } catch (const detail::Failure& error) {
        return std::unexpected(error.detail);
    }
}

} // namespace glamdb::json_adapter
