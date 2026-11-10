#include "core/api/codec.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"

namespace glamdb::detail {
namespace {

void WriteResult(Encoder &out, const AppendResult &result) {
    out.Text(result.collection_id);
    out.Text(result.operation_id);
    out.Text(result.payload_hash);
    out.Number(result.hash_version);
    out.Number(result.snapshot_id);
    out.OptionalNumber(result.parent_snapshot_id);
    out.Number(result.rows_appended);
    out.Number(result.data_files_added);
    out.Number(result.compressed_data_bytes_added);
    out.Number(result.snapshot_created_at_us);
    out.Text(result.durability_profile);
    out.Number(result.receipt_version);
}

AppendResult ReadResult(Decoder &in) {
    AppendResult result;
    result.collection_id = in.Text();
    result.operation_id = in.Text();
    result.payload_hash = in.Text();
    result.hash_version = in.Number<int32_t>();
    result.snapshot_id = in.Number<int64_t>();
    result.parent_snapshot_id = in.OptionalNumber();
    result.rows_appended = in.Number<int64_t>();
    result.data_files_added = in.Number<int64_t>();
    result.compressed_data_bytes_added = in.Number<int64_t>();
    result.snapshot_created_at_us = in.Number<int64_t>();
    result.durability_profile = in.Text();
    result.receipt_version = in.Number<int32_t>();
    return result;
}

} // namespace

Status ValidateOutcome(const AppendOutcome &outcome) {
    const auto invalid = [] {
        return std::unexpected(ErrorDetail{"CORRUPTION", "invalid append outcome", RetryHint::OperatorAction});
    };
    const auto valid_error = [](const ErrorDetail &error) {
        return !error.code.empty() && error.code.size() <= 128 && error.message.size() <= 2048 &&
               ValidUtf8(error.code) && ValidUtf8(error.message) &&
               error.retry_hint >= RetryHint::SameIdAfterReconcile && error.retry_hint <= RetryHint::OperatorAction;
    };
    if (!ValidUuid(outcome.operation_id) || outcome.phase < CommitPhase::Validation ||
        outcome.phase > CommitPhase::Complete) {
        return invalid();
    }
    if (outcome.disposition == CommitDisposition::Committed) {
        if (outcome.phase != CommitPhase::Complete || !outcome.result || outcome.error || outcome.recovery) {
            return invalid();
        }
        const auto &result = *outcome.result;
        if (!ValidUuid(result.collection_id) || result.operation_id != outcome.operation_id ||
            !ValidDigest(result.payload_hash) || result.hash_version != 1 || result.receipt_version != 1 ||
            result.rows_appended < 1 || result.rows_appended > 10000 || result.data_files_added != 1 ||
            result.compressed_data_bytes_added < 1 || result.compressed_data_bytes_added > 32LL * 1024 * 1024 ||
            result.snapshot_created_at_us < -62135596800000000LL ||
            result.snapshot_created_at_us > 253402300799999999LL || result.snapshot_created_at_us % 1000 != 0 ||
            result.durability_profile.empty() || result.durability_profile.size() > 128 ||
            !ValidUtf8(result.durability_profile) || result.parent_snapshot_id == result.snapshot_id) {
            return invalid();
        }
    } else {
        if (outcome.result || !outcome.error || outcome.phase == CommitPhase::Complete ||
            outcome.phase > CommitPhase::Reconciling || outcome.error->code.empty() ||
            outcome.error->code.size() > 128 || outcome.error->message.size() > 2048 ||
            !ValidUtf8(outcome.error->code) || !ValidUtf8(outcome.error->message) || !valid_error(*outcome.error)) {
            return invalid();
        }
        if (outcome.disposition == CommitDisposition::DefinitelyNotCommitted) {
            if (outcome.recovery) {
                return invalid();
            }
        } else if (outcome.disposition == CommitDisposition::CommitUnknown) {
            if (!outcome.recovery || outcome.phase == CommitPhase::Validation || outcome.phase == CommitPhase::Data ||
                outcome.phase == CommitPhase::Metadata) {
                return invalid();
            }
            const auto &context = *outcome.recovery;
            const auto &base = context.base;
            const auto prefix = "store://" + context.store_id + "/warehouse/";
            if (!ValidUuid(context.store_id) || !ValidUuid(context.collection_id) ||
                context.operation_id != outcome.operation_id || !ValidDigest(context.payload_hash) ||
                context.hash_version != 1 || !ValidUuid(context.attempt_id) || base.store_id != context.store_id ||
                base.collection_id != context.collection_id || !ValidUuid(base.table_uuid) ||
                !base.metadata_location.starts_with(prefix) || base.metadata_location.size() <= prefix.size() ||
                base.metadata_location.size() > 2048 || !ValidUtf8(base.metadata_location) || base.schema_id < 0 ||
                base.spec_id < 0 || !valid_error(context.first_error) ||
                (context.candidate_metadata_location && (!context.candidate_metadata_location->starts_with(prefix) ||
                                                         context.candidate_metadata_location->size() > 2048 ||
                                                         !ValidUtf8(*context.candidate_metadata_location))) ||
                (context.lookup_blocked && (context.publication_started || context.candidate_snapshot_id ||
                                            context.candidate_metadata_location))) {
                return invalid();
            }
        } else {
            return invalid();
        }
    }
    return {};
}

Result<std::string> EncodeOutcome(const AppendOutcome &outcome) {
    const auto valid = ValidateOutcome(outcome);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    if (outcome.disposition == CommitDisposition::CommitUnknown) {
        return std::unexpected(
            ErrorDetail{"INVALID_ARGUMENT", "unknown is not a terminal outcome", RetryHint::OperatorAction});
    }
    Encoder out;
    out.Number<uint16_t>(1);
    out.Text(outcome.operation_id);
    out.Number(static_cast<uint8_t>(outcome.disposition));
    out.Number(static_cast<uint8_t>(outcome.phase));
    if (outcome.result) {
        WriteResult(out, *outcome.result);
    } else {
        out.Text(outcome.error->code);
        out.Text(outcome.error->message);
        out.Number(static_cast<uint8_t>(outcome.error->retry_hint));
    }
    if (out.bytes.size() > 4096) {
        return std::unexpected(ErrorDetail{"RESOURCE_EXHAUSTED", "receipt exceeds 4 KiB", RetryHint::OperatorAction});
    }
    return out.bytes;
}

Result<AppendOutcome> DecodeOutcome(std::string_view bytes) {
    try {
        Require(bytes.size() <= 4096, "CORRUPTION", "oversized outcome");
        Decoder in{bytes};
        Require(in.Number<uint16_t>() == 1, "UNSUPPORTED_STORE_VERSION", "unknown receipt encoding");
        AppendOutcome outcome;
        outcome.operation_id = in.Text();
        outcome.disposition = static_cast<CommitDisposition>(in.Number<uint8_t>());
        outcome.phase = static_cast<CommitPhase>(in.Number<uint8_t>());
        if (outcome.disposition == CommitDisposition::Committed) {
            outcome.result = ReadResult(in);
        } else {
            outcome.error = ErrorDetail{in.Text(), in.Text(), static_cast<RetryHint>(in.Number<uint8_t>())};
        }
        in.End();
        const auto valid = ValidateOutcome(outcome);
        if (!valid || outcome.disposition == CommitDisposition::CommitUnknown) {
            return std::unexpected(
                valid ? ErrorDetail{"CORRUPTION", "persisted unknown outcome", RetryHint::OperatorAction}
                      : valid.error());
        }
        return outcome;
    } catch (const Failure &error) {
        return std::unexpected(error.detail);
    }
}

std::string EncodeBase(const BaseIdentity &base) {
    Encoder out;
    out.Number<uint16_t>(1);
    out.Text(base.store_id);
    out.Text(base.collection_id);
    out.Text(base.table_uuid);
    out.Text(base.metadata_location);
    out.OptionalNumber(base.current_snapshot_id);
    out.Number(base.schema_id);
    out.Number(base.spec_id);
    return out.bytes;
}

Result<BaseIdentity> DecodeBase(std::string_view bytes) {
    try {
        Require(bytes.size() <= 4096, "CORRUPTION", "oversized base identity");
        Decoder in{bytes};
        Require(in.Number<uint16_t>() == 1, "UNSUPPORTED_STORE_VERSION", "unknown base identity version");
        BaseIdentity base;
        base.store_id = in.Text();
        base.collection_id = in.Text();
        base.table_uuid = in.Text();
        base.metadata_location = in.Text();
        base.current_snapshot_id = in.OptionalNumber();
        base.schema_id = in.Number<int32_t>();
        base.spec_id = in.Number<int32_t>();
        in.End();
        Require(ValidUuid(base.store_id) && ValidUuid(base.collection_id) && ValidUuid(base.table_uuid) &&
                    !base.metadata_location.empty() && base.metadata_location.size() <= 2048 && base.schema_id >= 0 &&
                    base.spec_id >= 0,
                "CORRUPTION", "invalid base identity");
        return base;
    } catch (const Failure &error) {
        return std::unexpected(error.detail);
    }
}

} // namespace glamdb::detail
