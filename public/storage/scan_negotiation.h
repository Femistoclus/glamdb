#pragma once

#include "storage/read_view.h"

namespace glamdb {

// IDs are one-based schema positions, stable for the fixed collection schema.
// Spec must match every attribute, including name, nullability and decimal scale.
struct TScanFieldReference
{
    int32_t FieldID = 0;
    FieldSpec Spec;
};

enum class EScanTranslation : uint8_t { Exact, NecessaryCondition };
enum class EScanPushdown : uint8_t { Exact, PruningOnly, Unsupported };
enum class EScanExecution : uint8_t { Serial, Split };
enum class EScanOrdering : uint8_t { Unordered, Ordered };
enum class EScanLimit : uint8_t { Absent, Pushed, Residual, Split, Ordered };
enum class EScanOrderSupport : uint8_t { NotRequested, Unsupported };

struct TScanExpression
{
    // Populated on output; retained copies share the metadata reservation.
    std::shared_ptr<const void> ResourceLease{};
    std::string ID;
    // Caller-owned immutable compute expression. Storage never interprets it.
    std::shared_ptr<const void> Expression;
    // Complete inputs needed to evaluate the original compute expression.
    std::vector<TScanFieldReference> Fields;
    std::optional<Predicate> Translated;
    // Caller asserts equivalence under the documented typed SQL semantics, or
    // original TRUE => translated TRUE. Storage cannot prove opaque semantics.
    EScanTranslation Translation = EScanTranslation::Exact;
};

struct TScanNegotiationRequest
{
    // Independent top-level AND conjuncts, never fragments extracted from OR.
    std::vector<TScanExpression> Conjuncts;
    // nullopt: all fields; empty: zero final columns. No duplicates.
    std::optional<std::vector<int32_t>> Projection;
    // Additional inputs for compute operators (for example ORDER BY).
    std::vector<TScanFieldReference> ComputeFields;
    std::optional<int64_t> Limit;
    int64_t BatchRows = 65536;
    EScanExecution Execution = EScanExecution::Serial;
    EScanOrdering Ordering = EScanOrdering::Unordered;
};

struct TScanExpressionDecision
{
    TScanExpression Original;
    EScanPushdown Pushdown = EScanPushdown::Unsupported;
    // A rejected translation is an opaque residual, with the binder's reason.
    std::optional<ErrorDetail> TranslationError;
};

struct TScanNegotiation
{
    std::shared_ptr<const void> ResourceLease{};
    TReadViewScanRequest Scan;
    std::vector<TScanExpressionDecision> Expressions;
    std::vector<std::string> ResidualIDs;
    // Output column order of Scan, including auxiliary residual inputs.
    std::vector<int32_t> StorageFieldIDs;
    // Indices into Scan output, applied AFTER residual filtering. Empty indices
    // mean zero columns with the surviving row count, never zero rows.
    std::vector<size_t> FinalProjection;
    std::optional<int64_t> ComputeLimit;
    EScanLimit Limit = EScanLimit::Absent;
    EScanOrderSupport Order = EScanOrderSupport::NotRequested;
};

// Pure planning; no I/O or readiness check. Metadata uses the supplied owner
// or the finite process default. Opaque expression payloads remain host-budgeted.
// At most 64 conjuncts, 128 field references per conjunct, 256 bytes per ID;
// aggregate measured translated predicate capacity <=16 MiB. Existing predicate
// limits also apply to the combined accepted filter.
// Unsupported translations remain residuals. Malformed request identities,
// mappings, enums, batch sizes and negative limits fail with INVALID_ARGUMENT.
// Requires the same fixed schema used for scan execution. Contract 1.3 advertises
// native support. No casts/UDFs/collations/ordering are evaluated here.
Result<TScanNegotiation> NegotiateScan(const SchemaSpec& schema,
    const TScanNegotiationRequest& request,
    std::shared_ptr<ResourceOwner> resources = {});

} // namespace glamdb
