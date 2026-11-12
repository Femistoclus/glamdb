#include "storage/scan_negotiation.h"

#include "core/api/canonical.h"
#include "core/scan/predicate.h"
#include "storage/resources.h"

#include <algorithm>
#include <array>
#include <new>
#include <numeric>
#include <unordered_set>

namespace glamdb {
namespace {
ErrorDetail Invalid(std::string message)
{
    return {"INVALID_ARGUMENT", std::move(message)};
}

bool ValidFieldID(int32_t id, const SchemaSpec& schema)
{
    return id > 0 && static_cast<size_t>(id) <= schema.fields.size();
}

// Both temporary binding work and returned descriptors share this admission.
struct TAdmission
{
    std::shared_ptr<ResourceReservation> Descriptors;
    std::shared_ptr<ResourceReservation> Predicates;
};

Predicate Conjunction(const std::vector<Predicate>& predicates)
{
    if (predicates.empty()) {
        return Predicate::True();
    }
    return predicates.size() == 1 ? predicates.front() : Predicate::And(predicates);
}
} // namespace

Result<TScanNegotiation> NegotiateScan(const SchemaSpec& schema,
    const TScanNegotiationRequest& request,
    std::shared_ptr<ResourceOwner> resources)
{
    try {
        if (request.Conjuncts.size() > 64 || request.ComputeFields.size() > MaxSchemaFields ||
            (request.Projection && request.Projection->size() > MaxSchemaFields) ||
            (request.Limit && *request.Limit < 0) || request.BatchRows < 1 || request.BatchRows > 65536 ||
            (request.Execution != EScanExecution::Serial && request.Execution != EScanExecution::Split) ||
            (request.Ordering != EScanOrdering::Unordered && request.Ordering != EScanOrdering::Ordered))
        {
            return std::unexpected(Invalid("invalid scan negotiation request"));
        }
        if (!resources) {
            resources = ResourceOwner::ProcessDefault();
        }
        auto descriptors = resources->Reserve(ResourceClass::Metadata, 4ULL << 20);
        if (!descriptors) {
            return std::unexpected(descriptors.error());
        }
        auto admission = std::make_shared<TAdmission>();
        admission->Descriptors = std::move(*descriptors);
        // Canonical validation also bounds schema names before any result copy.
        const auto encodedSchema = detail::EncodeSchema(schema);
        if (!encodedSchema) {
            return std::unexpected(encodedSchema.error());
        }
        std::unordered_set<std::string> ids;
        uint64_t predicateBytes = 0;
        for (const auto& expression : request.Conjuncts) {
            if (expression.ID.empty() || expression.ID.size() > 256 || !expression.Expression ||
                expression.Fields.size() > MaxSchemaFields || !ids.insert(expression.ID).second ||
                (expression.Translation != EScanTranslation::Exact &&
                    expression.Translation != EScanTranslation::NecessaryCondition))
            {
                return std::unexpected(Invalid("invalid or duplicate expression identity or translation kind"));
            }
            std::array<bool, MaxSchemaFields> seen{};
            for (const auto& field : expression.Fields) {
                if (!ValidFieldID(field.FieldID, schema) ||
                    field.Spec != schema.fields[static_cast<size_t>(field.FieldID - 1)] ||
                    seen[static_cast<size_t>(field.FieldID - 1)])
                {
                    return std::unexpected(Invalid("invalid, duplicate or mismatched expression field identity"));
                }
                seen[static_cast<size_t>(field.FieldID - 1)] = true;
            }
            if (expression.Translated) {
                const auto bytes = detail::TPredicateResources::Measure(*expression.Translated);
                if (!bytes) {
                    return std::unexpected(bytes.error());
                }
                constexpr uint64_t MaxBytes = 16ULL << 20;
                if (*bytes > MaxBytes - predicateBytes) {
                    return std::unexpected(Invalid("translated predicates exceed 16 MiB"));
                }
                predicateBytes += *bytes;
            }
        }
        auto predicates = resources->Reserve(ResourceClass::Metadata, 4 * predicateBytes);
        if (!predicates) {
            return std::unexpected(predicates.error());
        }
        admission->Predicates = std::move(*predicates);
        TScanNegotiation result;
        result.ResourceLease = admission;
        result.Scan.BatchRows = request.BatchRows;
        if (request.Projection) {
            std::array<bool, MaxSchemaFields> seen{};
            for (const auto id : *request.Projection) {
                if (!ValidFieldID(id, schema) || seen[static_cast<size_t>(id - 1)]) {
                    return std::unexpected(Invalid("invalid or duplicate projection field identity"));
                }
                seen[static_cast<size_t>(id - 1)] = true;
                result.StorageFieldIDs.push_back(id);
            }
        } else {
            for (size_t i = 0; i < schema.fields.size(); ++i) {
                result.StorageFieldIDs.push_back(static_cast<int32_t>(i + 1));
            }
        }
        result.FinalProjection.resize(result.StorageFieldIDs.size());
        std::iota(result.FinalProjection.begin(), result.FinalProjection.end(), size_t{0});
        std::array<bool, MaxSchemaFields> computeSeen{};
        for (const auto& field : request.ComputeFields) {
            if (!ValidFieldID(field.FieldID, schema) ||
                field.Spec != schema.fields[static_cast<size_t>(field.FieldID - 1)] ||
                computeSeen[static_cast<size_t>(field.FieldID - 1)])
            {
                return std::unexpected(Invalid("invalid, duplicate or mismatched compute field identity"));
            }
            computeSeen[static_cast<size_t>(field.FieldID - 1)] = true;
            if (std::ranges::find(result.StorageFieldIDs, field.FieldID) == result.StorageFieldIDs.end()) {
                result.StorageFieldIDs.push_back(field.FieldID);
            }
        }
        std::vector<Predicate> accepted;
        for (const auto& expression : request.Conjuncts) {
            TScanExpressionDecision decision;
            decision.Original = expression;
            decision.Original.ResourceLease = admission;
            if (expression.Translated) {
                decision.Original.Translated = detail::TPredicateResources::Retain(*expression.Translated, admission);
                const auto bound = detail::BindPredicate(*expression.Translated, schema);
                if (!bound) {
                    decision.TranslationError = bound.error();
                } else {
                    for (const auto index : bound->ReferencedFields) {
                        if (std::ranges::none_of(expression.Fields, [index](const auto& field) {
                                return field.FieldID == static_cast<int32_t>(index + 1);
                            }))
                        {
                            decision.TranslationError = Invalid("translated field is absent from expression inputs");
                            break;
                        }
                    }
                    if (!decision.TranslationError) {
                        accepted.push_back(*decision.Original.Translated);
                        // Keep the aggregate within the scanner's existing complexity limits.
                        const auto combined = detail::BindPredicate(Conjunction(accepted), schema);
                        if (!combined) {
                            accepted.pop_back();
                            decision.TranslationError = combined.error();
                        } else {
                            decision.Pushdown = expression.Translation == EScanTranslation::Exact
                                ? EScanPushdown::Exact
                                : EScanPushdown::PruningOnly;
                        }
                    }
                }
            }
            if (decision.Pushdown != EScanPushdown::Exact) {
                result.ResidualIDs.push_back(expression.ID);
                for (const auto& field : expression.Fields) {
                    if (std::ranges::find(result.StorageFieldIDs, field.FieldID) == result.StorageFieldIDs.end()) {
                        result.StorageFieldIDs.push_back(field.FieldID);
                    }
                }
            }
            result.Expressions.push_back(std::move(decision));
        }
        result.Scan.Filter = detail::TPredicateResources::Retain(Conjunction(accepted), admission);
        result.Scan.Projection.emplace();
        for (const auto id : result.StorageFieldIDs) {
            result.Scan.Projection->push_back(schema.fields[static_cast<size_t>(id - 1)].name);
        }
        if (request.Ordering == EScanOrdering::Ordered) {
            result.Order = EScanOrderSupport::Unsupported;
        }
        if (request.Limit) {
            if (request.Ordering == EScanOrdering::Ordered) {
                result.Limit = EScanLimit::Ordered;
            } else if (request.Execution == EScanExecution::Split) {
                result.Limit = EScanLimit::Split;
            } else if (!result.ResidualIDs.empty()) {
                result.Limit = EScanLimit::Residual;
            } else {
                result.Limit = EScanLimit::Pushed;
                result.Scan.Limit = request.Limit;
            }
            if (result.Limit != EScanLimit::Pushed) {
                result.ComputeLimit = request.Limit;
            }
        }
        return result;
    } catch (const std::bad_alloc&) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", "scan negotiation allocation failed", RetryHint::OperatorAction});
    }
}
} // namespace glamdb
