#pragma once

#include "core/scan/predicate.h"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <variant>

namespace NGlamDB::NStorage {
struct TScanFieldMetrics;
} // namespace NGlamDB::NStorage

namespace parquet {
class RowGroupMetaData;
} // namespace parquet

namespace glamdb::detail {

// A bound value in the predicate's domain: strings, bools, int64 (also UTC
// timestamps in microseconds) and finite doubles.
using TStatValue = std::variant<std::string, bool, int64_t, double>;

// What metadata proves about one field over a set of rows (a data file or a
// row group). Bounds cover every non-NULL value; anything unknown is empty.
struct TFieldStats
{
    int64_t Rows = 0;
    std::optional<int64_t> Nulls{};
    std::optional<TStatValue> Lower{};
    std::optional<TStatValue> Upper{};
};

// False only when the statistics prove that the predicate's root is not TRUE
// for any row under SQL three-valued logic; the exact predicate must still be
// applied to rows that are read. `fields` is ordered as ReferencedFields.
// NOT is never reasoned about: a NOT subtree may be TRUE. AND may not be TRUE
// when any child may not be; OR only when no child may be.
bool MayMatch(const TBoundPredicate& predicate, std::span<const TFieldStats> fields);

// Statistics of one field of a data file with `rows` records from its manifest
// metrics (nullptr: none). String upper bounds that may have been truncated
// (at least `truncateLength` code points) are ignored; float bounds are used
// only with a known zero NaN count.
TFieldStats FileFieldStats(const NGlamDB::NStorage::TScanFieldMetrics* metrics,
    int64_t rows,
    FieldType type,
    int32_t truncateLength);

// Statistics of one column chunk from a verified Parquet footer. Only exact,
// correctly ordered min/max values are used; float bounds additionally need
// `floatsWithoutNaN` (the file's manifest NaN count is zero).
TFieldStats
RowGroupFieldStats(const parquet::RowGroupMetaData& group, int column, FieldType type, bool floatsWithoutNaN);

// Process-wide switch of file and row-group pruning (on by default), for
// comparison runs and tests. Cursors read it when they open.
void SetScanPruning(bool enabled);
bool ScanPruningEnabled();

} // namespace glamdb::detail
