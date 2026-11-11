#include "core/storage/scan_index.h"

#include <iceberg/manifest/manifest_entry.h>

#include <cstring>
#include <set>

namespace NGlamDB::NStorage {
namespace {

enum EMetric : std::uint8_t {
    NullCount = 1,
    NanCount = 2,
    Lower = 4,
    Upper = 8,
};

template <class T>
void Put(std::string& out, T value)
{
    char bytes[sizeof(T)];
    std::memcpy(bytes, &value, sizeof(T));
    out.append(bytes, sizeof(T));
}

template <class T>
bool Get(std::string_view& in, T& value)
{
    if (in.size() < sizeof(T)) {
        return false;
    }
    std::memcpy(&value, in.data(), sizeof(T));
    in.remove_prefix(sizeof(T));
    return true;
}

bool GetBytes(std::string_view& in, std::string_view& value)
{
    std::uint32_t size = 0;
    if (!Get(in, size) || in.size() < size) {
        return false;
    }
    value = in.substr(0, size);
    in.remove_prefix(size);
    return true;
}

} // namespace

std::string EncodeScanMetrics(const iceberg::DataFile& file)
{
    std::set<std::int32_t> fields;
    for (const auto& entry : file.null_value_counts) {
        fields.insert(entry.first);
    }
    for (const auto& entry : file.nan_value_counts) {
        fields.insert(entry.first);
    }
    for (const auto& entry : file.lower_bounds) {
        fields.insert(entry.first);
    }
    for (const auto& entry : file.upper_bounds) {
        fields.insert(entry.first);
    }
    std::string out;
    for (const auto field : fields) {
        if (const auto values = file.value_counts.find(field);
            values != file.value_counts.end() && values->second != file.record_count)
        {
            continue;
        }
        const auto nulls = file.null_value_counts.find(field);
        const auto nans = file.nan_value_counts.find(field);
        const auto lower = file.lower_bounds.find(field);
        const auto upper = file.upper_bounds.find(field);
        std::uint8_t mask = 0;
        mask |= nulls != file.null_value_counts.end() ? NullCount : 0;
        mask |= nans != file.nan_value_counts.end() ? NanCount : 0;
        mask |= lower != file.lower_bounds.end() ? Lower : 0;
        mask |= upper != file.upper_bounds.end() ? Upper : 0;
        Put(out, field);
        Put(out, mask);
        if (mask & NullCount) {
            Put(out, nulls->second);
        }
        if (mask & NanCount) {
            Put(out, nans->second);
        }
        for (const auto* bound : {mask & Lower ? &lower->second : nullptr, mask & Upper ? &upper->second : nullptr}) {
            if (bound) {
                Put(out, static_cast<std::uint32_t>(bound->size()));
                out.append(reinterpret_cast<const char*>(bound->data()), bound->size());
            }
        }
    }
    return out;
}

std::optional<TScanFieldMetrics> FindScanFieldMetrics(std::string_view metrics, std::int32_t fieldID)
{
    while (!metrics.empty()) {
        std::int32_t field = 0;
        std::uint8_t mask = 0;
        TScanFieldMetrics result;
        std::int64_t count = 0;
        std::string_view bound;
        if (!Get(metrics, field) || !Get(metrics, mask)) {
            return std::nullopt;
        }
        if (mask & NullCount) {
            if (!Get(metrics, count)) {
                return std::nullopt;
            }
            result.NullCount = count;
        }
        if (mask & NanCount) {
            if (!Get(metrics, count)) {
                return std::nullopt;
            }
            result.NanCount = count;
        }
        if (mask & Lower) {
            if (!GetBytes(metrics, bound)) {
                return std::nullopt;
            }
            result.Lower = bound;
        }
        if (mask & Upper) {
            if (!GetBytes(metrics, bound)) {
                return std::nullopt;
            }
            result.Upper = bound;
        }
        if (field == fieldID) {
            return result;
        }
    }
    return std::nullopt;
}

} // namespace NGlamDB::NStorage
