#include "core/io/store_uri.h"

#include <algorithm>
#include <charconv>
#include <cstdint>

namespace NGlamDB::NIO {
namespace {

constexpr std::string_view KScheme = "store://";
constexpr std::string_view KWarehouse = "/warehouse/";
constexpr std::size_t KUuidSize = 36;
constexpr auto KCollectionOffset = KScheme.size() + KUuidSize + KWarehouse.size();
constexpr auto KTableSize = KCollectionOffset + KUuidSize;

bool Digits(std::string_view value)
{
    return !value.empty() && std::ranges::all_of(value, [](char c) {
        return c >= '0' && c <= '9';
    });
}

EFileKind ClassifyMetadata(std::string_view leaf)
{
    if (leaf.ends_with(".metadata.json")) {
        leaf.remove_suffix(std::string_view(".metadata.json").size());
        const auto separator = leaf.find('-');
        if (separator >= 5 && separator != std::string_view::npos && Digits(leaf.substr(0, separator)) &&
            IsCanonicalUuid(leaf.substr(separator + 1)))
        {
            return EFileKind::Metadata;
        }
        return EFileKind::Other;
    }
    if (!leaf.ends_with(".avro")) {
        return EFileKind::Other;
    }
    leaf.remove_suffix(5);
    if (leaf.size() > KUuidSize + 2 && IsCanonicalUuid(leaf.substr(0, KUuidSize)) &&
        leaf.substr(KUuidSize, 2) == "-m" && Digits(leaf.substr(KUuidSize + 2)))
    {
        return EFileKind::Manifest;
    }
    if (!leaf.starts_with("snap-") || leaf.size() < 5 + 1 + 1 + 1 + 1 + KUuidSize) {
        return EFileKind::Other;
    }
    const auto uuidSeparator = leaf.size() - KUuidSize - 1;
    if (leaf[uuidSeparator] != '-' || !IsCanonicalUuid(leaf.substr(uuidSeparator + 1))) {
        return EFileKind::Other;
    }
    const auto numbers = leaf.substr(5, uuidSeparator - 5);
    const auto separator = numbers.rfind('-');
    if (separator == std::string_view::npos || !Digits(numbers.substr(separator + 1))) {
        return EFileKind::Other;
    }
    const auto snapshot = numbers.substr(0, separator);
    std::int64_t snapshotId = 0;
    const auto result = std::from_chars(snapshot.data(), snapshot.data() + snapshot.size(), snapshotId);
    return result.ec == std::errc{} && result.ptr == snapshot.data() + snapshot.size() ? EFileKind::ManifestList
                                                                                       : EFileKind::Other;
}

}

bool IsCanonicalUuid(std::string_view value) noexcept
{
    if (value.size() != KUuidSize) {
        return false;
    }
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (value[i] != '-') {
                return false;
            }
        } else if (!((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f'))) {
            return false;
        }
    }
    return true;
}

TStoreUri::TStoreUri(std::string_view value, EFileKind kind, std::size_t leafOffset)
    : Value_(value)
    , Kind_(kind)
    , LeafOffset_(leafOffset)
{ }

TResult<TStoreUri> TStoreUri::Parse(std::string_view value, std::string_view expectedStoreId)
{
    return ParseImpl(value, expectedStoreId, false);
}

TResult<TStoreUri> TStoreUri::ParseTable(std::string_view value, std::string_view expectedStoreId)
{
    return ParseImpl(value, expectedStoreId, true);
}

TResult<TStoreUri> TStoreUri::ParseImpl(std::string_view value, std::string_view expectedStoreId, bool table)
{
    if (!IsCanonicalUuid(expectedStoreId) || value.size() < KTableSize || !value.starts_with(KScheme) ||
        value.find_first_of("%\\?#") != std::string_view::npos || value.find('\0') != std::string_view::npos)
    {
        return std::errc::invalid_argument;
    }
    const auto storeId = value.substr(KScheme.size(), KUuidSize);
    if (!IsCanonicalUuid(storeId) || value.substr(KScheme.size() + KUuidSize, KWarehouse.size()) != KWarehouse ||
        !IsCanonicalUuid(value.substr(KCollectionOffset, KUuidSize)))
    {
        return std::errc::invalid_argument;
    }
    if (storeId != expectedStoreId) {
        return std::errc::permission_denied;
    }
    if (table) {
        if (value.size() != KTableSize) {
            return std::errc::invalid_argument;
        }
        return TStoreUri(value, EFileKind::Other, value.size());
    }
    if (value.size() <= KTableSize || value[KTableSize] != '/') {
        return std::errc::invalid_argument;
    }
    const auto directoryEnd = value.find('/', KTableSize + 1);
    if (directoryEnd == std::string_view::npos) {
        return std::errc::invalid_argument;
    }
    const auto directory = value.substr(KTableSize + 1, directoryEnd - KTableSize - 1);
    const auto leaf = value.substr(directoryEnd + 1);
    if (leaf.empty() || leaf == "." || leaf == ".." || leaf.contains('/')) {
        return std::errc::invalid_argument;
    }
    const auto kind = directory == "metadata"                                  ? ClassifyMetadata(leaf)
        : directory == "data" && leaf.size() > 8 && leaf.ends_with(".parquet") ? EFileKind::Data
                                                                               : EFileKind::Other;
    if (kind == EFileKind::Other) {
        return std::errc::invalid_argument;
    }
    return TStoreUri(value, kind, directoryEnd + 1);
}

std::string_view TStoreUri::StoreId() const noexcept
{
    return {Value_.data() + KScheme.size(), KUuidSize};
}

std::string_view TStoreUri::CollectionId() const noexcept
{
    return {Value_.data() + KCollectionOffset, KUuidSize};
}

std::string_view TStoreUri::Directory() const noexcept
{
    if (IsTable()) {
        return {};
    }
    return {Value_.data() + KTableSize + 1, LeafOffset_ - KTableSize - 2};
}

std::string_view TStoreUri::Leaf() const noexcept
{
    return {Value_.data() + LeafOffset_, Value_.size() - LeafOffset_};
}

std::string_view TStoreUri::RelativePath() const noexcept
{
    const auto offset = KScheme.size() + KUuidSize + 1;
    return {Value_.data() + offset, Value_.size() - offset};
}

const std::string& TStoreUri::String() const noexcept
{
    return Value_;
}

EFileKind TStoreUri::Kind() const noexcept
{
    return Kind_;
}

bool TStoreUri::IsTable() const noexcept
{
    return Kind_ == EFileKind::Other;
}

}
