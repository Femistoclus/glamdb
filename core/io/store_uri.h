#pragma once

#include "core/io/local_fs.h"

#include <cstddef>
#include <string>
#include <string_view>

namespace NGlamDB::NIO {

bool IsCanonicalUuid(std::string_view value) noexcept;

class TStoreUri
{
public:
    static TResult<TStoreUri> Parse(std::string_view value, std::string_view expectedStoreId);
    static TResult<TStoreUri> ParseTable(std::string_view value, std::string_view expectedStoreId);

    const std::string& String() const noexcept;
    std::string_view StoreId() const noexcept;
    std::string_view CollectionId() const noexcept;
    std::string_view Directory() const noexcept;
    std::string_view Leaf() const noexcept;
    std::string_view RelativePath() const noexcept;
    EFileKind Kind() const noexcept;
    bool IsTable() const noexcept;

private:
    std::string Value_;
    EFileKind Kind_;
    std::size_t LeafOffset_;

    TStoreUri(std::string_view value, EFileKind kind, std::size_t leafOffset);
    static TResult<TStoreUri> ParseImpl(std::string_view value, std::string_view expectedStoreId, bool table);
};

}
