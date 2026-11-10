#pragma once

#include "storage/types.h"

#include <filesystem>

namespace glamdb::detail {

class StoreLease
{
public:
    static std::shared_ptr<StoreLease> Create(const std::filesystem::path& root, const StoreInfo& info);
    static std::shared_ptr<StoreLease> Open(const std::filesystem::path& root, const StoreInfo& info);
    ~StoreLease();
    StoreLease(const StoreLease&) = delete;
    StoreLease& operator=(const StoreLease&) = delete;
    void MarkReady(const StoreInfo& info);
    const std::filesystem::path& Root() const
    {
        return root_;
    }

private:
    explicit StoreLease(std::filesystem::path root);
    void Lock();
    void WriteMarker(const char* name, std::string_view contents);
    std::string ReadMarker(const char* name);
    void SyncDirectories();
    std::filesystem::path root_;
    int descriptor_ = -1;
};

std::string NewUuid();

} // namespace glamdb::detail
