#pragma once

// A test SQLite VFS that pauses one rollback-journal existence check.

#include "core/api/binary.h"

#include <sqlite3.h>

#include <atomic>
#include <future>
#include <string>
#include <string_view>

namespace glamdb {

// Real SQLite VFS delegation. When armed, the next existence check of a
// rollback journal blocks until released. In DELETE journal mode SQLite makes
// that check at the start of every read transaction (hot-journal probe), so
// arming it right before an append pauses the writer inside its
// authoritative FindOperation, after it acquired the writer lock.
class AccessGateVfs
{
public:
    AccessGateVfs()
        : parent_(sqlite3_vfs_find(nullptr))
    {
        detail::Require(parent_ != nullptr, "INTERNAL", "default VFS is missing");
        // Retain the parent's pAppData: its other callbacks use it.
        vfs_ = *parent_;
        vfs_.pNext = nullptr;
        vfs_.zName = name_.c_str();
        vfs_.xAccess = &AccessGateVfs::Access;
        instance_.store(this);
        detail::Require(sqlite3_vfs_register(&vfs_, 0) == SQLITE_OK, "INTERNAL", "cannot register gate VFS");
    }

    ~AccessGateVfs()
    {
        Release();
        sqlite3_vfs_unregister(&vfs_);
        instance_.store(nullptr);
    }

    AccessGateVfs(const AccessGateVfs&) = delete;
    AccessGateVfs& operator=(const AccessGateVfs&) = delete;

    const std::string& Name() const
    {
        return name_;
    }

    void Arm()
    {
        armed_.store(true);
    }

    std::shared_future<void> Entered() const
    {
        return entered_future_;
    }

    void Release()
    {
        if (!released_.exchange(true)) {
            release_.set_value();
        }
    }

private:
    static int Access(sqlite3_vfs*, const char* path, int flags, int* result)
    {
        auto* self = instance_.load();
        if (self != nullptr && path != nullptr && std::string_view(path).ends_with("-journal") &&
            self->armed_.exchange(false))
        {
            self->entered_.set_value();
            self->release_future_.wait();
        }
        sqlite3_vfs* parent = self != nullptr ? self->parent_ : sqlite3_vfs_find(nullptr);
        return parent->xAccess(parent, path, flags, result);
    }

    static inline std::atomic<AccessGateVfs*> instance_{nullptr};

    std::string name_ = "glamdb-access-gate";
    sqlite3_vfs* parent_ = nullptr;
    sqlite3_vfs vfs_{};
    std::atomic<bool> armed_{false};
    std::atomic<bool> released_{false};
    std::promise<void> entered_;
    std::shared_future<void> entered_future_ = entered_.get_future().share();
    std::promise<void> release_;
    std::shared_future<void> release_future_ = release_.get_future().share();
};

// Releases the gate before any pending future is joined, so a failed
// assertion cannot deadlock the test on a paused writer.
struct ReleaseOnExit
{
    AccessGateVfs& vfs;
    ~ReleaseOnExit()
    {
        vfs.Release();
    }
};

} // namespace glamdb
