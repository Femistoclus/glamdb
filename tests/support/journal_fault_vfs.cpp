#include "tests/support/journal_fault_vfs.h"

#include "core/api/binary.h"

#include <mutex>
#include <unordered_map>

namespace glamdb::test {
namespace {
std::mutex registry_mutex;
std::unordered_map<sqlite3_vfs*, JournalFaultVfs*> registry;
std::atomic<int> next_id{0};
} // namespace

JournalFaultVfs::JournalFaultVfs()
    : parent_(sqlite3_vfs_find(nullptr))
    , name_("glamdb-journal-fault-" + std::to_string(++next_id))
{
    detail::Require(parent_ != nullptr, "INTERNAL", "default VFS is missing");
    // Retain the original pAppData: the parent's other callbacks use it.
    vfs_ = *parent_;
    vfs_.pNext = nullptr;
    vfs_.zName = name_.c_str();
    vfs_.xDelete = Delete;
    {
        std::lock_guard lock(registry_mutex);
        registry[&vfs_] = this;
    }
    detail::Require(sqlite3_vfs_register(&vfs_, 0) == SQLITE_OK, "INTERNAL", "cannot register fault VFS");
}

JournalFaultVfs::~JournalFaultVfs()
{
    sqlite3_vfs_unregister(&vfs_);
    std::lock_guard lock(registry_mutex);
    registry.erase(&vfs_);
}

void JournalFaultVfs::Arm(int journal_delete_number, bool after_unlink)
{
    std::lock_guard lock(control_mutex_);
    after_unlink_ = after_unlink;
    remaining_ = journal_delete_number;
}
void JournalFaultVfs::Disarm()
{
    std::lock_guard lock(control_mutex_);
    remaining_ = 0;
}

void JournalFaultVfs::PauseBeforeDelete(int journal_delete_number)
{
    std::lock_guard lock(control_mutex_);
    paused_ = 0;
    released_ = false;
    pause_remaining_ = journal_delete_number;
}
bool JournalFaultVfs::WaitUntilPaused(std::chrono::milliseconds timeout, int count)
{
    std::unique_lock lock(control_mutex_);
    return pause_changed_.wait_for(lock, timeout, [&] {
        return paused_ >= count;
    });
}
void JournalFaultVfs::Release()
{
    std::lock_guard lock(control_mutex_);
    released_ = true;
    pause_changed_.notify_all();
}

int JournalFaultVfs::Delete(sqlite3_vfs* vfs, const char* path, int sync_directory)
{
    JournalFaultVfs* owner = nullptr;
    {
        std::lock_guard lock(registry_mutex);
        const auto found = registry.find(vfs);
        if (found == registry.end()) {
            return SQLITE_IOERR_DELETE;
        }
        owner = found->second;
    }
    const bool journal = path != nullptr && std::string_view(path).ends_with("-journal");
    bool fail = false, after_unlink = false;
    if (journal) {
        std::unique_lock lock(owner->control_mutex_);
        const bool pause = owner->pause_remaining_ == 1 || owner->pause_remaining_ < 0;
        if (owner->pause_remaining_ > 0) {
            --owner->pause_remaining_;
        } else if (owner->pause_remaining_ < 0) {
            ++owner->pause_remaining_;
        }
        if (pause) {
            ++owner->paused_;
            owner->pause_changed_.notify_all();
            // Waiting releases the control mutex so a test can arm/disarm
            // this deletion before releasing it.
            owner->pause_changed_.wait(lock, [&] {
                return owner->released_;
            });
        }
        fail = owner->remaining_ == 1;
        if (owner->remaining_ > 0) {
            --owner->remaining_;
        }
        after_unlink = owner->after_unlink_;
    }
    if (fail && !after_unlink) {
        owner->failures_.fetch_add(1, std::memory_order_relaxed);
        return SQLITE_IOERR_DELETE;
    }
    const auto result = owner->parent_->xDelete(owner->parent_, path, sync_directory);
    if (fail && result == SQLITE_OK) {
        owner->failures_.fetch_add(1, std::memory_order_relaxed);
        return SQLITE_IOERR_DIR_FSYNC;
    }
    return result;
}

} // namespace glamdb::test
