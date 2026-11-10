#include "tests/support/journal_fault_vfs.h"

#include "core/api/binary.h"

#include <mutex>
#include <unordered_map>

namespace glamdb::test {
namespace {
std::mutex registry_mutex;
std::unordered_map<sqlite3_vfs *, JournalFaultVfs *> registry;
std::atomic<int> next_id{0};
} // namespace

JournalFaultVfs::JournalFaultVfs()
    : parent_(sqlite3_vfs_find(nullptr)), name_("glamdb-journal-fault-" + std::to_string(++next_id)) {
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

JournalFaultVfs::~JournalFaultVfs() {
    sqlite3_vfs_unregister(&vfs_);
    std::lock_guard lock(registry_mutex);
    registry.erase(&vfs_);
}

void JournalFaultVfs::Arm(int journal_delete_number, bool after_unlink) {
    after_unlink_ = after_unlink;
    remaining_ = journal_delete_number;
}
void JournalFaultVfs::Disarm() {
    remaining_ = 0;
}

int JournalFaultVfs::Delete(sqlite3_vfs *vfs, const char *path, int sync_directory) {
    JournalFaultVfs *owner = nullptr;
    {
        std::lock_guard lock(registry_mutex);
        const auto found = registry.find(vfs);
        if (found == registry.end()) {
            return SQLITE_IOERR_DELETE;
        }
        owner = found->second;
    }
    const bool journal = path != nullptr && std::string_view(path).ends_with("-journal");
    const bool fail = journal && owner->remaining_.load() > 0 && owner->remaining_.fetch_sub(1) == 1;
    if (fail && !owner->after_unlink_) {
        return SQLITE_IOERR_DELETE;
    }
    const auto result = owner->parent_->xDelete(owner->parent_, path, sync_directory);
    return fail && result == SQLITE_OK ? SQLITE_IOERR_DIR_FSYNC : result;
}

} // namespace glamdb::test
