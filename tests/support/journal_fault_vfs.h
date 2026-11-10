#pragma once

#include <atomic>
#include <sqlite3.h>
#include <string>

namespace glamdb::test {

// Real SQLite VFS delegation; fail before or after an actual rollback-journal
// unlink. This models a lost COMMIT result, not loss of the OS page cache.
class JournalFaultVfs
{
public:
    JournalFaultVfs();
    ~JournalFaultVfs();
    JournalFaultVfs(const JournalFaultVfs&) = delete;
    JournalFaultVfs& operator=(const JournalFaultVfs&) = delete;
    void Arm(int journal_delete_number, bool after_unlink);
    void Disarm();
    const std::string& Name() const
    {
        return name_;
    }

private:
    static int Delete(sqlite3_vfs* vfs, const char* path, int sync_directory);
    sqlite3_vfs* parent_ = nullptr;
    sqlite3_vfs vfs_{};
    std::string name_;
    std::atomic<int> remaining_{0};
    bool after_unlink_ = false;
};

} // namespace glamdb::test
