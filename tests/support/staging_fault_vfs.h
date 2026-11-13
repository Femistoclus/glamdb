#pragma once
#include <sqlite3.h>
#include <string>
namespace glamdb::test {
// Delegates real native I/O; injects SQLite's disk-full result at xWrite,
// or exits after an actual journal fsync. Never used in production.
class TStagingFaultVfs
{
public:
    TStagingFaultVfs();
    ~TStagingFaultVfs();
    void FullOnNextWrite()
    {
        Full_ = true;
    }
    void CrashAfterNextJournalSync()
    {
        Crash_ = true;
    }
    int Failures = 0;

private:
    struct TFile;
    sqlite3_vfs* Parent_;
    sqlite3_vfs Vfs_;
    std::string Name_;
    bool Full_ = false, Crash_ = false;
    static int Open(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);
    static int Write(sqlite3_file*, const void*, int, sqlite3_int64);
    static int Sync(sqlite3_file*, int);
};
}
