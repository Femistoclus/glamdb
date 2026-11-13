#include "tests/support/staging_fault_vfs.h"
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>
namespace glamdb::test {
struct TStagingFaultVfs::TFile
{
    sqlite3_file Base{};
    TStagingFaultVfs* Owner;
    bool Stage, Journal;
    sqlite3_file* Inner()
    {
        return reinterpret_cast<sqlite3_file*>(this + 1);
    }
    static TFile& File(sqlite3_file* file)
    {
        return *reinterpret_cast<TFile*>(file);
    }
};
int TStagingFaultVfs::Write(sqlite3_file* raw, const void* data, int count, sqlite3_int64 offset)
{
    auto& f = TFile::File(raw);
    if (f.Stage && f.Owner->Full_) {
        f.Owner->Full_ = false;
        ++f.Owner->Failures;
        return SQLITE_FULL;
    }
    return f.Inner()->pMethods->xWrite(f.Inner(), data, count, offset);
}
int TStagingFaultVfs::Sync(sqlite3_file* raw, int flags)
{
    auto& f = TFile::File(raw);
    const auto result = f.Inner()->pMethods->xSync(f.Inner(), flags);
    if (result == SQLITE_OK && f.Journal && f.Owner->Crash_) {
        std::_Exit(73);
    }
    return result;
}
int TStagingFaultVfs::Open(sqlite3_vfs* vfs, const char* name, sqlite3_file* raw, int flags, int* output)
{
    auto& owner = *static_cast<TStagingFaultVfs*>(vfs->pAppData);
    auto* f = new (raw) TFile;
    f->Owner = &owner;
    const std::string_view path(name ? name : "");
    f->Stage = path.ends_with("staging.sqlite") || path.ends_with("staging.sqlite-journal");
    f->Journal = path.ends_with("staging.sqlite-journal");
    auto result = owner.Parent_->xOpen(owner.Parent_, name, f->Inner(), flags, output);
    if (result != SQLITE_OK) {
        return result;
    }
    static const sqlite3_io_methods Methods = [] {
        sqlite3_io_methods m{};
        m.iVersion = 1;
        m.xClose = [](sqlite3_file* p) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xClose(f);
        };
        m.xRead = [](sqlite3_file* p, void* b, int n, sqlite3_int64 o) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xRead(f, b, n, o);
        };
        m.xWrite = Write;
        m.xTruncate = [](sqlite3_file* p, sqlite3_int64 n) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xTruncate(f, n);
        };
        m.xSync = Sync;
        m.xFileSize = [](sqlite3_file* p, sqlite3_int64* n) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xFileSize(f, n);
        };
        m.xLock = [](sqlite3_file* p, int n) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xLock(f, n);
        };
        m.xUnlock = [](sqlite3_file* p, int n) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xUnlock(f, n);
        };
        m.xCheckReservedLock = [](sqlite3_file* p, int* n) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xCheckReservedLock(f, n);
        };
        m.xFileControl = [](sqlite3_file* p, int n, void* b) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xFileControl(f, n, b);
        };
        m.xSectorSize = [](sqlite3_file* p) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xSectorSize(f);
        };
        m.xDeviceCharacteristics = [](sqlite3_file* p) {
            auto* f = TFile::File(p).Inner();
            return f->pMethods->xDeviceCharacteristics(f);
        };
        return m;
    }();
    f->Base.pMethods = &Methods;
    return SQLITE_OK;
}
TStagingFaultVfs::TStagingFaultVfs()
    : Parent_(sqlite3_vfs_find(nullptr))
    , Vfs_(*Parent_)
{
    static std::atomic<int> sequence{0};
    Name_ = "staging-fault-" + std::to_string(++sequence);
    Vfs_.zName = Name_.c_str();
    Vfs_.pAppData = this;
    Vfs_.xOpen = Open;
    Vfs_.szOsFile = sizeof(TFile) + Parent_->szOsFile;
    if (sqlite3_vfs_register(&Vfs_, 1) != SQLITE_OK) {
        std::abort();
    }
}
TStagingFaultVfs::~TStagingFaultVfs()
{
    sqlite3_vfs_register(Parent_, 1);
    sqlite3_vfs_unregister(&Vfs_);
}
}
