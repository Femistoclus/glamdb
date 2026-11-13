#include "core/write_sessions/journal.h"

#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace glamdb::detail {
namespace {
constexpr int64_t EmergencyBytes = 64LL << 20;
bool AllocateEmergency(int fd)
{
#ifdef __APPLE__
    struct stat info
    { };
    if (fstat(fd, &info) != 0) {
        return false;
    }
    if (info.st_size < EmergencyBytes || info.st_blocks * 512 < EmergencyBytes) {
        fstore_t allocation{};
        allocation.fst_flags = F_ALLOCATEALL;
        allocation.fst_posmode = F_PEOFPOSMODE;
        allocation.fst_length = EmergencyBytes;
        if (fcntl(fd, F_PREALLOCATE, &allocation) != 0 || ftruncate(fd, EmergencyBytes) != 0) {
            return false;
        }
    }
    return fcntl(fd, F_FULLFSYNC) == 0;
#else
    return posix_fallocate(fd, 0, EmergencyBytes) == 0 && fsync(fd) == 0;
#endif
}
void SyncDirectory(const std::filesystem::path& path)
{
    const int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    Require(fd >= 0, "IO_ERROR", "cannot open staging directory for sync");
    const auto synced = fsync(fd);
    const auto closed = close(fd);
    Require(synced == 0 && closed == 0, "IO_ERROR", "cannot sync staging directory");
}
template <class T>
T ArrowTake(arrow::Result<T> result)
{
    Require(result.ok(), "INVALID_ARGUMENT", "invalid staged Arrow IPC");
    return std::move(*result);
}
void ArrowCheck(arrow::Status status)
{
    Require(status.ok(), "INVALID_ARGUMENT", "cannot encode staged Arrow IPC");
}
}

TWriteJournal::TWriteJournal(std::filesystem::path directory,
    std::shared_ptr<ResourceOwner> resources,
    const std::string& store,
    const std::string& scope,
    TWriteSessionLimits limits)
    : Limits(limits)
    , Directory_(std::move(directory))
    , Resources_(resources)
{
    try {
        Require(Limits.MaxSessions > 0 && Limits.MaxSessions <= 128 && Limits.MaxParts > 0 && Limits.MaxParts <= 4096 &&
                Limits.MaxBatches > 0 && Limits.MaxBatches <= 8192 && Limits.MaxSpoolBytes > 0 &&
                Limits.MaxSpoolBytes <= (256ULL << 20),
            "INVALID_ARGUMENT",
            "invalid staging limits");
        Require(Directory_.is_absolute() && Directory_.string().find('\0') == std::string::npos &&
                Directory_.lexically_normal() == std::filesystem::weakly_canonical(Directory_),
            "INVALID_ARGUMENT",
            "staging requires a canonical absolute directory without symlinks");
        const bool exists = std::filesystem::exists(Directory_);
        const uint64_t dbBytes = Limits.MaxSpoolBytes + (64ULL << 20);
        Reservations_.push_back(SessionTake(resources->Reserve(ResourceClass::Metadata, 16ULL << 20)));
        Reservations_.push_back(SessionTake(resources->Reserve(ResourceClass::Completion, 1ULL << 20)));
        // Database plus worst-case rollback journal plus physically allocated emergency file.
        const uint64_t envelope = 2 * dbBytes + (2ULL << 20) + EmergencyBytes;
        Reservations_.push_back(SessionTake(std::filesystem::exists(Directory_ / "staging.sqlite")
                ? resources->ReserveDevice(Directory_.string(), envelope, 8)
                : resources
                      ->ReserveStagingDevice(Directory_.string(), envelope, 8, (12ULL << 30) + (64ULL << 20), 512)));
        if (!exists) {
            Require(std::filesystem::create_directory(Directory_), "IO_ERROR", "cannot create staging directory");
            SyncDirectory(Directory_.parent_path());
        }
        Lock_ = open((Directory_ / "owner.lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
        Require(Lock_ >= 0, "IO_ERROR", "cannot open staging lock");
        Require(flock(Lock_, LOCK_EX | LOCK_NB) == 0, "STORE_LOCKED", "staging directory already owned");
        size_t entries = 0;
        for (const auto& entry : std::filesystem::directory_iterator(Directory_)) {
            const auto name = entry.path().filename().string();
            Require(++entries <= 4 && entry.is_regular_file() && !entry.is_symlink() &&
                    (name == "owner.lock" || name == "staging.sqlite" || name == "staging.sqlite-journal" ||
                        name == "completion.reserve"),
                "CORRUPTION",
                "unexpected staging artifact");
            if (name == "staging.sqlite" || name == "staging.sqlite-journal") {
                Require(entry.file_size() <= dbBytes + (name.ends_with("-journal") ? (2ULL << 20) : 0),
                    "RESOURCE_EXHAUSTED",
                    "staging artifact exceeds its bound");
            }
            if (name == "completion.reserve") {
                Require(entry.file_size() <= EmergencyBytes, "CORRUPTION", "invalid completion reserve");
            }
        }
        const bool initialized = std::filesystem::exists(Directory_ / "staging.sqlite");
        DB.Open(Directory_ / "staging.sqlite", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, {}, 32 << 20);
        DB.Execute("PRAGMA trusted_schema=OFF");
        DB.Execute("PRAGMA foreign_keys=ON");
        DB.Execute("PRAGMA journal_mode=PERSIST");
        DB.Execute("PRAGMA synchronous=EXTRA");
        DB.Execute("PRAGMA fullfsync=ON");
        DB.Execute("PRAGMA cache_size=-1024");
        DB.Execute("PRAGMA mmap_size=0");
        DB.Execute("PRAGMA temp_store=MEMORY");
        Require(DB.Integer("PRAGMA page_size") == 4096, "CORRUPTION", "unsupported staging page size");
        DB.Execute("PRAGMA max_page_count=" + std::to_string(dbBytes / 4096));
        Encoder binding;
        binding.Text("glamdb-staging-1");
        binding.Text(store);
        binding.Text(scope);
        binding.Number(Limits.MaxSessions);
        binding.Number(Limits.MaxParts);
        binding.Number(Limits.MaxBatches);
        binding.Number(Limits.MaxSpoolBytes);
        if (!initialized || DB.Integer("SELECT count(*) FROM sqlite_master WHERE type='table'") == 0) {
            Transaction([&] {
                DB.Execute("CREATE TABLE owner(binding BLOB NOT NULL)");
                DB.Execute("INSERT INTO owner VALUES(?)", {Blob{binding.bytes}});
                DB.Execute("CREATE TABLE sessions(id TEXT PRIMARY KEY, schema BLOB NOT NULL, mutation INTEGER NOT "
                           "NULL, precondition INTEGER NOT NULL, snapshot INTEGER NOT NULL, lifetime INTEGER NOT NULL, "
                           "expires INTEGER NOT NULL, state INTEGER NOT NULL DEFAULT 0, rows INTEGER NOT NULL DEFAULT "
                           "0, hash TEXT NOT NULL DEFAULT '', selection BLOB NOT NULL DEFAULT '')");
                DB.Execute("CREATE TABLE parts(session TEXT NOT NULL REFERENCES sessions(id), part TEXT NOT NULL, rows "
                           "INTEGER NOT NULL, hash TEXT NOT NULL, PRIMARY KEY(session,part))");
                DB.Execute("CREATE TABLE batches(session TEXT NOT NULL, part TEXT NOT NULL, ordinal INTEGER NOT NULL, "
                           "payload BLOB NOT NULL, PRIMARY KEY(session,part,ordinal))");
            });
        }
        Require(DB.Text("SELECT binding FROM owner") == binding.bytes,
            "IDENTITY_MISMATCH",
            "staging store, scope or limits differ");
        Require(DB.Integer("SELECT count(*) FROM sessions") <= Limits.MaxSessions &&
                DB.Integer("SELECT count(*) FROM parts") <= Limits.MaxParts &&
                DB.Integer("SELECT count(*) FROM batches") <= Limits.MaxBatches &&
                DB.Integer("SELECT coalesce(sum(length(payload)),0) FROM batches") <=
                    static_cast<int64_t>(Limits.MaxSpoolBytes),
            "CORRUPTION",
            "staging inventory exceeds limits");
        Require(DB.Text("PRAGMA quick_check(1)") == "ok", "CORRUPTION", "staging database integrity failure");
        Require(DB.Integer("SELECT count(*) FROM batches b LEFT JOIN parts p ON b.session=p.session AND b.part=p.part "
                           "WHERE p.session IS NULL") == 0,
            "CORRUPTION",
            "unacknowledged batch inventory survived recovery");
        // Allocate before acknowledging uploads. Failure still permits resolution/cleanup.
        const int reserve =
            open((Directory_ / "completion.reserve").c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (reserve >= 0) {
            ReserveAvailable_ = AllocateEmergency(reserve);
            if (close(reserve) != 0) {
                ReserveAvailable_ = false;
            }
        }
        SyncDirectory(Directory_);
    } catch (...) {
        try {
            DB.Close();
        } catch (...) {
        }
        if (Lock_ >= 0) {
            close(Lock_);
            Lock_ = -1;
        }
        throw;
    }
}
TWriteJournal::~TWriteJournal()
{
    // Close SQLite before releasing the directory lock.
    try {
        DB.Close();
    } catch (...) {
    }
    if (Lock_ >= 0) {
        close(Lock_);
    }
}
void TWriteJournal::ReleaseEmergency()
{
    Require(unlink((Directory_ / "completion.reserve").c_str()) == 0 || errno == ENOENT,
        "IO_ERROR",
        "cannot release completion reserve");
    ReserveAvailable_ = false;
    SyncDirectory(Directory_);
}
void TWriteJournal::Transaction(const std::function<void()>& action, bool control)
{
    for (int attempt = 0;; ++attempt) {
        try {
            DB.Execute("BEGIN IMMEDIATE");
            action();
            DB.Execute("COMMIT");
            return;
        } catch (const Failure& failure) {
            if (!DB.Autocommit()) {
                DB.Execute("ROLLBACK");
            }
            if (control && attempt == 0 && ReserveAvailable_ && (failure.native_code & 255) == SQLITE_FULL) {
                ReleaseEmergency();
                continue;
            }
            throw;
        } catch (...) {
            if (!DB.Autocommit()) {
                DB.Execute("ROLLBACK");
            }
            throw;
        }
    }
}
void TWriteJournal::RequireUploadCapacity()
{
    Require(ReserveAvailable_, "RESOURCE_EXHAUSTED", "completion reserve unavailable; cleanup and reopen staging");
    // Reopening restores accounted recovery inventory even under pressure. New
    // uploads still recheck shared publisher headroom while holding that envelope.
    (void)SessionTake(Resources_->ReserveStagingDevice(Directory_.string(), 0, 0, (12ULL << 30) + (64ULL << 20), 512));
}
std::string WriteKey(const TWriteIdentity& id)
{
    Require(ValidUuid(id.CollectionID) && ValidUuid(id.OperationID), "INVALID_ARGUMENT", "invalid write identity");
    return id.CollectionID + "/" + id.OperationID;
}
std::string PartKey(TWritePart part)
{
    // Numeric ordering is determined at seal, never by textual SQL ordering.
    return std::to_string(part.Task) + "/" + std::to_string(part.Attempt) + "/" + std::to_string(part.Part);
}
int64_t SessionNow()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}
std::string EncodeSessionBatch(const std::shared_ptr<arrow::RecordBatch>& batch)
{
    auto sink = ArrowTake(arrow::io::BufferOutputStream::Create());
    auto writer = ArrowTake(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
    ArrowCheck(writer->WriteRecordBatch(*batch));
    ArrowCheck(writer->Close());
    auto buffer = ArrowTake(sink->Finish());
    Require(buffer->size() <= (24LL << 20), "RESOURCE_EXHAUSTED", "staged IPC batch exceeds limit");
    return buffer->ToString();
}
std::shared_ptr<arrow::RecordBatch> DecodeSessionBatch(std::string bytes)
{
    auto buffer = arrow::Buffer::FromString(std::move(bytes));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader = ArrowTake(arrow::ipc::RecordBatchStreamReader::Open(input));
    auto batch = ArrowTake(reader->Next());
    Require(batch != nullptr && ArrowTake(reader->Next()) == nullptr, "CORRUPTION", "invalid staged batch framing");
    // IPC arrays retain the entire framed message as their parent buffer. Replay
    // must own only column buffers, matching the input admitted by PutPart, so
    // framing cannot push a valid near-limit batch over the retained-byte bound.
    return SessionTake(CopyRecordBatch(*batch, nullptr));
}
} // namespace glamdb::detail
