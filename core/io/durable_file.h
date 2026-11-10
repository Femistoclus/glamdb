#pragma once

#include "core/io/io_attempt.h"
#include "core/io/local_fs.h"
#include "core/io/sha256.h"
#include "core/io/write_latch.h"

#include <mutex>
#include <optional>

namespace NGlamDB::NIO {

class TDurableFile
{
public:
    static TResult<std::shared_ptr<TDurableFile>> Create(std::shared_ptr<ILocalFS> fs,
        THandle directory,
        std::string path,
        std::shared_ptr<TWriteLatch> latch,
        std::shared_ptr<TIOAttempt> attempt = {});
    ~TDurableFile();

    std::error_code Write(std::span<const std::byte> bytes);
    std::error_code Flush();
    std::error_code Close();
    TResult<std::uint64_t> Position() const;
    std::uint64_t StoredLength() const;
    std::error_code Error() const;
    EFileState State() const;
    std::optional<TDurableFileReceipt> Receipt() const;

private:
    std::shared_ptr<ILocalFS> Fs_;
    THandle Directory_;
    THandle File_;
    std::string Path_;
    std::shared_ptr<TWriteLatch> Latch_;
    std::shared_ptr<TIOAttempt> Attempt_;
    mutable std::mutex Mutex_;
    EFileKind Kind_ = EFileKind::Other;
    EFileState State_ = EFileState::Open;
    TSha256 Hash_;
    std::uint64_t Position_ = 0;
    std::uint64_t StoredLength_ = 0;
    bool PositionOverflow_ = false;
    bool CloseCalled_ = false;
    std::error_code Error_;
    std::optional<TDurableFileReceipt> Receipt_;

    TDurableFile(std::shared_ptr<ILocalFS> fs,
        THandle directory,
        std::string path,
        std::shared_ptr<TWriteLatch> latch,
        std::shared_ptr<TIOAttempt> attempt);
    void Fail(std::error_code error);
    bool CheckWrite();
    bool Advance(EFileState next);
    TResult<std::size_t> WriteOnce(std::span<const std::byte> bytes);
    void CloseHandle();
};

}
