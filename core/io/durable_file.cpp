#include "core/io/durable_file.h"

#include <limits>
#include <new>

namespace NGlamDB::NIO {

TDurableFile::TDurableFile(std::shared_ptr<ILocalFS> fs,
    THandle directory,
    std::string path,
    std::shared_ptr<TWriteLatch> latch,
    std::shared_ptr<TIOAttempt> attempt)
    : Fs_(std::move(fs))
    , Directory_(std::move(directory))
    , Path_(std::move(path))
    , Latch_(std::move(latch))
    , Attempt_(std::move(attempt))
{ }

TResult<std::shared_ptr<TDurableFile>> TDurableFile::Create(std::shared_ptr<ILocalFS> fs,
    THandle directory,
    std::string path,
    std::shared_ptr<TWriteLatch> latch,
    std::shared_ptr<TIOAttempt> attempt)
{
    const auto leaf = std::string_view(path).substr(path.find_last_of('/') + 1);
    if (!fs || !directory || !latch || (attempt && attempt->Latch() != latch) || leaf.empty() || leaf == "." ||
        leaf == ".." || path.find('\0') != std::string::npos)
    {
        if (attempt) {
            attempt->FailFile(path, std::make_error_code(std::errc::invalid_argument));
        }
        return std::errc::invalid_argument;
    }
    if (auto error = latch->Error()) {
        return error;
    }
    auto file = std::shared_ptr<TDurableFile>(
        new TDurableFile(std::move(fs), std::move(directory), std::move(path), std::move(latch), std::move(attempt)));
    try {
        if (file->Attempt_) {
            auto registered = file->Attempt_->RegisterFile(file->Path_);
            if (!registered) {
                file->Fail(registered.Error());
                return file->Error_;
            }
            file->Kind_ = registered.Value();
        }
        const auto name = std::string_view(file->Path_).substr(file->Path_.find_last_of('/') + 1);
        auto opened = file->Fs_->CreateExclusive(file->Directory_,
            name,
            {file->Kind_, file->Attempt_ ? file->Attempt_->Id() : std::string{}});
        if (!opened) {
            file->Latch_->Observe(opened.Error());
            file->Fail(opened.Error());
            return file->Error_;
        }
        file->File_ = std::move(opened).Value();
        return file;
    } catch (const std::bad_alloc&) {
        file->Fail(std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        file->Fail(std::make_error_code(std::errc::io_error));
    }
    return file->Error_;
}

TDurableFile::~TDurableFile()
{
    if (State_ != EFileState::DirSynced && State_ != EFileState::Failed) {
        Fail(std::make_error_code(std::errc::operation_canceled));
    }
    CloseHandle();
}

void TDurableFile::Fail(std::error_code error)
{
    if (!error) {
        error = std::make_error_code(std::errc::io_error);
    }
    if (!Error_) {
        Error_ = error;
    }
    State_ = EFileState::Failed;
    Receipt_.reset();
    if (Attempt_) {
        Attempt_->FailFile(Path_, Error_);
    }
}

bool TDurableFile::CheckWrite()
{
    if (Attempt_) {
        if (auto error = Attempt_->CheckWrite(Path_)) {
            Fail(error);
        }
    }
    if (auto error = Latch_->Error()) {
        Fail(error);
    }
    return !Error_;
}

bool TDurableFile::Advance(EFileState next)
{
    if (Attempt_) {
        if (auto error = Attempt_->Advance(Path_, next)) {
            Fail(error);
            return false;
        }
    }
    State_ = next;
    return true;
}

void TDurableFile::CloseHandle()
{
    if (!File_ || CloseCalled_) {
        return;
    }
    CloseCalled_ = true;
    try {
        if (auto error = Fs_->Close(File_)) {
            Latch_->Observe(error);
            Fail(error);
        }
    } catch (...) {
        Fail(std::make_error_code(std::errc::io_error));
    }
    File_.reset();
}

TResult<std::size_t> TDurableFile::WriteOnce(std::span<const std::byte> bytes)
{
    try {
        return Fs_->Write(File_, bytes);
    } catch (const std::bad_alloc&) {
        Fail(std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        Fail(std::make_error_code(std::errc::io_error));
    }
    return Error_;
}

std::error_code TDurableFile::Write(std::span<const std::byte> bytes)
{
    const std::lock_guard lock(Mutex_);
    if (bytes.size() > std::numeric_limits<std::uint64_t>::max() - Position_) {
        PositionOverflow_ = true;
        Fail(std::make_error_code(std::errc::value_too_large));
    } else {
        Position_ += bytes.size();
    }
    try {
        if (!CheckWrite()) {
            return {};
        }
        if (State_ != EFileState::Open) {
            Fail(std::make_error_code(std::errc::bad_file_descriptor));
            return {};
        }
        while (!bytes.empty()) {
            std::optional<TWriteReservation> reservation;
            if (Attempt_) {
                auto reserved = Attempt_->ReserveWrite(Path_, bytes.size());
                if (!reserved) {
                    Fail(reserved.Error());
                    break;
                }
                reservation.emplace(std::move(reserved).Value());
            }
            auto written = WriteOnce(bytes);
            if (!written) {
                const auto error = written.Error();
                if (!Error_) {
                    Latch_->Observe(error);
                }
                if (error != std::errc::interrupted) {
                    Fail(error);
                }
                const auto settled = reservation ? reservation->Finish(0) : std::error_code{};
                if (settled) {
                    Fail(settled);
                }
                if (error == std::errc::interrupted && !Error_) {
                    continue;
                }
                break;
            }
            const auto accepted = written.Value();
            if (accepted == 0 || accepted > bytes.size()) {
                Fail(std::make_error_code(std::errc::io_error));
                if (reservation) {
                    const auto settled = reservation->Finish(0);
                    if (settled) {
                        Fail(settled);
                    }
                }
                break;
            }
            StoredLength_ += accepted;
            std::optional<TSha256Digest> digest;
            if (auto error = Hash_.Update(bytes.first(accepted))) {
                Fail(error);
            } else {
                digest = Hash_.Finalize();
            }
            if (reservation) {
                if (auto error = reservation->Finish(accepted, digest)) {
                    Fail(error);
                }
            }
            if (Error_) {
                break;
            }
            bytes = bytes.subspan(accepted);
        }
    } catch (const std::bad_alloc&) {
        Fail(std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        Fail(std::make_error_code(std::errc::io_error));
    }
    return {};
}

std::error_code TDurableFile::Flush()
{
    const std::lock_guard lock(Mutex_);
    if (CheckWrite() && State_ != EFileState::Open) {
        Fail(std::make_error_code(std::errc::bad_file_descriptor));
    }
    return {};
}

std::error_code TDurableFile::Close()
{
    const std::lock_guard lock(Mutex_);
    if (State_ == EFileState::DirSynced) {
        return {};
    }
    try {
        if (!CheckWrite()) {
            CloseHandle();
            return Error_;
        }
        if (auto error = Fs_->SyncFile(File_)) {
            Latch_->Observe(error);
            Fail(error);
        } else if (Advance(EFileState::Synced)) {
            const auto info = Fs_->Stat(File_);
            if (!info) {
                Fail(info.Error());
            } else if (info.Value().Type != EFileType::Regular || info.Value().Size != StoredLength_) {
                Fail(std::make_error_code(std::errc::state_not_recoverable));
            }
        }
        CloseHandle();
        if (Error_ || !Advance(EFileState::Closed)) {
            return Error_;
        }
        if (auto error = Fs_->SyncDir(Directory_)) {
            Latch_->Observe(error);
            Fail(error);
            return Error_;
        }
        if (!Advance(EFileState::DirSynced)) {
            return Error_;
        }
        const auto digest = Hash_.Finalize();
        if (Attempt_) {
            auto receipt = Attempt_->CompleteFile(Path_, StoredLength_, digest);
            if (!receipt) {
                Fail(receipt.Error());
            } else {
                Receipt_ = std::move(receipt).Value();
            }
        } else {
            Receipt_ = TDurableFileReceipt{Path_, Kind_, StoredLength_, digest, {}};
        }
    } catch (const std::bad_alloc&) {
        Fail(std::make_error_code(std::errc::not_enough_memory));
        CloseHandle();
    } catch (...) {
        Fail(std::make_error_code(std::errc::io_error));
        CloseHandle();
    }
    return Error_;
}

TResult<std::uint64_t> TDurableFile::Position() const
{
    const std::lock_guard lock(Mutex_);
    if (PositionOverflow_) {
        return std::errc::value_too_large;
    }
    return Position_;
}

std::uint64_t TDurableFile::StoredLength() const
{
    const std::lock_guard lock(Mutex_);
    return StoredLength_;
}

std::error_code TDurableFile::Error() const
{
    const std::lock_guard lock(Mutex_);
    return Error_;
}

EFileState TDurableFile::State() const
{
    const std::lock_guard lock(Mutex_);
    return State_;
}

std::optional<TDurableFileReceipt> TDurableFile::Receipt() const
{
    const std::lock_guard lock(Mutex_);
    return Receipt_;
}

}
