#include "core/scan/read_budget.h"

#include <iceberg/file_io.h>

namespace glamdb::detail {

TReadBudget::TReadBudget(uint64_t bytes,
    std::chrono::steady_clock::time_point deadline,
    std::shared_ptr<const std::atomic<bool>> cancellation)
    : Remaining_(bytes)
    , Deadline_(deadline)
    , Cancellation_(std::move(cancellation))
{ }

Status TReadBudget::Charge(uint64_t requested)
{
    std::lock_guard lock(Mutex_);
    if (!Error_) {
        if (Cancellation_ && Cancellation_->load()) {
            Error_ = ErrorDetail{"CANCELLED", "bounded read was cancelled", RetryHint::SameIdAfterReconcile};
        } else if (std::chrono::steady_clock::now() >= Deadline_) {
            Error_ = ErrorDetail{"DEADLINE_EXCEEDED", "bounded read deadline expired", RetryHint::SameIdAfterReconcile};
        } else if (requested > Remaining_) {
            Error_ = ErrorDetail{"RESOURCE_EXHAUSTED", "bounded read I/O budget exhausted", RetryHint::FixInput};
        } else {
            Remaining_ -= requested;
        }
    }
    return Error_ ? Status(std::unexpected(*Error_)) : Status{};
}

std::optional<ErrorDetail> TReadBudget::Error() const
{
    std::lock_guard lock(Mutex_);
    return Error_;
}

namespace {
class TBudgetStream final : public iceberg::SeekableInputStream
{
public:
    TBudgetStream(std::unique_ptr<iceberg::SeekableInputStream> stream, std::shared_ptr<TReadBudget> budget)
        : Stream_(std::move(stream))
        , Budget_(std::move(budget))
    { }
    iceberg::Result<int64_t> Position() const override
    {
        return Stream_->Position();
    }
    iceberg::Status Seek(int64_t position) override
    {
        return Stream_->Seek(position);
    }
    iceberg::Result<int64_t> Read(std::span<std::byte> out) override
    {
        if (auto charged = Budget_->Charge(out.size()); !charged) {
            return iceberg::IOError("RESOURCE: {}", charged.error().message);
        }
        return Stream_->Read(out);
    }
    iceberg::Status ReadFully(int64_t position, std::span<std::byte> out) override
    {
        if (auto charged = Budget_->Charge(out.size()); !charged) {
            return iceberg::IOError("RESOURCE: {}", charged.error().message);
        }
        return Stream_->ReadFully(position, out);
    }
    iceberg::Status Close() override
    {
        return Stream_->Close();
    }

private:
    std::unique_ptr<iceberg::SeekableInputStream> Stream_;
    std::shared_ptr<TReadBudget> Budget_;
};

class TBudgetInput final : public iceberg::InputFile
{
public:
    TBudgetInput(std::unique_ptr<iceberg::InputFile> input, std::shared_ptr<TReadBudget> budget)
        : Input_(std::move(input))
        , Budget_(std::move(budget))
    { }
    std::string_view location() const override
    {
        return Input_->location();
    }
    iceberg::Result<int64_t> Size() const override
    {
        return Input_->Size();
    }
    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        auto opened = Input_->Open();
        if (!opened) {
            return std::unexpected(opened.error());
        }
        return std::make_unique<TBudgetStream>(std::move(*opened), Budget_);
    }

private:
    std::unique_ptr<iceberg::InputFile> Input_;
    std::shared_ptr<TReadBudget> Budget_;
};

class TBudgetFileIO final : public iceberg::FileIO
{
public:
    TBudgetFileIO(std::shared_ptr<iceberg::FileIO> base, std::shared_ptr<TReadBudget> budget)
        : Base_(std::move(base))
        , Budget_(std::move(budget))
    { }
    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string path) override
    {
        auto input = Base_->NewInputFile(std::move(path));
        if (!input) {
            return std::unexpected(input.error());
        }
        return std::make_unique<TBudgetInput>(std::move(*input), Budget_);
    }
    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string path, size_t length) override
    {
        auto input = Base_->NewInputFile(std::move(path), length);
        if (!input) {
            return std::unexpected(input.error());
        }
        return std::make_unique<TBudgetInput>(std::move(*input), Budget_);
    }
    iceberg::Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string) override
    {
        return iceberg::NotAllowed("bounded reader has no write authority");
    }

private:
    std::shared_ptr<iceberg::FileIO> Base_;
    std::shared_ptr<TReadBudget> Budget_;
};
} // namespace

std::shared_ptr<iceberg::FileIO> BudgetReadOnlyFileIO(std::shared_ptr<iceberg::FileIO> base,
    std::shared_ptr<TReadBudget> budget)
{
    return std::make_shared<TBudgetFileIO>(std::move(base), std::move(budget));
}

} // namespace glamdb::detail
