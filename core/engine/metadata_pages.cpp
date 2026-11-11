#include "core/engine/metadata_pages.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <sys/random.h>
#include <type_traits>

namespace glamdb::detail {
namespace {

template <typename T>
auto& Items(T& listing)
{
    if constexpr (std::is_same_v<std::remove_cvref_t<T>, TCollectionPage>) {
        return listing.Collections;
    } else if constexpr (std::is_same_v<std::remove_cvref_t<T>, SnapshotHistory>) {
        return listing.snapshots;
    } else {
        return listing.Files;
    }
}

size_t Count(const TMetadataPages::TListing& listing)
{
    return std::visit(
        [](const auto& value) {
            return Items(value).size();
        },
        listing);
}

std::string NewToken()
{
    std::array<unsigned char, 32> bytes{};
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto count = getrandom(bytes.data() + offset, bytes.size() - offset, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        Require(count > 0, "IO_ERROR", "Cannot generate a metadata cursor.");
        offset += static_cast<size_t>(count);
    }
    constexpr char Hex[] = "0123456789abcdef";
    std::string token;
    token.reserve(64);
    for (const auto byte : bytes) {
        token += Hex[byte >> 4];
        token += Hex[byte & 15];
    }
    return token;
}

class TCharge
{
public:
    void Add(size_t bytes)
    {
        Require(bytes <= std::numeric_limits<size_t>::max() - Bytes_,
            "RESOURCE_EXHAUSTED",
            "Metadata cursor size overflows.");
        Bytes_ += bytes;
    }

    void Add(const std::string& text)
    {
        Add(text.capacity() + 1);
    }

    void Add(const BaseIdentity& identity)
    {
        Add(identity.store_id);
        Add(identity.collection_id);
        Add(identity.table_uuid);
        Add(identity.metadata_location);
    }

    void Add(const CollectionInfo& collection)
    {
        Add(collection.collection_id);
        Add(collection.name);
        Add(collection.table_uuid);
        Add(collection.schema.fields.capacity() * sizeof(FieldSpec));
        for (const auto& field : collection.schema.fields) {
            Add(field.name);
        }
    }

    void Add(const SnapshotMetadata& snapshot)
    {
        Add(snapshot.receipt.collection_id);
        Add(snapshot.receipt.operation_id);
        Add(snapshot.receipt.payload_hash);
        Add(snapshot.receipt.durability_profile);
    }

    void Add(const TDataFileMetadata& file)
    {
        Add(file.FileID);
        Add(file.Format);
        Add(file.FieldMetrics.capacity() * sizeof(TFileFieldMetrics));
        for (const auto& field : file.FieldMetrics) {
            Add(field.LowerBound ? field.LowerBound->capacity() : 0);
            Add(field.UpperBound ? field.UpperBound->capacity() : 0);
        }
    }

    size_t GetBytes() const
    {
        return Bytes_;
    }

private:
    size_t Bytes_ = 0;
};

size_t Measure(const TMetadataPages::TListing& listing)
{
    TCharge charge;
    std::visit(
        [&](const auto& value) {
            using T = std::remove_cvref_t<decltype(value)>;
            if constexpr (std::is_same_v<T, SnapshotHistory>) {
                charge.Add(value.identity);
            } else if constexpr (std::is_same_v<T, TFileListing>) {
                charge.Add(value.Identity);
            }
            const auto& items = Items(value);
            charge.Add(items.capacity() * sizeof(typename std::remove_cvref_t<decltype(items)>::value_type));
            for (const auto& item : items) {
                charge.Add(item);
            }
        },
        listing);
    return charge.GetBytes();
}

} // namespace

TMetadataPages::TMetadataPages()
    : TMetadataPages(TOptions{})
{ }

TMetadataPages::TMetadataPages(TOptions options)
    : Options_(options)
{ }

void TMetadataPages::Expire(TClock::time_point now)
{
    std::erase_if(Sessions_, [&](const auto& session) {
        if (session.Expires <= now) {
            ChargedBytes_ -= session.ChargedBytes;
            return true;
        }
        return false;
    });
}

TMetadataPages::TListing TMetadataPages::ReadPage(const TSession& session, size_t offset, size_t pageSize)
{
    auto result = std::visit(
        [&](const auto& value) -> TListing {
            using T = std::remove_cvref_t<decltype(value)>;
            T page;
            if constexpr (std::is_same_v<T, TCollectionPage>) {
                page.CreationHighWaterMark = value.CreationHighWaterMark;
            } else if constexpr (std::is_same_v<T, SnapshotHistory>) {
                page.identity = value.identity;
            } else {
                page.Identity = value.Identity;
                page.SelectorResolved = value.SelectorResolved;
            }
            const auto& source = Items(value);
            const auto begin = source.begin() + static_cast<std::ptrdiff_t>(offset);
            const auto count = std::min(pageSize, source.size() - offset);
            Items(page).assign(begin, begin + static_cast<std::ptrdiff_t>(count));
            if (offset + count < source.size()) {
                page.NextPageToken = session.Tokens[offset / pageSize];
            }
            return page;
        },
        session.Listing);
    return result;
}

Result<TMetadataPages::TListing> TMetadataPages::Read(const TMetadataPageRequest& request,
    std::string_view parameters,
    const TLoad& load,
    TClock::time_point now,
    std::optional<uint64_t> admissionGeneration)
{
    bool pending = false;
    uint64_t generation = 0;
    try {
        Require(request.PageSize >= 1 && request.PageSize <= 1024 && request.AuthorizationContext.size() <= 1024 &&
                parameters.size() <= 2048,
            "INVALID_ARGUMENT",
            "Metadata page parameters exceed their limits.");
        Encoder encoded;
        encoded.Text(parameters);
        encoded.Number(request.PageSize);
        encoded.Text(request.AuthorizationContext);
        const auto pageSize = static_cast<size_t>(request.PageSize);
        {
            std::lock_guard lock(Mutex_);
            Require(!admissionGeneration || *admissionGeneration == Generation_,
                "INVALID_CURSOR",
                "Metadata cursor admission was invalidated.");
            Expire(now);
            if (!request.PageToken.empty()) {
                Require(ValidDigest(request.PageToken), "INVALID_CURSOR", "Metadata cursor is invalid or expired.");
                for (const auto& session : Sessions_) {
                    const auto positionIt = session.Positions.find(request.PageToken);
                    if (positionIt != session.Positions.end()) {
                        Require(session.Binding == encoded.bytes,
                            "INVALID_CURSOR",
                            "Metadata cursor parameters or authorization changed.");
                        return ReadPage(session, positionIt->second, pageSize);
                    }
                }
                throw Failure({"INVALID_CURSOR", "Metadata cursor is invalid or expired.", RetryHint::FixInput});
            }
            Require(Sessions_.size() + Pending_ < Options_.MaxSessions,
                "RESOURCE_EXHAUSTED",
                "Metadata cursor capacity is exhausted.");
            ++Pending_;
            pending = true;
            generation = Generation_;
        }
        auto listing = load();
        if (!listing) {
            throw Failure(listing.error());
        }
        TSession session{.Binding = std::move(encoded.bytes),
            .Listing = std::move(*listing),
            .Expires = now + Options_.Lifetime};
        const auto count = Count(session.Listing);
        if (count > pageSize) {
            const auto tokenCount = (count - 1) / pageSize;
            TCharge charge;
            charge.Add(sizeof(TSession));
            charge.Add(session.Binding);
            charge.Add(Measure(session.Listing));
            // Reserve both token strings, tree nodes and vector capacity up front.
            Require(tokenCount <= Options_.MaxBytes / 512,
                "RESOURCE_EXHAUSTED",
                "Metadata cursor exceeds its memory limit.");
            charge.Add(tokenCount * 512);
            session.ChargedBytes = charge.GetBytes();
            Require(session.ChargedBytes <= Options_.MaxBytes,
                "RESOURCE_EXHAUSTED",
                "Metadata cursor exceeds its memory limit.");
            session.Tokens.reserve(tokenCount);
            for (size_t index = 0; index < tokenCount; ++index) {
                auto token = NewToken();
                Require(session.Positions.emplace(token, (index + 1) * pageSize).second,
                    "INTERNAL",
                    "Metadata cursor collision.");
                session.Tokens.push_back(std::move(token));
            }
        }
        auto page = ReadPage(session, 0, pageSize);
        {
            std::lock_guard lock(Mutex_);
            --Pending_;
            pending = false;
            Require(generation == Generation_, "INVALID_CURSOR", "Metadata cursor admission was invalidated.");
            Require(count <= pageSize || session.Expires > std::max(now, TClock::now()),
                "INVALID_CURSOR",
                "Metadata cursor expired while loading its view.");
            if (count > pageSize) {
                Require(session.ChargedBytes <= Options_.MaxBytes - ChargedBytes_,
                    "RESOURCE_EXHAUSTED",
                    "Metadata cursor memory is exhausted.");
                Sessions_.push_back(std::move(session));
                ChargedBytes_ += Sessions_.back().ChargedBytes;
            }
        }
        return page;
    } catch (const Failure& error) {
        if (pending) {
            std::lock_guard lock(Mutex_);
            --Pending_;
        }
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        if (pending) {
            std::lock_guard lock(Mutex_);
            --Pending_;
        }
        return std::unexpected(
            ErrorDetail{dynamic_cast<const std::bad_alloc*>(&error) ? "RESOURCE_EXHAUSTED" : "INTERNAL",
                BoundedDiagnostic(error.what()),
                RetryHint::OperatorAction});
    }
}

void TMetadataPages::Clear()
{
    std::lock_guard lock(Mutex_);
    Sessions_.clear();
    ChargedBytes_ = 0;
    ++Generation_;
}

uint64_t TMetadataPages::GetGeneration() const
{
    std::lock_guard lock(Mutex_);
    return Generation_;
}

} // namespace glamdb::detail
