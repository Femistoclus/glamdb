#include "core/io/store_dirs.h"
#include "core/io/store_uri.h"

#include <initializer_list>
#include <utility>

namespace NGlamDB::NIO {
namespace {

bool ValidRoot(const std::filesystem::path& path)
{
    return path.is_absolute() && !path.filename().empty() && path.native().find('\0') == std::string::npos &&
        path.lexically_normal().native() == path.native();
}

TResult<THandle> Child(ILocalFS& fs, TWriteLatch& latch, const THandle& parent, std::string_view name, bool create)
{
    if (create) {
        if (auto error = latch.Error()) {
            return error;
        }
        const auto error = fs.MakeDir(parent, name);
        latch.Observe(error);
        if (error && error != std::errc::file_exists) {
            return error;
        }
    }
    auto opened = fs.OpenDir(parent, name);
    if (create) {
        latch.Observe(opened.Error());
    }
    return opened;
}

std::error_code Sync(ILocalFS& fs, TWriteLatch& latch, std::initializer_list<THandle> directories)
{
    for (const auto& directory : directories) {
        const auto error = fs.SyncDir(directory);
        latch.Observe(error);
        if (error) {
            return error;
        }
    }
    return {};
}

}

TStoreDirs::TStoreDirs(std::shared_ptr<ILocalFS> fs,
    std::shared_ptr<TWriteLatch> latch,
    std::filesystem::path path,
    THandle root,
    THandle warehouse,
    TFileStat identity)
    : Fs_(std::move(fs))
    , Latch_(std::move(latch))
    , Path_(std::move(path))
    , Root_(std::move(root))
    , Warehouse_(std::move(warehouse))
    , Identity_(identity)
{ }

TResult<std::shared_ptr<TStoreDirs>> TStoreDirs::Create(std::shared_ptr<ILocalFS> fs,
    const std::filesystem::path& absoluteRoot,
    std::shared_ptr<TWriteLatch> latch)
{
    return Build(std::move(fs), absoluteRoot, std::move(latch), true);
}

TResult<std::shared_ptr<TStoreDirs>> TStoreDirs::Open(std::shared_ptr<ILocalFS> fs,
    const std::filesystem::path& absoluteRoot,
    std::shared_ptr<TWriteLatch> latch)
{
    return Build(std::move(fs), absoluteRoot, std::move(latch), false);
}

TResult<std::shared_ptr<TStoreDirs>> TStoreDirs::Build(std::shared_ptr<ILocalFS> fs,
    const std::filesystem::path& absoluteRoot,
    std::shared_ptr<TWriteLatch> latch,
    bool create)
{
    if (!fs || !latch || !ValidRoot(absoluteRoot)) {
        return std::errc::invalid_argument;
    }
    if (create && latch->IsSet()) {
        return latch->Error();
    }
    auto parent = fs->OpenDir(absoluteRoot.parent_path());
    if (!parent) {
        if (create) {
            latch->Observe(parent.Error());
        }
        return parent.Error();
    }
    auto root = Child(*fs, *latch, parent.Value(), absoluteRoot.filename().native(), create);
    if (!root) {
        return root.Error();
    }
    auto warehouse = Child(*fs, *latch, root.Value(), "warehouse", create);
    if (!warehouse) {
        return warehouse.Error();
    }
    if (create) {
        if (auto error = Sync(*fs, *latch, {warehouse.Value(), root.Value(), parent.Value()})) {
            return error;
        }
    }
    auto identity = fs->Stat(root.Value());
    if (!identity) {
        return identity.Error();
    }
    auto store = std::shared_ptr<TStoreDirs>(new TStoreDirs(std::move(fs),
        std::move(latch),
        absoluteRoot,
        std::move(root).Value(),
        std::move(warehouse).Value(),
        identity.Value()));
    if (auto error = store->CheckIdentity()) {
        return error;
    }
    return store;
}

TResult<TCollectionDirs> TStoreDirs::Collection(std::string_view collectionId, bool create) const
{
    if (!IsCanonicalUuid(collectionId)) {
        return std::errc::invalid_argument;
    }
    if (create) {
        if (auto error = Latch_->Error()) {
            return error;
        }
        if (auto error = CheckIdentity()) {
            return error;
        }
    }
    auto collection = Child(*Fs_, *Latch_, Warehouse_, collectionId, create);
    if (!collection) {
        return collection.Error();
    }
    auto data = Child(*Fs_, *Latch_, collection.Value(), "data", create);
    if (!data) {
        return data.Error();
    }
    auto metadata = Child(*Fs_, *Latch_, collection.Value(), "metadata", create);
    if (!metadata) {
        return metadata.Error();
    }
    if (create) {
        if (auto error = Sync(*Fs_, *Latch_, {data.Value(), metadata.Value(), collection.Value(), Warehouse_, Root_})) {
            return error;
        }
        if (auto error = CheckIdentity()) {
            return error;
        }
    }
    return TCollectionDirs{std::move(collection).Value(), std::move(data).Value(), std::move(metadata).Value()};
}

TResult<TCollectionDirs> TStoreDirs::CreateCollection(std::string_view collectionId)
{
    return Collection(collectionId, true);
}

TResult<TCollectionDirs> TStoreDirs::OpenCollection(std::string_view collectionId) const
{
    return Collection(collectionId, false);
}

std::error_code TStoreDirs::CheckIdentity() const
{
    // Evidence that the path no longer names this store blocks writes until
    // reopen. Other failures, such as descriptor or memory exhaustion, fail
    // only this call; EIO and EROFS still latch like any write-path syscall.
    const auto fail = [this](std::error_code error) {
        if (error == std::errc::no_such_file_or_directory || error == std::errc::not_a_directory ||
            error == std::errc::too_many_symbolic_link_levels)
        {
            Latch_->Block();
        } else {
            Latch_->Observe(error);
        }
        return error;
    };
    auto current = Fs_->OpenDir(Path_);
    if (!current) {
        return fail(current.Error());
    }
    auto identity = Fs_->Stat(current.Value());
    if (!identity) {
        return fail(identity.Error());
    }
    if (identity.Value().Device != Identity_.Device || identity.Value().Inode != Identity_.Inode) {
        Latch_->Block();
        return std::make_error_code(std::errc::state_not_recoverable);
    }
    return {};
}

const std::shared_ptr<ILocalFS>& TStoreDirs::Fs() const noexcept
{
    return Fs_;
}

const std::shared_ptr<TWriteLatch>& TStoreDirs::Latch() const noexcept
{
    return Latch_;
}

const THandle& TStoreDirs::Root() const noexcept
{
    return Root_;
}

const THandle& TStoreDirs::Warehouse() const noexcept
{
    return Warehouse_;
}

const std::filesystem::path& TStoreDirs::Path() const noexcept
{
    return Path_;
}

}
