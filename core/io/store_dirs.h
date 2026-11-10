#pragma once

#include "core/io/local_fs.h"
#include "core/io/write_latch.h"

namespace NGlamDB::NIO {

struct TCollectionDirs
{
    THandle Collection;
    THandle Data;
    THandle Metadata;
};

class TStoreDirs
{
public:
    static TResult<std::shared_ptr<TStoreDirs>>
    Create(std::shared_ptr<ILocalFS> fs, const std::filesystem::path& absoluteRoot, std::shared_ptr<TWriteLatch> latch);
    static TResult<std::shared_ptr<TStoreDirs>>
    Open(std::shared_ptr<ILocalFS> fs, const std::filesystem::path& absoluteRoot, std::shared_ptr<TWriteLatch> latch);

    TResult<TCollectionDirs> CreateCollection(std::string_view collectionId);
    TResult<TCollectionDirs> OpenCollection(std::string_view collectionId) const;
    std::error_code CheckIdentity() const;

    const std::shared_ptr<ILocalFS>& Fs() const noexcept;
    const std::shared_ptr<TWriteLatch>& Latch() const noexcept;
    const THandle& Root() const noexcept;
    const THandle& Warehouse() const noexcept;
    const std::filesystem::path& Path() const noexcept;

private:
    std::shared_ptr<ILocalFS> Fs_;
    std::shared_ptr<TWriteLatch> Latch_;
    std::filesystem::path Path_;
    THandle Root_;
    THandle Warehouse_;
    TFileStat Identity_;

    TStoreDirs(std::shared_ptr<ILocalFS> fs,
        std::shared_ptr<TWriteLatch> latch,
        std::filesystem::path path,
        THandle root,
        THandle warehouse,
        TFileStat identity);
    static TResult<std::shared_ptr<TStoreDirs>> Build(std::shared_ptr<ILocalFS> fs,
        const std::filesystem::path& absoluteRoot,
        std::shared_ptr<TWriteLatch> latch,
        bool create);
    TResult<TCollectionDirs> Collection(std::string_view collectionId, bool create) const;
};

}
