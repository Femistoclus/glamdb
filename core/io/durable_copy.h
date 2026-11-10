#pragma once

#include "core/io/io_attempt.h"
#include "core/io/local_fs.h"
#include "core/io/write_latch.h"

namespace NGlamDB::NIO {

struct TCopySource
{
    std::shared_ptr<ILocalFS> Fs;
    THandle Directory;
    std::string Name;
};

struct TCopyTarget
{
    std::shared_ptr<ILocalFS> Fs;
    THandle Directory;
    std::string Name;
    std::shared_ptr<TWriteLatch> Latch;
};

// The caller holds an offline snapshot/lock and has durably created the target directory.
TResult<TDurableFileReceipt>
CopyDurableFile(const TCopySource& source, const TCopyTarget& target, std::uint64_t expectedLength);

}
