#pragma once

#include "storage/metadata.h"
#include "storage/resources.h"

namespace glamdb::detail {
Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, TCollectionPage& result);
Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, SnapshotHistory& result);
Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, CollectionStatistics& result);
Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, ScanExplanation& result);
Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, TFileListing& result);
Status RetainMetadata(const std::shared_ptr<ResourceOwner>& owner, TFileDetails& result);
} // namespace glamdb::detail
