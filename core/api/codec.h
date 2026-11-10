#pragma once

#include "storage/types.h"

#include <string_view>

namespace glamdb::detail {

Status ValidateOutcome(const AppendOutcome& outcome);
Result<std::string> EncodeOutcome(const AppendOutcome& outcome);
Result<AppendOutcome> DecodeOutcome(std::string_view bytes);
std::string EncodeBase(const BaseIdentity& base);
Result<BaseIdentity> DecodeBase(std::string_view bytes);

} // namespace glamdb::detail
