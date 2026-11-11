#pragma once

#include <algorithm>
#include <deque>
#include <map>
#include <optional>
#include <string>

namespace glamdb::NServer {
// Used only on the I/O thread. One FIFO per database, rotated after each start.
template <class T>
class TFairQueue
{
public:
    struct TLimits
    {
        size_t Active;
        size_t PerDatabase;
    };
    explicit TFairQueue(TLimits limits)
        : ActiveLimit_(limits.Active)
        , DatabaseLimit_(limits.PerDatabase)
    { }
    bool Enqueue(const std::string& database, size_t bytes, T value)
    {
        auto& calls = Databases_[database];
        if (Pending_ >= 64 || calls.Pending.size() >= 16 || bytes > (size_t{64} << 20) - Bytes_ ||
            bytes > (size_t{32} << 20) - calls.Bytes)
        {
            if (calls.Active == 0 && calls.Pending.empty()) {
                Databases_.erase(database);
            }
            return false;
        }
        try {
            calls.Pending.push_back({std::move(value), bytes});
        } catch (...) {
            if (calls.Active == 0 && calls.Pending.empty()) {
                Databases_.erase(database);
            }
            throw;
        }
        ++Pending_;
        Bytes_ += bytes;
        calls.Bytes += bytes;
        return true;
    }
    std::optional<T> Take()
    {
        if (Active_ >= ActiveLimit_ || Pending_ == 0) {
            return {};
        }
        auto next = Databases_.upper_bound(Last_);
        for (size_t checked = 0; checked < Databases_.size(); ++checked) {
            if (next == Databases_.end()) {
                next = Databases_.begin();
            }
            auto& [database, calls] = *next++;
            if (!calls.Pending.empty() && calls.Active < DatabaseLimit_) {
                Last_ = database;
                auto call = std::move(calls.Pending.front());
                calls.Pending.pop_front();
                --Pending_;
                Bytes_ -= call.Bytes;
                calls.Bytes -= call.Bytes;
                ++Active_;
                ++calls.Active;
                return std::move(call.Value);
            }
        }
        return {};
    }
    void Release(const std::string& database)
    {
        auto found = Databases_.find(database);
        if (found == Databases_.end()) {
            return;
        }
        --found->second.Active;
        --Active_;
        EraseEmpty(found);
    }
    void Remove(const std::string& database, const T& value)
    {
        const auto found = Databases_.find(database);
        if (found == Databases_.end()) {
            return;
        }
        auto& calls = found->second;
        const auto call = std::find_if(calls.Pending.begin(), calls.Pending.end(), [&](const auto& item) {
            return item.Value == value;
        });
        if (call != calls.Pending.end()) {
            Bytes_ -= call->Bytes;
            calls.Bytes -= call->Bytes;
            --Pending_;
            calls.Pending.erase(call);
        }
        EraseEmpty(found);
    }

private:
    struct TCall
    {
        T Value;
        size_t Bytes;
    };
    struct TDatabase
    {
        size_t Active = 0, Bytes = 0;
        std::deque<TCall> Pending;
    };
    void EraseEmpty(typename std::map<std::string, TDatabase>::iterator found)
    {
        if (found->second.Active == 0 && found->second.Pending.empty()) {
            Databases_.erase(found);
        }
    }
    const size_t ActiveLimit_, DatabaseLimit_;
    size_t Active_ = 0, Pending_ = 0, Bytes_ = 0;
    std::string Last_;
    std::map<std::string, TDatabase> Databases_;
};
} // namespace glamdb::NServer
