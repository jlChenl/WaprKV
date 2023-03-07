#ifndef STORAGE_LEVELDB_DB_VLOG_MANAGER_H_
#define STORAGE_LEVELDB_DB_VLOG_MANAGER_H_

#include <iostream>
#include <set>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "db/vlog_reader.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/mutexlock.h"

namespace leveldb {
class VlogManager {
public:
    struct VlogInfo {
        log::VReader* vlog_;
        uint64_t count_;
    };

    VlogManager(uint64_t garbage_threshold);
    ~VlogManager();

    void SetNowVlog(uint32_t vlog_numb);
    void SetNowSortVlog(uint32_t vlog_numb);

    void AddVlog(uint32_t vlog_numb, log::VReader* vlog);
    void RemoveVlog(uint32_t vlog_numb);
    log::VReader* GetVlog(uint32_t vlog_numb);
    void AddFullVlog(uint32_t vlog_numb);
    void RemoveFullVlog(uint32_t vlog_numb);
    std::set<uint32_t> GetFullVlog();
    std::set<uint32_t> GetAllVlog();

    uint64_t GetDropCount(uint32_t vlog_numb);
    void AddDropCount(uint32_t vlog_numb, uint64_t drop_size = 1);
    void AddDropCount(std::tr1::unordered_map<uint32_t, uint64_t>& vlog_numb);

    bool NeedClean(uint32_t vlog_numb);
    bool HasVlogToClean();
    uint32_t GetVlogToClean();
    std::set<uint32_t> GetVlogsToClean(uint64_t garbage_threshold);

    bool Serialize(std::string& val);
    bool Deserialize(std::string& val);
    void PrintVlogSet();

    std::tr1::unordered_map<uint32_t, VlogInfo> manager_;
    std::tr1::unordered_set<uint32_t> full_vlog_set_;
    uint64_t full_threshold_;
    uint32_t now_vlog_;
    uint32_t now_sort_vlog_;
    port::Mutex mutex_;
};
} // namespace leveldb

#endif
