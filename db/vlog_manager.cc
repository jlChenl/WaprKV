#include "db/vlog_manager.h"

#include "db/vlog_reader.h"
#include "util/coding.h"

namespace leveldb {

VlogManager::VlogManager(uint64_t garbage_threshold) {
    full_threshold_ = garbage_threshold;
    now_vlog_ = 0;
}

VlogManager::~VlogManager() {
    for (auto iter = manager_.begin(); iter != manager_.end(); iter++) {
        delete iter->second.vlog_;
    }
}

void VlogManager::SetNowVlog(uint32_t vlog_numb) {
    if (now_vlog_ == vlog_numb - 1) {
        auto iter = manager_.find(vlog_numb);
        if (iter != manager_.end()) {
            if (iter->second.count_ >= full_threshold_) {
                full_vlog_set_.insert(now_vlog_);
            }
        }
    }
    now_vlog_ = vlog_numb;
}

void VlogManager::SetNowSortVlog(uint32_t sort_vlog_numb) {
    if (now_sort_vlog_ == sort_vlog_numb - 1) {
        auto iter = manager_.find(sort_vlog_numb);
        if (iter != manager_.end()) {
            if (iter->second.count_ >= full_threshold_) {
                full_vlog_set_.insert(now_sort_vlog_);
            }
        }
    }
    now_sort_vlog_ = sort_vlog_numb;
}

log::VReader* VlogManager::GetVlog(uint32_t vlog_numb) {
    MutexLock l(&mutex_);
    auto iter = manager_.find(vlog_numb);
    if (iter == manager_.end())
        return NULL;
    else
        return iter->second.vlog_;
}

void VlogManager::AddVlog(uint32_t vlog_numb, log::VReader* vlog) {
    MutexLock l(&mutex_);
    VlogInfo v;
    v.vlog_ = vlog;
    v.count_ = 0;
    manager_[vlog_numb] = v;
}

void VlogManager::RemoveVlog(uint32_t vlog_numb) {
    MutexLock l(&mutex_);
    auto iter = manager_.find(vlog_numb);
    if (iter != manager_.end()) {
        delete iter->second.vlog_;
        manager_.erase(iter);
    }
}

void VlogManager::AddFullVlog(uint32_t vlog_numb) { full_vlog_set_.insert(vlog_numb); }

void VlogManager::RemoveFullVlog(uint32_t vlog_numb) { full_vlog_set_.erase(vlog_numb); }

std::set<uint32_t> VlogManager::GetFullVlog() {
    std::set<uint32_t> res;
    for (auto iter = full_vlog_set_.begin(); iter != full_vlog_set_.end(); iter++) {
        res.insert(*iter);
    }
    return res;
}

std::set<uint32_t> VlogManager::GetAllVlog() {
    std::set<uint32_t> res;
    for (auto iter = manager_.begin(); iter != manager_.end(); iter++) {
        res.insert(iter->first);
    }
    return res;
}

uint64_t VlogManager::GetDropCount(uint32_t vlog_numb) { return manager_[vlog_numb].count_; }

void VlogManager::AddDropCount(uint32_t vlog_numb, uint64_t drop_size) {
    auto iter = manager_.find(vlog_numb);
    if (iter == manager_.end()) return;
    if (vlog_numb == now_vlog_ || vlog_numb == now_sort_vlog_) {
        iter->second.count_ += drop_size;
    } else {
        iter->second.count_ += drop_size;
        if (iter->second.count_ >= full_threshold_) {
            full_vlog_set_.insert(vlog_numb);
        }
    }
}

void VlogManager::AddDropCount(std::tr1::unordered_map<uint32_t, uint64_t>& vlog_numb) {
    for (auto iter = vlog_numb.begin(); iter != vlog_numb.end(); iter++) {
        AddDropCount(iter->first, iter->second);
    }
}

bool VlogManager::NeedClean(uint32_t vlog_numb) {
    if (manager_.count(vlog_numb) > 0) {
        return manager_[vlog_numb].count_ >= full_threshold_;
    }
    return false;
}

bool VlogManager::HasVlogToClean() { return !full_vlog_set_.empty(); }

uint32_t VlogManager::GetVlogToClean() {
    uint32_t result = *full_vlog_set_.begin();
    auto iter = full_vlog_set_.begin();
    iter++;
    for (; iter != full_vlog_set_.end(); iter++) {
        if (manager_[result].count_ < manager_[*iter].count_) {
            result = *iter;
        }
    }
    return result;
}

std::set<uint32_t> VlogManager::GetVlogsToClean(uint64_t garbage_threshold) {
    std::set<uint32_t> res;
    for (auto iter = manager_.begin(); iter != manager_.end(); iter++) {
        if (iter->second.count_ >= garbage_threshold && iter->first != now_vlog_) {
            res.insert(iter->first);
        }
    }
    return res;
}

bool VlogManager::Serialize(std::string& val) {
    val.clear();
    uint64_t size = manager_.size();
    if (size == 0) return false;

    auto iter = manager_.begin();
    for (; iter != manager_.end(); iter++) {
        char buf[8];
        EncodeFixed64(buf, (iter->second.count_ << 24) | iter->first);
        val.append(buf, 8);
    }
    return true;
}

bool VlogManager::Deserialize(std::string& val) {
    Slice input(val);
    while (!input.empty()) {
        uint64_t code = DecodeFixed64(input.data());
        uint64_t vlog_numb = code & 0xffffff;
        size_t count = code >> 24;
        if (manager_.count(vlog_numb) > 0) {
            manager_[vlog_numb].count_ = count;
            if (count >= full_threshold_ && vlog_numb != now_vlog_ && vlog_numb != now_sort_vlog_) {
                full_vlog_set_.insert(vlog_numb);
            }
        }
        input.remove_prefix(8);
    }
    return true;
}

void VlogManager::PrintVlogSet() {
    std::cout << "now vlog=" << now_vlog_ << " vlog_count=" << manager_.size() << "\n";
    for (auto iter = manager_.begin(); iter != manager_.end(); iter++) {
        std::cout << iter->first << "  " << iter->second.count_ << "   ";
    }
    std::cout << "\n";
    // std::cout << "full=" << full_vlog_set_.size() << "  ";
    // for (auto iter = full_vlog_set_.begin(); iter != full_vlog_set_.end(); iter++) {
    //     std::cout << *iter << "  ";
    // }
    // std::cout << "\n";
}

} // namespace leveldb
