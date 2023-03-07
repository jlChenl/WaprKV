
#ifndef STORAGE_LEVELDB_DB_GARBAGE_COLLECTOR_H_
#define STORAGE_LEVELDB_DB_GARBAGE_COLLECTOR_H_

#include <pthread.h>
#include <iostream>
#include <numeric>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include "db/memtable.h"
#include "db/vlog_reader.h"
#include "stdint.h"

namespace leveldb {
class VReader;
class DBImpl;
class VersionEdit;

class GarbageCollector {
public:
    GarbageCollector(DBImpl* db);
    ~GarbageCollector();
    void SetVlog(uint64_t vlog_number, uint64_t garbage_beg_pos = 0);
    void BeginGarbageCollect(VersionEdit* edit, bool* save_edit);
    void FullGarbageCollect(VersionEdit* edit, bool* save_edit, bool* delete_vlog);

    uint64_t vlog_number_;
    uint64_t garbage_pos_;
    uint64_t initial_pos_;
    log::VReader* vlog_reader_;
    DBImpl* db_;
    MemTable* mem_;
    std::tr1::unordered_map<uint32_t, uint64_t> vlog_rewrite_;
    port::AtomicPointer need_pause_;
    bool finished_;
    bool flushing_;
    std::tr1::unordered_set<uint64_t> flushed_files_;
    pthread_mutex_t mutex_;
    pthread_cond_t cond_recover_, cond_pause_;
};

} // namespace leveldb

#endif
