// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <algorithm>
#include <deque>
#include <iostream>
#include <numeric>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/vlog_manager.h"
#include "db/vlog_reader.h"
#include "db/vlog_writer.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "time.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class GarbageCollector;

class DBImpl : public DB {
public:
    DBImpl(const Options& options, const std::string& dbname);
    virtual ~DBImpl();

    Status RealValue(Slice val_ptr, std::string* value);
    Status RealValue(uint64_t size, uint32_t file_numb, uint64_t pos, std::string* value, bool print = false,
                     std::vector<uint64_t>* visited_file = NULL);
    // Implementations of the DB interface
    virtual Status Scan(const ReadOptions& options, const Slice& start, const Slice& end,
                        std::vector<std::string>& key_buf, std::vector<std::string>& value_buf, int* count = NULL,
                        std::vector<int>* vlogs = NULL);
    virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value,
                       std::vector<uint64_t>* visited_file = NULL);
    virtual Status GetPtr(const ReadOptions& options, const Slice& key, std::string* value,
                          std::tr1::unordered_set<uint64_t>* vlog_record = NULL,
                          std::vector<uint64_t>* visited_file = NULL);
    virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
    virtual Status Delete(const WriteOptions&, const Slice& key);
    virtual Status Write(const WriteOptions& options, WriteBatch* updates);
    //  virtual const Snapshot* GetSnapshot();//不支持快照
    //  virtual void ReleaseSnapshot(const Snapshot* snapshot);//同上
    virtual bool GetProperty(const Slice& property, std::string* value);
    virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);

    virtual void CompactRange(const Slice* begin, const Slice* end);
    Status TEST_CompactMemTable();
    void TEST_CompactRange(int level, const Slice* begin, const Slice* end);
    virtual Iterator* NewIterator(const ReadOptions&);
    Iterator* NewInternalIterator(const ReadOptions&, SequenceNumber* latest_snapshot, uint32_t* seed);
    Iterator* TEST_NewInternalIterator();
    int64_t TEST_MaxNextLevelOverlappingBytes();

    void RecordReadSample(Slice key);
    void CleanVlog();
    bool has_cleaned_;
    void MaybeScheduleClean(bool isManualClean = false);
    bool IsShutDown() { return shutting_down_.Acquire_Load(); }
    int TotalVlogFiles();
    uint64_t GetVlogNumber();
    uint64_t GetVlogHead();
    void PrintFiles();
    void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    bool needVisitAllLevel;
    std::vector<std::tr1::unordered_map<uint64_t, uint64_t>> gc_rewrite_off, gc_delete_off, gc_skip_off;
    double rewrite_count_;
    uint64_t flushed_seq;
    uint64_t flushed_vlog;

    uint64_t last_delete_vlog;

    double reorder_rewrite_;

private:
    friend class DB;
    friend class GarbageCollector;
    struct CompactionState;
    struct Writer;

    Status NewDB();
    void MaybeIgnoreError(Status* s) const;
    WriteBatch* BuildBatchGroup(Writer** last_writer);

    void RecordBackgroundError(const Status& s);
    Status MakeRoomForWrite(bool force) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    static void BGWork(void* db);
    void BackgroundCall();
    Status FullGCCompaction(const uint64_t check_numb, const uint64_t check_offset, uint64_t* flushed_file);
    void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base, uint64_t* file_numb = NULL,
                            bool* is_gc = NULL) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status DoCompactionWork(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status OpenCompactionOutputFile(CompactionState* compact);
    Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
    Status InstallCompactionResults(CompactionState* compact, const uint64_t check_numb, const uint64_t check_offset)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void DeleteObsoleteFiles();

    static void BGClean(void* db);
    static void BGCleanAll(void* db);
    void BackgroundClean();
    void BackgroundCleanAll();
    void CleanupCompaction(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status Recover(VersionEdit* edit, bool* save_manifest) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest, VersionEdit* edit,
                          SequenceNumber* max_sequence) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    static void BGCleanRecover(void* db);
    void BackgroundRecoverClean();

    // Constant after construction
    Env* const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_; // options_.comparator == &internal_comparator_
    bool owns_info_log_;
    bool owns_cache_;
    const std::string dbname_;

    // table_cache_ provides its own synchronization
    TableCache* table_cache_;

    // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
    FileLock* db_lock_;

    // State below is protected by mutex_
    port::Mutex mutex_;
    port::AtomicPointer shutting_down_;
    port::CondVar bg_cv_; // Signalled when background work finishes
    MemTable* mem_;
    MemTable* imm_;               // Memtable being compacted
    port::AtomicPointer has_imm_; // So bg thread can detect non-NULL imm_
    uint64_t logfile_number_;     // 当前vlog文件的编号
    uint64_t vlog_head_;          // 当前vlog文件的偏移写
    log::VWriter* vlog_;          // 写vlog的包装类
    WritableFile* vlogfile_;      // vlog文件写打开
    uint64_t check_point_;        // vlog文件的重启点
    uint64_t check_log_;          // 从那个vlog文件开始回放
    uint64_t drop_count_; // 合并产生了多少条垃圾记录，这些新产生的信息还没有持久化到sst文件
    uint64_t recover_clean_vlog_number_;
    uint64_t recover_clean_pos_;
    VlogManager vlog_manager_;
    uint32_t seed_; // For sampling.

    // Queue of writers.
    std::deque<Writer*> writers_;
    WriteBatch* tmp_batch_;

    //  SnapshotList snapshots_;

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pending_outputs_;
    std::tr1::unordered_map<uint64_t, int> sstable_refs_, vlog_refs_;
    std::tr1::unordered_set<uint64_t> vlog_delete_;
    std::tr1::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> vlog_recycle_;

    // Has a background compaction been scheduled or is running?
    bool bg_compaction_scheduled_;
    bool bg_clean_scheduled_;
    bool bg_gc_scheduled_;
    GarbageCollector* full_gc_;
    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey* begin; // NULL means beginning of key range
        const InternalKey* end;   // NULL means end of key range
        InternalKey tmp_storage;  // Used to keep track of compaction progress
    };
    ManualCompaction* manual_compaction_;

    VersionSet* versions_;

    // Have we encountered a background error in paranoid mode?
    Status bg_error_;

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;

        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats& c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }
    };
    CompactionStats stats_[config::kNumLevels];

    Status ReWriteValue(std::vector<std::string>& v_buf, std::vector<SequenceNumber>& seq_buf,
                        std::vector<std::string>& ptr_buf, const size_t count,
                        std::tr1::unordered_map<uint32_t, uint64_t>& vlog_rewrite, uint64_t& check_numb,
                        uint64_t& check_offset);

    Status ReWriteValue(
        std::vector<std::string>& v_buf, std::vector<SequenceNumber>& seq_buf, std::vector<size_t>& rewrite_idx,
        std::vector<uint64_t>& size_buf, std::vector<std::pair<uint32_t, uint64_t>>& file_pos_buf,
        std::tr1::unordered_map<uint32_t, uint64_t>& vlog_drop_count,
        std::tr1::unordered_map<uint32_t, std::tr1::unordered_map<uint64_t, uint64_t>>& vlog_drop_offset,
        std::tr1::unordered_map<uint32_t, uint64_t>& vlog_rewrite, uint64_t& check_numb, uint64_t& check_offset);
    void MergeRange(std::vector<uint64_t>& file_numb, std::vector<std::pair<std::string, std::string>>& resort_range,
                    std::vector<std::tr1::unordered_set<uint64_t>>& range_file);
    void PartitionRange(std::vector<std::pair<std::string, std::string>>& tmp_range,
                        std::vector<std::string>& bound_vec,
                        std::vector<std::pair<std::string, std::string>>& file_range);
    void ReduceRange(uint64_t file_numb, std::vector<std::pair<std::string, std::string>>& tmp_range,
                     std::tr1::unordered_set<std::string>& bound_set);

    std::tr1::unordered_map<uint64_t, std::vector<std::pair<std::string, std::string>>> disperse_range_;
    std::tr1::unordered_map<uint32_t, std::tr1::unordered_map<uint64_t, uint64_t>> vlog_partial_offset_;

    port::Mutex sort_mutex_;
    uint64_t sort_logfile_number_;
    uint64_t sort_vlog_head_;
    log::VWriter* sort_vlog_;
    WritableFile* sort_vlogfile_;

    // No copying allowed
    DBImpl(const DBImpl&);
    void operator=(const DBImpl&);

    const Comparator* user_comparator() const { return internal_comparator_.user_comparator(); }
};

extern Options SanitizeOptions(const std::string& db, const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy, const Options& src);

extern Status ParsePtr(Slice& val_ptr, uint64_t& size, std::pair<uint32_t, uint64_t>& file_pos);
extern Status ParsePtr(std::vector<std::string>& v_buf, std::vector<uint64_t>& size_buf,
                       std::vector<std::pair<uint32_t, uint64_t>>& file_pos_buf);
extern void SortFilePos(std::vector<std::pair<uint32_t, uint64_t>>& file_pos_buf, std::vector<size_t>& idx);

extern const uint64_t VlogBlockSize;
extern const uint64_t KVBufferSize;
extern const uint64_t KVReWriteSize;
extern const uint64_t test_key_size;
extern const uint64_t test_value_size;
extern const uint64_t test_kv_size;
extern const uint64_t sort_vlog_number_initial;
extern const uint64_t scan_len;

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_DB_IMPL_H_
