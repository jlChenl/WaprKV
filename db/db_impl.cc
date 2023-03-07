// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/garbage_collector.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/vlog_reader.h"
#include "db/vlog_writer.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const uint64_t sort_vlog_number_initial = 1 << 23;
const int kNumNonTableCacheFiles = 10;
const uint64_t VlogBlockSize = 4096;
const uint64_t KVBufferSize = 4096;
const uint64_t KVReWriteSize = 128;
const uint64_t test_key_size = 16;
const uint64_t test_value_size = 256;
const uint64_t test_kv_size = test_key_size + test_value_size;
const uint64_t scan_len = 200;
const uint64_t scan_resort_rate = 4;
const uint64_t resort_minumum_disperse_L0_level = 256;
const uint64_t resort_minumum_disperse_per_level = 1024;
const uint64_t scan_minimum_visit_L0_level = 8;
const uint64_t scan_minimum_visit_per_level = scan_len * 0.1;

// Information kept for every waiting writer
struct DBImpl::Writer {
    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;

    explicit Writer(port::Mutex* mu) : cv(mu) {}
};

struct DBImpl::CompactionState {
    Compaction* const compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    SequenceNumber smallest_snapshot;

    // Files produced by compaction
    struct Output {
        uint64_t number;
        uint64_t file_size;
        InternalKey smallest, largest;
    };
    std::vector<Output> outputs;

    // State kept for output being generated
    WritableFile* outfile;
    TableBuilder* builder;

    uint64_t total_bytes;

    Output* current_output() { return &outputs[outputs.size() - 1]; }

    explicit CompactionState(Compaction* c) : compaction(c), outfile(NULL), builder(NULL), total_bytes(0) {}
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string& dbname, const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy, const Options& src) {
    Status status;
    Options result = src;
    result.comparator = icmp;
    result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
    ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
    ClipToRange(&result.block_size, 1 << 10, 4 << 20);
    if (result.info_log == NULL) {
        // Open a log file in the same directory as the db
        src.env->CreateDir(dbname); // In case it does not exist
        src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        Status status = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!status.ok()) {
            // No place suitable for logging
            result.info_log = NULL;
        }
    }
    if (result.block_cache == NULL) {
        result.block_cache = NewLRUCache(8 << 20);
    }
    return result;
}

Status ParsePtr(Slice& value_ptr, uint64_t& size, std::pair<uint32_t, uint64_t>& file_pos) {
    Status status;
    if (!GetVarint64(&value_ptr, &size)) status = Status::Corruption("parse size false in RealValue");
    if (!GetVarint32(&value_ptr, &file_pos.first)) status = Status::Corruption("parse file_numb false in RealValue");
    if (!GetVarint64(&value_ptr, &file_pos.second)) status = Status::Corruption("parse pos false in RealValue");
    return status;
}

Status ParsePtr(std::vector<std::string>& v_buf, std::vector<uint64_t>& size_buf,
                std::vector<std::pair<uint32_t, uint64_t>>& file_pos_buf) {
    Status status;
    for (size_t i = 0; i < v_buf.size(); i++) {
        Slice val_ptr(v_buf[i]);
        status = ParsePtr(val_ptr, size_buf[i], file_pos_buf[i]);
        if (!status.ok()) break;
    }
    return status;
}

void SortFilePos(std::vector<std::pair<uint32_t, uint64_t>>& file_pos_buf, std::vector<size_t>& idx) {
    idx.resize(file_pos_buf.size());
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(),
              [&file_pos_buf](size_t i, size_t j) { return file_pos_buf[i] < file_pos_buf[j]; });
}

Status DBImpl::ReWriteValue(std::vector<std::string>& v_buf, std::vector<SequenceNumber>& seq_buf,
                            std::vector<std::string>& ptr_buf, const size_t count,
                            std::tr1::unordered_map<uint32_t, uint64_t>& vlog_rewrite, uint64_t& check_numb,
                            uint64_t& check_offset) {
    Status status;
    uint64_t total_size = 4 + count * 8;
    ptr_buf.resize(count);
    for (size_t i = 0; i < count; i++) {
        total_size += v_buf[i].length();
    }

    int head_size = 0;
    uint64_t file_numb, head;
    // std::cout << "ReWriteValue sort_vlog_head_ start=" << sort_vlog_head_ << "\n";
    {
        MutexLock l(&sort_mutex_);
        if (total_size <= 409600) {
            char buf[total_size];
            EncodeFixed32(buf, count);
            uint64_t cur_idx = 4;
            for (size_t i = 0; i < count; i++) {
                EncodeFixed64(buf + cur_idx, seq_buf[i]);
                cur_idx += 8;
                for (size_t j = 0; j < v_buf[i].length(); j++) {
                    buf[cur_idx++] = v_buf[i][j];
                }
            }
            status = sort_vlog_->AddRecord(Slice(buf, total_size), head_size);
        } else {
            char* buf = new char[total_size];
            EncodeFixed32(buf, count);
            uint64_t cur_idx = 4;
            for (size_t i = 0; i < count; i++) {
                EncodeFixed64(buf + cur_idx, seq_buf[i]);
                cur_idx += 8;
                for (size_t j = 0; j < v_buf[i].length(); j++) {
                    buf[cur_idx++] = v_buf[i][j];
                }
            }
            status = sort_vlog_->AddRecord(Slice(buf, total_size), head_size);
            delete[] buf;
        }
        if (!status.ok()) return status;

        head = sort_vlog_head_ + head_size + 4;
        sort_vlog_head_ += total_size + head_size;
        file_numb = sort_logfile_number_;
        vlog_rewrite[sort_logfile_number_] += total_size + head_size;

        if (sort_vlog_head_ >= options_.max_vlog_size) {
            mutex_.Lock();
            WritableFile* vlfile;
            sort_logfile_number_ = versions_->NewSortVlogNumber();
            status = env_->NewWritableFile(VLogFileName(dbname_, sort_logfile_number_), &vlfile);
            if (!status.ok()) {
                mutex_.Unlock();
                return status;
            }
            sort_vlog_head_ = 0;
            delete sort_vlog_;
            delete sort_vlogfile_;
            sort_vlogfile_ = vlfile;
            sort_vlog_ = new log::VWriter(vlfile);
            SequentialFile* vlr_file;
            status = options_.env->NewSequentialFile(VLogFileName(dbname_, sort_logfile_number_), &vlr_file);
            log::VReader* vlog_reader = new log::VReader(vlr_file, true, 0);
            vlog_manager_.AddVlog(sort_logfile_number_, vlog_reader);
            mutex_.Unlock();
        }
        if (!status.ok()) return status;
        check_numb = sort_logfile_number_;
        check_offset = sort_vlog_head_;
    }

    // std::cout << "ReWriteValue sort_vlog_head_ start=" << head << "\n";

    for (size_t i = 0; i < count; i++) {
        head += 8;
        PutVarint64(&ptr_buf[i], v_buf[i].length());
        PutVarint32(&ptr_buf[i], file_numb);
        PutVarint64(&ptr_buf[i], head);
        head += v_buf[i].length();
    }

    // std::cout << "ReWriteValue " << check_numb << " " << check_offset << "\n";

    return status;
}

Status DBImpl::ReWriteValue(
    std::vector<std::string>& v_buf, std::vector<SequenceNumber>& seq_buf, std::vector<size_t>& rewrite_idx,
    std::vector<uint64_t>& size_buf, std::vector<std::pair<uint32_t, uint64_t>>& file_pos_buf,
    std::tr1::unordered_map<uint32_t, uint64_t>& vlog_drop_count,
    std::tr1::unordered_map<uint32_t, std::tr1::unordered_map<uint64_t, uint64_t>>& vlog_drop_offset,
    std::tr1::unordered_map<uint32_t, uint64_t>& vlog_rewrite, uint64_t& check_numb, uint64_t& check_offset) {
    Status status;
    size_t count = rewrite_idx.size();
    std::vector<uint64_t> rewrite_size(count);
    std::vector<std::pair<uint32_t, uint64_t>> rewrite_file_pos(count);
    std::vector<uint64_t> rewrite_off(count);
    uint64_t total_size = 4;
    for (size_t i = 0; i < count; i++) {
        rewrite_size[i] = size_buf[rewrite_idx[i]];
        rewrite_file_pos[i] = file_pos_buf[rewrite_idx[i]];
        rewrite_off[i] = total_size;
        total_size += size_buf[rewrite_idx[i]] + 8;
        reorder_rewrite_ += size_buf[rewrite_idx[i]];
    }
    std::vector<size_t> idx;
    SortFilePos(rewrite_file_pos, idx);

    int head_size = 0;
    uint64_t file_numb, head;
    {
        MutexLock l(&sort_mutex_);
        rewrite_count_ += total_size;
        if (total_size <= 409600) {
            char buf[total_size];
            EncodeFixed32(buf, count);
            for (size_t i = 0; i < count; i++) {
                EncodeFixed64(buf + rewrite_off[idx[i]], seq_buf[rewrite_idx[idx[i]]]);
                vlog_manager_.GetVlog(rewrite_file_pos[idx[i]].first)
                    ->Read(buf + rewrite_off[idx[i]] + 8, rewrite_size[idx[i]], rewrite_file_pos[idx[i]].second);
            }
            status = sort_vlog_->AddRecord(Slice(buf, total_size), head_size);
        } else {
            char* buf = new char[total_size];
            EncodeFixed32(buf, count);
            for (size_t i = 0; i < count; i++) {
                EncodeFixed64(buf + rewrite_off[idx[i]], seq_buf[rewrite_idx[idx[i]]]);
                vlog_manager_.GetVlog(rewrite_file_pos[idx[i]].first)
                    ->Read(buf + rewrite_off[idx[i]] + 8, rewrite_size[idx[i]], rewrite_file_pos[idx[i]].second);
            }
            status = sort_vlog_->AddRecord(Slice(buf, total_size), head_size);
            delete[] buf;
        }
        if (!status.ok()) return status;

        head = sort_vlog_head_ + head_size + 4;
        sort_vlog_head_ += total_size + head_size;
        file_numb = sort_logfile_number_;
        vlog_rewrite[sort_logfile_number_] += total_size + head_size;
        if (sort_vlog_head_ >= options_.max_vlog_size) {
            mutex_.Lock();
            WritableFile* vlfile;
            sort_logfile_number_ = versions_->NewSortVlogNumber();
            status = env_->NewWritableFile(VLogFileName(dbname_, sort_logfile_number_), &vlfile);
            if (!status.ok()) {
                mutex_.Unlock();
                return status;
            }
            sort_vlog_head_ = 0;
            delete sort_vlog_;
            delete sort_vlogfile_;
            sort_vlogfile_ = vlfile;
            sort_vlog_ = new log::VWriter(vlfile);
            SequentialFile* vlr_file;
            status = options_.env->NewSequentialFile(VLogFileName(dbname_, sort_logfile_number_), &vlr_file);
            log::VReader* vlog_reader = new log::VReader(vlr_file, true, 0);
            vlog_manager_.AddVlog(sort_logfile_number_, vlog_reader);
            mutex_.Unlock();
        }
        if (!status.ok()) return status;
        check_numb = sort_logfile_number_;
        check_offset = sort_vlog_head_;
    }

    for (size_t i = 0; i < count; i++) {
        head += 8;
        std::string v;
        PutVarint64(&v, rewrite_size[i]);
        PutVarint32(&v, file_numb);
        PutVarint64(&v, head);
        v_buf[rewrite_idx[i]] = v;
        head += rewrite_size[i];
        vlog_drop_count[rewrite_file_pos[i].first] += rewrite_size[i];
        vlog_drop_offset[rewrite_file_pos[i].first][rewrite_file_pos[i].second] = rewrite_size[i];
    }
    return status;
}

void DBImpl::MergeRange(std::vector<uint64_t>& file_numb,
                        std::vector<std::pair<std::string, std::string>>& resort_range,
                        std::vector<std::tr1::unordered_set<uint64_t>>& range_file) {
    size_t file_count = file_numb.size();

    std::vector<std::vector<std::pair<std::string, std::string>>> tmp_range(file_count);
    std::vector<std::vector<std::pair<std::string, std::string>>> file_range(file_count);
    std::tr1::unordered_set<std::string> bound_set;
    for (size_t i = 0; i < file_count; i++) {
        ReduceRange(file_numb[i], tmp_range[i], bound_set);
    }
    if (bound_set.empty()) return;
    std::vector<std::string> bound_vec;
    bound_vec.assign(bound_set.begin(), bound_set.end());
    std::sort(bound_vec.begin(), bound_vec.end());
    for (size_t i = 0; i < file_count; i++) {
        PartitionRange(tmp_range[i], bound_vec, file_range[i]);
    }
    std::vector<size_t> idx(file_count, 0);
    for (size_t i = 0; i < bound_vec.size() - 1; i++) {
        std::vector<uint64_t> tmp_files;
        for (size_t j = 0; j < file_count; j++) {
            if (idx[j] == file_range[j].size()) {
                continue;
            }
            if (file_range[j][idx[j]].first == bound_vec[i]) {
                tmp_files.push_back(file_numb[j]);
                idx[j]++;
            }
        }
        if (tmp_files.size()) {
            resort_range.emplace_back(std::make_pair(bound_vec[i], bound_vec[i + 1]));
            range_file.emplace_back(std::tr1::unordered_set<uint64_t>(tmp_files.begin(), tmp_files.end()));
        }
    }
}

void DBImpl::PartitionRange(std::vector<std::pair<std::string, std::string>>& tmp_range,
                            std::vector<std::string>& bound_vec,
                            std::vector<std::pair<std::string, std::string>>& file_range) {
    if (tmp_range.size() == 0) return;
    for (size_t i = 0, j = 0; i < tmp_range.size();) {
        if (j == bound_vec.size()) {
            file_range.emplace_back(tmp_range[i]);
            i++;
        } else {
            if (tmp_range[i].first >= bound_vec[j]) {
                j++;
            } else {
                file_range.emplace_back(std::make_pair(tmp_range[i].first, bound_vec[j]));
                if (tmp_range[i].second > bound_vec[j]) {
                    tmp_range[i].first = bound_vec[j];
                    j++;
                } else {
                    i++;
                }
            }
        }
    }
}

void DBImpl::ReduceRange(uint64_t file_numb, std::vector<std::pair<std::string, std::string>>& tmp_range,
                         std::tr1::unordered_set<std::string>& bound_set) {
    if (disperse_range_[file_numb].empty()) return;
    sort(disperse_range_[file_numb].begin(), disperse_range_[file_numb].end());
    auto last_range = disperse_range_[file_numb][0];
    std::string cur_bound = disperse_range_[file_numb][0].first;
    for (size_t i = 1; i < disperse_range_[file_numb].size(); i++) {
        int res = internal_comparator_.Compare(Slice(last_range.second), Slice(disperse_range_[file_numb][i].first));
        if (last_range.second <= disperse_range_[file_numb][i].first) {
            last_range = disperse_range_[file_numb][i];
        } else {
            if (disperse_range_[file_numb][i].second > cur_bound) {
                std::string left_bound = max(cur_bound, disperse_range_[file_numb][i].first);
                tmp_range.emplace_back(std::make_pair(left_bound, disperse_range_[file_numb][i].second));
                bound_set.insert(left_bound);
                bound_set.insert(disperse_range_[file_numb][i].second);
                cur_bound = disperse_range_[file_numb][i].second;
            }
            if (last_range.second < disperse_range_[file_numb][i].second) {
                last_range.second = disperse_range_[file_numb][i].second;
            }
        }
        // if (last_range.second < disperse_range_[file_numb][i].first) {
        //     tmp_range.emplace_back(last_range);
        //     bound_set.insert(last_range.first);
        //     bound_set.insert(last_range.second);
        //     last_range = disperse_range_[file_numb][i];
        // } else if (last_range.second < disperse_range_[file_numb][i].second) {
        //     last_range.second = disperse_range_[file_numb][i].second;
        // }
    }
    // tmp_range.emplace_back(last_range);
    // bound_set.insert(last_range.first);
    // bound_set.insert(last_range.second);
}

Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) { return DB::Put(o, key, val); }

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) { return DB::Delete(options, key); }

Status DBImpl::Scan(const ReadOptions& options, const Slice& start, const Slice& end, std::vector<std::string>& key_buf,
                    std::vector<std::string>& value_buf, int* vlog_count, std::vector<int>* vlogs) {
    std::vector<std::string> k_buf;
    std::vector<uint64_t> size_buf;
    std::vector<std::pair<uint32_t, uint64_t>> file_pos_buf;
    std::vector<uint64_t> file_numb_buf;
    int count = 0;
    Status status;
    Iterator* it = NewIterator(options);
    for (it->Seek(start); it->Valid() && end.compare(it->key()) >= 0; it->Next()) {
        count++;
        uint64_t size;
        std::pair<uint32_t, uint64_t> file_pos;
        Slice val_ptr = it->value();
        status = ParsePtr(val_ptr, size, file_pos);
        if (!status.ok()) {
            delete it;
            return status;
        }

        k_buf.emplace_back(it->key().ToString());
        size_buf.emplace_back(size);
        file_pos_buf.emplace_back(file_pos);
        file_numb_buf.emplace_back(it->fileNumber());
    }
    size_t value_count = k_buf.size();
    std::vector<size_t> idx;
    SortFilePos(file_pos_buf, idx);

    std::tr1::unordered_map<uint64_t, std::pair<uint32_t, uint64_t>> file_last_visit;
    std::tr1::unordered_map<uint64_t, uint64_t> visit_count;
    std::tr1::unordered_map<uint64_t, uint64_t> block_count;
    std::tr1::unordered_map<uint64_t, std::pair<std::string, std::string>> file_min_max;

    for (size_t i = 0; i < value_count; i++) {
        uint64_t file_numb = file_numb_buf[idx[i]];
        if (i != 0) {
            if (!(file_pos_buf[idx[i]].first == file_pos_buf[idx[i - 1]].first &&
                  (file_pos_buf[idx[i]].second + size_buf[idx[i]]) / VlogBlockSize ==
                      file_pos_buf[idx[i - 1]].second / VlogBlockSize) &&
                !(visit_count.count(file_numb) > 0 && file_pos_buf[idx[i]].first == file_last_visit[file_numb].first &&
                  (file_pos_buf[idx[i]].second + size_buf[idx[i]]) / VlogBlockSize ==
                      file_last_visit[file_numb].second / VlogBlockSize)) {
                block_count[file_numb]++;
            }
        }
        if (file_last_visit.count(file_numb) > 0) {
            if (k_buf[idx[i]] < file_min_max[file_numb].first) file_min_max[file_numb].first = k_buf[idx[i]];
            if (k_buf[idx[i]] > file_min_max[file_numb].second) file_min_max[file_numb].second = k_buf[idx[i]];
        } else {
            file_min_max[file_numb].first = k_buf[idx[i]];
            file_min_max[file_numb].second = k_buf[idx[i]];
            block_count[file_numb] = 1;
        }
        visit_count[file_numb]++;
        file_last_visit[file_numb] = file_pos_buf[idx[i]];
    }

    bool maybeCompact = false;
    mutex_.Lock();
    for (auto iter = visit_count.begin(); iter != visit_count.end(); iter++) {
        if (iter->first <= 1 || versions_->file_maps_.count(iter->first) == 0 ||
            iter->second < std::min(scan_len, scan_minimum_visit_L0_level +
                                                  versions_->file_maps_[iter->first] * scan_minimum_visit_per_level))
            continue;
        if (block_count[iter->first] >= scan_resort_rate) {
            disperse_range_[iter->first].emplace_back(file_min_max[iter->first]);
            if (disperse_range_[iter->first].size() >
                resort_minumum_disperse_L0_level +
                    versions_->file_maps_[iter->first] * resort_minumum_disperse_per_level) {
                maybeCompact = true;
                versions_->resort_set_.insert(iter->first);
            }
        }
    }

    if (maybeCompact) {
        MaybeScheduleCompaction();
    }
    mutex_.Unlock();

    value_buf.resize(value_count);
    size_t i = 0, j = 1;
    for (; j <= value_count; j++) {
        if (j < value_count && file_pos_buf[idx[j]].first == file_pos_buf[idx[j - 1]].first &&
            file_pos_buf[idx[j]].second - file_pos_buf[idx[j - 1]].second - test_kv_size < VlogBlockSize) {
            continue;
        }

        size_t size = file_pos_buf[idx[j - 1]].second - file_pos_buf[idx[i]].second + size_buf[idx[i]];
        uint64_t begin_pos = file_pos_buf[idx[i]].second;
        char buf[size];
        if (vlog_count) {
            (*vlog_count)++;
        }
        auto vlog_reader = vlog_manager_.GetVlog(file_pos_buf[idx[i]].first);

        if (vlog_reader == NULL) {
            mutex_.Lock();
            auto vlog_set = vlog_manager_.GetAllVlog();
            std::cout << "vlog_set=";
            for (auto iit = vlog_set.begin(); iit != vlog_set.end(); iit++) {
                std::cout << *iit << "-" << vlog_refs_[*iit] << " ";
            }
            std::cout << "\n";
            std::cout << "vlog_delete=";
            for (auto iit = vlog_delete_.begin(); iit != vlog_delete_.end(); iit++) {
                std::cout << *iit << " ";
            }
            std::cout << "\n";
            std::cout << "not null reader=";
            for (auto iit = vlog_set.begin(); iit != vlog_set.end(); iit++) {
                // if (*iit == file_pos_buf[idx[i]].first) continue;
                vlog_reader = vlog_manager_.GetVlog(*iit);
                if (vlog_reader != NULL) {
                    std::cout << *iit << " ";
                }
            }
            std::cout << "\n";
            std::cout << "scan null reader " << file_pos_buf[idx[i]].first << " -----------------\n ";
            mutex_.Unlock();
            status = Status::Corruption("corrupted key for null reader");
            if (!status.ok()) {
                delete it;
                return status;
            }
        }

        bool b = vlog_reader->Read(buf, size, begin_pos);

        while (i < j) {
            Slice input(buf + file_pos_buf[idx[i]].second - begin_pos, size_buf[idx[i]]);
            Slice k, v;
            char tag = input[0];
            input.remove_prefix(1);
            switch (tag) {
                case kTypeValue:
                    if (GetLengthPrefixedSlice(&input, &k) && GetLengthPrefixedSlice(&input, &v)) {
                        value_buf[idx[i]].assign(v.data(), v.size());
                    } else {
                        mutex_.Lock();
                        auto vlog_set = vlog_manager_.GetAllVlog();
                        std::cout << "vlog_set=";
                        for (auto iit = vlog_set.begin(); iit != vlog_set.end(); iit++) {
                            std::cout << *iit << "-" << vlog_refs_[*iit] << " ";
                        }
                        std::cout << "\n";
                        std::cout << "vlog_delete=";
                        for (auto iit = vlog_delete_.begin(); iit != vlog_delete_.end(); iit++) {
                            std::cout << *iit << " ";
                        }
                        std::cout << "\n";
                        mutex_.Unlock();
                        status = Status::Corruption("corrupted key for 1");
                    }
                    break;
                default:
                    mutex_.Lock();
                    auto vlog_set = vlog_manager_.GetAllVlog();
                    std::cout << "vlog_set=";
                    for (auto iit = vlog_set.begin(); iit != vlog_set.end(); iit++) {
                        std::cout << *iit << "-" << vlog_refs_[*iit] << " ";
                    }
                    std::cout << "\n";
                    std::cout << "vlog_delete=";
                    for (auto iit = vlog_delete_.begin(); iit != vlog_delete_.end(); iit++) {
                        std::cout << *iit << " ";
                    }
                    std::cout << "\n";
                    mutex_.Unlock();
                    status = Status::Corruption("corrupted key for 2");
            }
            i++;
            if (!status.ok()) {
                delete it;
                return status;
            }
        }
    }

    delete it;
    return status;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value,
                   std::vector<uint64_t>* visited_file) {
    Status status;
    std::string val;
    std::tr1::unordered_set<uint64_t> vlog_record;
    status = GetPtr(options, key, &val, &vlog_record, visited_file);
    if (status.ok()) {
        uint64_t size;
        std::pair<uint32_t, uint64_t> file_pos;
        Slice val_ptr(val);
        status = ParsePtr(val_ptr, size, file_pos);
        if (status.ok()) {
            status = RealValue(size, file_pos.first, file_pos.second, value, true, visited_file);
        }
    }

    mutex_.Lock();
    for (auto iter = vlog_record.begin(); iter != vlog_record.end(); iter++) {
        vlog_refs_[*iter]--;
    }
    mutex_.Unlock();

    return status;
}

Status DBImpl::GetPtr(const ReadOptions& options, const Slice& key, std::string* value,
                      std::tr1::unordered_set<uint64_t>* vlog_record, std::vector<uint64_t>* visited_file) {
    Status status;
    MutexLock l(&mutex_);
    SequenceNumber snapshot;
    snapshot = versions_->LastSequence();

    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = versions_->current();
    auto gc_file_set = versions_->full_gc_set_;
    mem->Ref();
    if (imm != NULL) {
        imm->Ref();
        if (vlog_record && visited_file) {
            visited_file->push_back(1);
        }
    }
    current->Ref();

    if (vlog_record) {
        std::set<uint32_t> all_set = vlog_manager_.GetAllVlog();
        for (auto iter = all_set.begin(); iter != all_set.end(); iter++) {
            if (vlog_delete_.count(*iter) == 0) {
                vlog_refs_[*iter]++;
                vlog_record->insert(*iter);
            }
        }
    }

    bool have_stat_update = false;
    Version::GetStats stats;

    // Unlock while reading from files and memtables
    {
        mutex_.Unlock();
        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &status)) {
            // Done
        } else if (imm != NULL && imm->Get(lkey, value, &status)) {
            // Done
        } else {
            status = current->Get(options, lkey, value, &stats, &gc_file_set, visited_file);
            // have_stat_update = true;
        }
        mutex_.Lock();
    }

    // if (have_stat_update && current->UpdateStats(stats)) {
    //     MaybeScheduleCompaction();
    // }

    mem->Unref();
    if (imm != NULL) imm->Unref();

    current->Unref();

    return status;
}

Status DBImpl::RealValue(Slice val_ptr, std::string* value) {
    uint64_t size;
    std::pair<uint32_t, uint64_t> file_pos;
    Status status = ParsePtr(val_ptr, size, file_pos);
    if (status.ok()) {
        status = RealValue(size, file_pos.first, file_pos.second, value);
    }
    return status;
}

Status DBImpl::RealValue(uint64_t size, uint32_t file_numb, uint64_t pos, std::string* value, bool print,
                         std::vector<uint64_t>* visited_file) {
    // if (print) std::cout << "start read real value\n";
    // if (print) std::cout << "111111111111111111111111  " << file_numb << " " << pos << " " << size << "  \n";
    log::VReader* vlog_reader = vlog_manager_.GetVlog(file_numb);
    Status status;
    if (size <= 409600) {
        char buf[size];
        if (print) {
            if (vlog_reader == NULL) {
                std::cout << "null reader " << file_numb << " " << pos << " " << size << "\n";
                // std::cout << "read sstable=";
                // for (auto iter = visited_file->begin(); iter != visited_file->end(); iter++) {
                //     std::cout << *iter << " ";
                // }
                // std::cout << "\n";
                if (gc_rewrite_off[file_numb].count(pos) > 0) {
                    std::cout << "value has been rewrited " << gc_rewrite_off[file_numb][pos] << "\n";
                    return Status::OK();
                    // } else if (gc_skip_off[file_numb].count(pos) > 0) {
                    //     std::cout << "value has been skiped " << gc_skip_off[file_numb].size() << "\n";
                    // } else if (gc_delete_off[file_numb].count(pos) > 0) {
                    //     std::cout << "value has been deleted " << gc_delete_off[file_numb].size() << "\n";
                } else {
                    // std::cout << "value has not been rewrited or skiped or deleted " <<
                    // gc_rewrite_off[file_numb].size()
                    //           << "\n";
                }
            }
        }
        bool b = vlog_reader->Read(buf, size, pos);
        assert(b);
        Slice input(buf, size);
        Slice k, v;
        char tag = input[0];
        input.remove_prefix(1);
        switch (tag) {
            case kTypeValue:
                if (GetLengthPrefixedSlice(&input, &k) && GetLengthPrefixedSlice(&input, &v)) {
                    value->assign(v.data(), v.size());
                } else {
                    status = Status::Corruption("corrupted key 1 for ");
                }
                break;
            default:
                std::cout << "111111111111111111111111 corrupted " << file_numb << " " << pos << " " << size << "  \n";
                status = Status::Corruption("corrupted key 2 for ");
        }
    } else { // 如果size太大，栈空间不够，就需要用堆来存放
        char* buf = new char[size];
        bool b = vlog_reader->Read(buf, size, pos);
        assert(b);
        Slice input(buf, size);
        Slice k, v;
        char tag = input[0];
        input.remove_prefix(1);
        switch (tag) {
            case kTypeValue:
                if (GetLengthPrefixedSlice(&input, &k) && GetLengthPrefixedSlice(&input, &v)) {
                    value->assign(v.data(), v.size());
                } else {
                    status = Status::Corruption("corrupted key 3 for ");
                }
                break;
            default:
                status = Status::Corruption("corrupted key 4 for ");
        }
        delete[] buf;
    }
    // if (print) std::cout << "finish read real value\n";

    return status;
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
    Writer w(&mutex_);
    w.batch = my_batch;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&mutex_);
    writers_.push_back(&w);
    while (!w.done && &w != writers_.front()) {
        w.cv.Wait();
    }
    if (w.done) {
        return w.status;
    }
    // May temporarily unlock and wait.
    Status status = MakeRoomForWrite(my_batch == NULL);
    uint64_t last_sequence = versions_->LastSequence();
    Writer* last_writer = &w;
    if (status.ok() && my_batch != NULL) { // NULL batch is for compactions
        WriteBatch* updates = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(updates, last_sequence + 1);
        last_sequence += WriteBatchInternal::Count(updates);

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem_.
        {
            mutex_.Unlock();
            int head_size = 0;
            status = vlog_->AddRecord(WriteBatchInternal::Contents(updates), head_size);
            bool sync_error = false;
            if (status.ok() && options.sync) {
                status = vlogfile_->Sync();
                if (!status.ok()) {
                    sync_error = true;
                }
            }
            vlog_head_ += head_size;
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(updates, mem_, vlog_head_,
                                                        logfile_number_); // vlog_head_代表每条kv对在vlog中的位置
            }
            mutex_.Lock();
            if (sync_error) {
                // The state of the log file is indeterminate: the log record we
                // just added may or may not show up when the DB is re-opened.
                // So we force the DB into a mode where all future writes fail.
                RecordBackgroundError(status);
            }
        }
        if (updates == tmp_batch_) tmp_batch_->Clear();

        versions_->SetLastSequence(last_sequence);
    }

    while (true) {
        Writer* ready = writers_.front();
        writers_.pop_front();
        if (ready != &w) {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }

    // Notify new head of write queue
    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }

    return status;
}

WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
    assert(!writers_.empty());
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;
    assert(result != NULL);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth so we do not slow
    // down the small write too much.
    size_t max_size = 1 << 20;
    if (size <= (128 << 10)) {
        max_size = size + (128 << 10);
    }

    *last_writer = first;
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter; // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
        Writer* w = *iter;
        if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
        }

        if (w->batch != NULL) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;
    }
    return result;
}

Status DBImpl::MakeRoomForWrite(bool force) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    bool allow_delay = !force;
    Status status;

    if (vlog_head_ >= options_.max_vlog_size) {
        // 新生成的vlog文件的编号会和imm生成的sst文件一起应用到version中，见CompactMemTable
        uint32_t new_log_number =
            versions_->NewVlogNumber(); // 对于newdb且不能重用上次的log(即不能logandapply新生成的log)，会有bug

        vlog_head_ = 0;
        WritableFile* vlfile;
        status = env_->NewWritableFile(VLogFileName(dbname_, new_log_number), &vlfile);
        if (!status.ok()) {
            versions_->ReuseVlogNumber(new_log_number);
            return status;
        }
        delete vlog_;
        delete vlogfile_;
        vlogfile_ = vlfile;
        logfile_number_ = new_log_number;
        vlog_ = new log::VWriter(vlfile);
        SequentialFile* vlr_file;
        status = options_.env->NewSequentialFile(VLogFileName(dbname_, new_log_number), &vlr_file);
        log::VReader* vlog_reader = new log::VReader(vlr_file, true, 0);
        vlog_manager_.AddVlog(new_log_number, vlog_reader);
        vlog_manager_.SetNowVlog(new_log_number);
    }

    while (true) {
        if (!bg_error_.ok()) {
            // Yield previous error
            status = bg_error_;
            break;
        } else if (allow_delay && versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
            // We are getting close to hitting a hard limit on the number of
            // L0 files.  Rather than delaying a single write by several
            // seconds when we hit the hard limit, start delaying each
            // individual write by 1ms to reduce latency variance.  Also,
            // this delay hands over some CPU to the compaction thread in
            // case it is sharing the same core as the writer.
            // std::cout << "SleepForMicroseconds(1000)...............\n";
            mutex_.Unlock();
            env_->SleepForMicroseconds(1000);
            allow_delay = false; // Do not delay a single write more than once
            mutex_.Lock();
        } else if (!force && (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
            // There is room in current memtable
            break;
        } else if (imm_ != NULL) {
            // We have filled up the current memtable, but the previous
            // one is still being compacted, so we wait.
            Log(options_.info_log, "Current memtable full; waiting...\n");
            std::cout << "Current memtable full; waiting...............\n";
            bg_cv_.Wait();
            // std::cout << "signal from Current memtable full..................\n";
        } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
            // There are too many level-0 files.
            Log(options_.info_log, "Too many L0 files; waiting...............\n");
            std::cout << "Too many L0 files; waiting......................\n";
            bg_cv_.Wait();
        } else {
            // Attempt to switch to a new memtable and trigger compaction of old
            // assert(versions_->PrevLogNumber() == 0);
            imm_ = mem_;
            uint64_t last_sequence = versions_->LastSequence();
            flushed_seq = last_sequence;
            flushed_vlog = logfile_number_;
            last_sequence++;
            versions_->SetLastSequence(last_sequence);
            check_point_ = vlog_head_;
            check_log_ = logfile_number_;
            has_imm_.Release_Store(imm_);
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
            force = false; // Do not force another compaction if have room
            MaybeScheduleCompaction();
        }
    }
    return status;
}

void DBImpl::MaybeScheduleCompaction() {
    mutex_.AssertHeld();
    // while (bg_compaction_scheduled_ == false && bg_clean_scheduled_ == true && full_gc_ != NULL &&
    //        full_gc_->finished_) {
    //     bg_cv_.SignalAll();
    //     bg_cv_.Wait();
    // }
    if (bg_compaction_scheduled_) {
        // Already scheduled
    } else if (shutting_down_.Acquire_Load()) {
        // DB is being deleted; no more background compactions
    } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
    } else if (imm_ == NULL && manual_compaction_ == NULL && !versions_->NeedsCompaction()) {
        // No work to be done
    } else {
        bg_compaction_scheduled_ = true;
        env_->Schedule(&DBImpl::BGWork, this);
    }
}

void DBImpl::RecordBackgroundError(const Status& status) {
    mutex_.AssertHeld();
    if (bg_error_.ok()) {
        bg_error_ = status;
        // bg_cv_.SignalAll();
    }
}

void DBImpl::DeleteObsoleteFiles() {
    if (!bg_error_.ok()) {
        // After a background error, we don't know whether a new version may
        // or may not have been committed, so we cannot safely garbage collect.
        return;
    }

    // Make a set of all of the live files
    std::set<uint64_t> live = pending_outputs_;
    versions_->AddLiveFiles(&live);

    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
    uint64_t number;
    FileType type;
    // std::cout << "delete file=";
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            bool keep = true;
            switch (type) {
                case kLogFile:
                    keep = ((number >= versions_->LogNumber()) || (number == versions_->PrevLogNumber()));
                    break;
                case kDescriptorFile:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions_->ManifestFileNumber());
                    break;
                case kTableFile:
                    // keep = ((live.find(number) != live.end()) || (sstable_refs_[number] > 0));
                    keep = (live.find(number) != live.end());
                    break;
                case kTempFile:
                    // keep = ((live.find(number) != live.end()) || (sstable_refs_[number] > 0));
                    keep = (live.find(number) != live.end());
                    break;
                case kCurrentFile:
                    break;
                case kDBLockFile:
                    break;
                case kInfoLogFile:
                    break;
                case kVLogFile:
                    keep = (vlog_delete_.find(number) == vlog_delete_.end() || vlog_refs_[number] > 0);
                    break;
            }

            if (!keep) {
                if (type == kTableFile) {
                    table_cache_->Evict(number);
                    env_->DeleteFile(dbname_ + "/" + filenames[i]);
                }
                if (type == kVLogFile) {
                    env_->DeleteFile(VLogFileName(dbname_, number));
                    vlog_manager_.RemoveVlog(number);
                }
            }
        }
    }
    // std::cout << "\n";
}

void DBImpl::BGWork(void* db) { reinterpret_cast<DBImpl*>(db)->BackgroundCall(); }

void DBImpl::BackgroundCall() {
    MutexLock l(&mutex_);
    assert(bg_compaction_scheduled_);
    if (shutting_down_.Acquire_Load()) {
        // No more background work when shutting down.
    } else if (!bg_error_.ok()) {
        // No more background work after a background error.
    } else {
        // std::cout << "BackgroundCompaction start\n";
        BackgroundCompaction();
        // std::cout << "BackgroundCompaction finish vlog=" << logfile_number_ << " vlog_drop=";
        // for (auto it = vlog_manager_.manager_.begin(); it != vlog_manager_.manager_.end(); it++) {
        //     std::cout << " " << it->first << " " << it->second.count_;
        // }
        // std::cout << "\n";
    }

    bg_compaction_scheduled_ = false;
    // while (bg_clean_scheduled_ == true && full_gc_ != NULL && full_gc_->finished_) {
    //     bg_cv_.SignalAll();
    //     std::cout << "wait for clean finish\n";
    //     bg_cv_.Wait();
    //     std::cout << "signal from clean finish\n";
    // }
    MaybeScheduleCompaction();
    bg_cv_.SignalAll();
}

Status DBImpl::FullGCCompaction(const uint64_t check_numb, const uint64_t check_offset, uint64_t* flushed_file) {
    // std::cout << "run FullGCCompaction\n";
    MutexLock l(&mutex_);
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    uint64_t file_numb;
    bool is_gc = true;
    Status status = WriteLevel0Table(full_gc_->mem_, &edit, base, &file_numb, &is_gc);

    if (status.ok() && shutting_down_.Acquire_Load()) {
        status = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (status.ok()) {
        *flushed_file = file_numb;
        if (check_numb > 0) {
            edit.SetSortHeadInfo(check_numb, check_offset);
        }
        edit.SetTailInfo(full_gc_->vlog_number_, full_gc_->garbage_pos_);
        edit.SetFullGcFile(file_numb);
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(logfile_number_);
        status = versions_->LogAndApply(&edit, &mutex_);
    }
    if (status.ok()) {
        versions_->full_gc_set_.insert(file_numb);
        full_gc_->flushed_files_.insert(file_numb);
        pending_outputs_.erase(file_numb);
        MaybeScheduleCompaction();
        // DeleteObsoleteFiles();
    }
    base->Unref();
    // PrintFiles();

    return status;
}

void DBImpl::BackgroundCompaction() {
    mutex_.AssertHeld();
    // std::cout << "start BackgroundCompaction\n";
    if (imm_ != NULL) {
        CompactMemTable();
        return;
    }

    Compaction* c;
    bool is_manual = (manual_compaction_ != NULL);
    InternalKey manual_end;
    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        c = versions_->CompactRange(m->level, m->begin, m->end);
        m->done = (c == NULL);
        if (c != NULL) {
            manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
        }
        Log(options_.info_log, "Manual compaction at level-%d from %s .. %s; will stop at %s\n", m->level,
            (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
            (m->end ? m->end->DebugString().c_str() : "(end)"), (m->done ? "(end)" : manual_end.DebugString().c_str()));
    } else {
        c = versions_->PickCompaction();
    }
    // std::cout << "finish PickCompaction\n";
    Status status;
    bool finished = false;
    if (c == NULL) {
        finished = true;
    } else if (!is_manual && c->IsTrivialMove()) {
        // std::cout << "start TrivialMove\n";
        // Move file to next level
        assert(c->num_input_files(0) == 1);
        FileMetaData* f = c->input(0, 0);
        if (disperse_range_[f->number].empty()) {
            finished = true;
            c->edit()->DeleteFile(c->level(), f->number);
            c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest, f->largest);
            status = versions_->LogAndApply(c->edit(), &mutex_);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            versions_->file_maps_[f->number] = c->level() + 1;
        }
        // std::cout << "finish TrivialMove\n";
    }
    if (!finished) {
        // std::cout << "start DoCompactionWork\n";
        CompactionState* compact = new CompactionState(c);
        status = DoCompactionWork(compact);
        // std::cout << "finish DoCompactionWork\n";
        if (!status.ok()) {
            RecordBackgroundError(status);
        } else {
            for (size_t i = 0; i < 2; i++) {
                for (size_t j = 0; j < c->num_input_files(i); j++) {
                    versions_->file_maps_.erase(c->input(i, j)->number);
                    versions_->resort_set_.erase(c->input(i, j)->number);
                    disperse_range_.erase(c->input(i, j)->number);
                    if (i == 0 && c->level() == 0) {
                        versions_->full_gc_set_.erase(c->input(i, j)->number);
                    }
                }
            }
            for (size_t i = 0; i < compact->outputs.size(); i++) {
                versions_->file_maps_[compact->outputs[i].number] = c->level() + 1;
            }
        }
        CleanupCompaction(compact);
        c->ReleaseInputs();
        DeleteObsoleteFiles();
        // std::cout << "DeleteObsoleteFiles() finish\n";
    }

    delete c;
    if (status.ok()) {
        // Done
    } else if (shutting_down_.Acquire_Load()) {
        // Ignore compaction errors found during shutting down
    } else {
        Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
    }

    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        if (!status.ok()) {
            m->done = true;
        }
        if (!m->done) {
            // We only compacted part of the requested range.  Update *m
            // to the range that is left to be compacted.
            m->tmp_storage = manual_end;
            m->begin = &m->tmp_storage;
        }
        manual_compaction_ = NULL;
    }
}

void DBImpl::CompactMemTable() {
    mutex_.AssertHeld();
    // std::cout << "start CompactMemTable\n";
    assert(imm_ != NULL);

    // Save the contents of the memtable as a new Table
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    uint64_t file_numb;
    Status status = WriteLevel0Table(imm_, &edit, base, &file_numb);
    edit.SetHeadInfo(check_log_, check_point_);
    base->Unref();

    if (status.ok() && shutting_down_.Acquire_Load()) {
        status = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (status.ok()) {
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(logfile_number_); // Earlier logs no longer needed
        status = versions_->LogAndApply(&edit, &mutex_);
    }
    if (status.ok()) {
        // Commit to the new state
        imm_->Unref();
        imm_ = NULL;
        has_imm_.Release_Store(NULL);
        pending_outputs_.erase(file_numb);
        DeleteObsoleteFiles();
        // std::cout << "DeleteObsoleteFiles() finish\n";
    } else {
        RecordBackgroundError(status);
    }
    // std::cout << "finish CompactMemTable\n";
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base, uint64_t* file_numb, bool* is_gc) {
    mutex_.AssertHeld();
    Status status;
    const uint64_t start_micros = env_->NowMicros();
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();

    pending_outputs_.insert(meta.number);
    Iterator* iter = mem->NewIterator();
    mutex_.Unlock();
    status = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    if (file_numb) {
        *file_numb = meta.number;
    }

    mutex_.Lock();

    delete iter;

    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.
    int level = 0;
    CompactionStats stats;
    if (status.ok() && meta.file_size > 0) {
        const Slice min_user_key = meta.smallest.user_key();
        const Slice max_user_key = meta.largest.user_key();
        if (base != NULL) {
            level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
        }
        edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
    }
    // if (is_gc) {
    //     std::cout << "full gc ";
    // }
    // std::cout << "WriteLevel0Table start " << meta.number << " vlog=" << logfile_number_ << " level=" << level <<
    // "\n";

    stats.micros = env_->NowMicros() - start_micros;
    stats.bytes_written += meta.file_size;
    stats_[level].Add(stats);
    // std::cout << "WriteLevel0Table finish " << meta.number << " vlog=" << logfile_number_ << "\n";
    return status;
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
    mutex_.AssertHeld();
    const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0; // Micros spent doing imm_ compactions

    compact->smallest_snapshot = versions_->LastSequence();
    // std::cout << "111\n";
    std::vector<uint64_t> file_numb;
    std::vector<std::pair<std::string, std::string>> resort_range;
    std::vector<std::tr1::unordered_set<uint64_t>> range_file;
    // std::set<uint32_t> partial_set = vlog_manager_.GetPartialVlog();
    auto all_set = vlog_manager_.GetAllVlog();
    for (auto iter = all_set.begin(); iter != all_set.end(); iter++) {
        vlog_refs_[*iter]++;
    }
    std::set<uint32_t> partial_set;
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
            file_numb.push_back(compact->compaction->input(which, i)->number);
        }
    }
    int range_count = 0;
    for (size_t i = 0; i < file_numb.size(); i++) {
        range_count += disperse_range_[file_numb[i]].size();
    }
    MergeRange(file_numb, resort_range, range_file);

    Status status;
    Iterator* input;
    input = versions_->MakeInputIterator(compact->compaction);
    bool has_full_gc = false;
    uint64_t resort_count = 0;
    double reorder_size = 0;

    if (compact->compaction->level() == 0 && full_gc_ != NULL) {
        has_full_gc = true;
        mutex_.Unlock();
        pthread_mutex_lock(&full_gc_->mutex_);
        if (full_gc_->finished_ == false) {
            full_gc_->need_pause_.Release_Store(full_gc_->mem_);
            pthread_cond_wait(&full_gc_->cond_pause_, &full_gc_->mutex_);
            full_gc_->need_pause_.Release_Store(NULL);
        }
        pthread_mutex_unlock(&full_gc_->mutex_);

        mutex_.Lock();
        if (versions_->full_gc_set_.empty() == false) {
            versions_->SupplementCompaction(compact->compaction);
            // std::cout << "versions_->SupplementCompaction(compact->compaction) 1\n";
        }
        mutex_.Unlock();
        input = versions_->MakeInputIterator(compact->compaction, full_gc_->mem_->NewIterator());
    } else {
        if (versions_->full_gc_set_.empty() == false) {
            versions_->SupplementCompaction(compact->compaction);
            // std::cout << "versions_->SupplementCompaction(compact->compaction) 2\n";
        }
        mutex_.Unlock();
        input = versions_->MakeInputIterator(compact->compaction);
    }

    input->SeekToFirst();
    // std::cout << "input->SeekToFirst() finish\n";
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false; // 是否是第一次出现这个user_key
    uint64_t drop_count = 0;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    std::vector<std::string> k_buf(KVBufferSize), v_buf(KVBufferSize);
    std::vector<SequenceNumber> seq_buf(KVBufferSize);
    std::vector<uint64_t> size_buf(KVBufferSize);
    std::vector<std::pair<uint32_t, uint64_t>> file_pos_buf(KVBufferSize);
    std::tr1::unordered_map<uint32_t, uint64_t> vlog_drop_count, vlog_rewrite;
    std::tr1::unordered_map<uint32_t, std::tr1::unordered_map<uint64_t, uint64_t>> vlog_drop_offset;
    std::vector<size_t> rewrite_idx;
    Slice key, value;
    std::string user_key;
    size_t key_idx = 0, flush_idx = 0, range_idx = 0;
    bool in_resort_range = false;
    uint64_t check_numb = 0, check_offset = 0;
    // std::cout << "222\n";

    for (; input->Valid() && !shutting_down_.Acquire_Load();) {
        // Prioritize immutable compaction work
        if (has_imm_.NoBarrier_Load() != NULL) {
            const uint64_t imm_start = env_->NowMicros();
            mutex_.Lock();
            if (imm_ != NULL) {
                CompactMemTable();
                bg_cv_.SignalAll(); // Wakeup MakeRoomForWrite() if necessary
            }
            mutex_.Unlock();
            imm_micros += (env_->NowMicros() - imm_start);
        }

        bool drop = false;
        key = input->key();
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key || user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
            }

            if (last_sequence_for_key <= compact->smallest_snapshot) {
                // Hidden by an newer entry for same user key
                if (ikey.type != kTypeDeletion) {
                    value = input->value();
                    if (!value.empty()) {
                        uint64_t size;
                        std::pair<uint32_t, uint64_t> file_pos;
                        status = ParsePtr(value, size, file_pos);
                        if (!status.ok()) {
                            break;
                        }
                        vlog_drop_count[file_pos.first] += size;
                    }
                }
                drop = true;
            } else if (ikey.type == kTypeDeletion && ikey.sequence <= compact->smallest_snapshot &&
                       compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                drop = true;
            }
            last_sequence_for_key = ikey.sequence;
        }

        if (!drop) {
            k_buf[key_idx] = input->key().ToString();
            v_buf[key_idx] = input->value().ToString();
            seq_buf[key_idx] = ikey.sequence;
            if (ikey.type != kTypeDeletion) {
                value = input->value();
                status = ParsePtr(value, size_buf[key_idx], file_pos_buf[key_idx]);
                if (!status.ok()) break;
            }
            user_key = ExtractUserKey(key).ToString();
            if (!in_resort_range && range_idx != resort_range.size()) {
                in_resort_range = user_key == resort_range[range_idx].first;
            }
            if (in_resort_range) {
                if (ikey.type != kTypeDeletion && all_set.count(file_pos_buf[key_idx].first)) {
                    if (range_file[range_idx].count(input->fileNumber()) ||
                        partial_set.count(file_pos_buf[key_idx].first)) {
                        rewrite_idx.push_back(key_idx);
                    }
                }
                if (user_key == resort_range[range_idx].second) {
                    range_idx++;
                    in_resort_range = false;
                }
            } else {
                if (ikey.type != kTypeDeletion && partial_set.count(file_pos_buf[key_idx].first)) {
                    rewrite_idx.push_back(key_idx);
                }
            }
            key_idx++;

            if (key_idx == KVBufferSize || rewrite_idx.size() == KVReWriteSize) {
                if (rewrite_idx.size()) {
                    status = ReWriteValue(v_buf, seq_buf, rewrite_idx, size_buf, file_pos_buf, vlog_drop_count,
                                          vlog_drop_offset, vlog_rewrite, check_numb, check_offset);
                    if (!status.ok()) break;
                }

                for (size_t i = flush_idx; i < key_idx; i++) {
                    if (has_imm_.NoBarrier_Load() != NULL) {
                        const uint64_t imm_start = env_->NowMicros();
                        mutex_.Lock();
                        if (imm_ != NULL) {
                            CompactMemTable();
                            bg_cv_.SignalAll();
                        }
                        mutex_.Unlock();
                        imm_micros += (env_->NowMicros() - imm_start);
                    }

                    if (compact->builder == NULL) {
                        status = OpenCompactionOutputFile(compact);
                        if (!status.ok()) break;
                    }
                    if (compact->builder->NumEntries() == 0) {
                        compact->current_output()->smallest.DecodeFrom(k_buf[i]);
                    }
                    compact->current_output()->largest.DecodeFrom(k_buf[i]);
                    compact->builder->Add(Slice(k_buf[i]), Slice(v_buf[i]));

                    if (compact->compaction->ShouldStopBefore(Slice(k_buf[i])) ||
                        compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
                        status = FinishCompactionOutputFile(compact, input);
                        if (!status.ok()) break;
                    }
                }
                if (!status.ok()) break;

                if (key_idx == KVBufferSize) {
                    key_idx = 0;
                }

                resort_count += rewrite_idx.size();
                rewrite_idx.clear();
                flush_idx = key_idx;
            }
        }

        input->Next();
    }
    // std::cout << "3331\n";
    if (status.ok() && shutting_down_.Acquire_Load()) {
        status = Status::IOError("Deleting DB during compaction");
    }
    if (status.ok() && rewrite_idx.size()) {
        resort_count += rewrite_idx.size();
        status = ReWriteValue(v_buf, seq_buf, rewrite_idx, size_buf, file_pos_buf, vlog_drop_count, vlog_drop_offset,
                              vlog_rewrite, check_numb, check_offset);
    }
    if (status.ok() && flush_idx < key_idx) {
        for (size_t i = flush_idx; i < key_idx; i++) {
            if (compact->builder == NULL) {
                status = OpenCompactionOutputFile(compact);
                if (!status.ok()) break;
            }
            if (compact->builder->NumEntries() == 0) {
                compact->current_output()->smallest.DecodeFrom(k_buf[i]);
            }
            compact->current_output()->largest.DecodeFrom(k_buf[i]);
            compact->builder->Add(Slice(k_buf[i]), Slice(v_buf[i]));
            if (compact->compaction->ShouldStopBefore(Slice(k_buf[i])) ||
                compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) break;
            }
        }
    }
    // std::cout << "3332\n";
    if (status.ok() && compact->builder != NULL) status = FinishCompactionOutputFile(compact, input);

    // std::cout << "3333\n";
    if (status.ok()) {
        status = input->status();
    }
    delete input;
    input = NULL;

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
            stats.bytes_read += compact->compaction->input(which, i)->file_size;
        }
    }
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        stats.bytes_written += compact->outputs[i].file_size;
    }
    if (!vlog_drop_count.empty()) {
        for (auto iter = vlog_drop_count.begin(); iter != vlog_drop_count.end(); iter++) {
            drop_count += iter->second;
        }
    }
    stats.bytes_written += drop_count;
    // std::cout << "444\n";

    bool delete_vlog = false;
    if (has_full_gc) {
        pthread_mutex_lock(&full_gc_->mutex_);
        if (full_gc_->finished_ == true) {
            delete_vlog = true;
        } else {
            full_gc_->mem_->Unref();
            full_gc_->mem_ = new MemTable(internal_comparator_);
            full_gc_->mem_->Ref();
            pthread_cond_signal(&full_gc_->cond_recover_);
        }
        pthread_mutex_unlock(&full_gc_->mutex_);
    }
    mutex_.Lock();
    if (delete_vlog) {
        delete full_gc_;
        full_gc_ = NULL;
        bg_clean_scheduled_ = false;
    }
    for (auto iter = all_set.begin(); iter != all_set.end(); iter++) {
        vlog_refs_[*iter]--;
    }
    stats_[compact->compaction->level() + 1].Add(stats);
    if (status.ok()) {
        drop_count_ += drop_count;
        vlog_manager_.AddDropCount(vlog_drop_count);
        status = InstallCompactionResults(compact, check_numb, check_offset);
        MaybeScheduleClean();
    }
    if (!status.ok()) {
        vlog_manager_.AddDropCount(vlog_rewrite);
        RecordBackgroundError(status);
    }
    // std::cout << "DoCompactionWork finish";
    // std::cout << " level=" << compact->compaction->level();
    // std::cout << " range=" << range_count << " " << resort_range.size() << " " << range_file.size();
    // std::cout << " resort=" << resort_count;
    // std::cout << " in=";
    // for (size_t i = 0; i < compact->compaction->num_input_files(0); i++) {
    //     const CompactionState::Output& out = compact->outputs[i];
    //     std::cout << compact->compaction->input(0, i)->number << " ";
    // }
    // for (size_t i = 0; i < compact->compaction->num_input_files(1); i++) {
    //     const CompactionState::Output& out = compact->outputs[i];
    //     std::cout << compact->compaction->input(1, i)->number << " ";
    // }
    // std::cout << "out=";
    // for (size_t i = 0; i < compact->outputs.size(); i++) {
    //     const CompactionState::Output& out = compact->outputs[i];
    //     std::cout << out.number << " ";
    // }
    // std::cout << "\n";

    return status;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
    assert(compact != NULL);
    assert(compact->builder == NULL);
    uint64_t file_number;
    {
        mutex_.Lock();
        file_number = versions_->NewFileNumber();
        pending_outputs_.insert(file_number);
        CompactionState::Output out;
        out.number = file_number;
        out.smallest.Clear();
        out.largest.Clear();
        compact->outputs.push_back(out);
        mutex_.Unlock();
    }

    // Make the output file
    std::string fname = TableFileName(dbname_, file_number);
    Status status = env_->NewWritableFile(fname, &compact->outfile);
    if (status.ok()) compact->builder = new TableBuilder(options_, compact->outfile);

    return status;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact, Iterator* input) {
    assert(compact != NULL);
    assert(compact->outfile != NULL);
    assert(compact->builder != NULL);

    const uint64_t output_number = compact->current_output()->number;
    assert(output_number != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t current_entries = compact->builder->NumEntries();
    if (s.ok()) {
        s = compact->builder->Finish();
    } else {
        compact->builder->Abandon();
    }
    const uint64_t current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = NULL;

    // Finish and check for file errors
    if (s.ok()) {
        s = compact->outfile->Sync();
    }
    if (s.ok()) {
        s = compact->outfile->Close();
    }
    delete compact->outfile;
    compact->outfile = NULL;

    if (s.ok() && current_entries > 0) {
        // Verify that the table is usable
        Iterator* iter = table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
        s = iter->status();
        delete iter;
        if (s.ok()) {
            Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes", (unsigned long long)output_number,
                compact->compaction->level(), (unsigned long long)current_entries, (unsigned long long)current_bytes);
        }
    }
    return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact, const uint64_t check_numb,
                                        const uint64_t check_offset) {
    mutex_.AssertHeld();
    // Add compaction outputs
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    // std::cout << "InstallCompactionResults ";
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output& out = compact->outputs[i];
        // std::cout << out.number << " ";
        compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size, out.smallest, out.largest);
    }
    // std::cout << "\n";
    if (check_numb > 0) {
        // std::cout<<"SetSortHeadInfo 2 vlog="<<check_numb<<" head="<<check_offset<<"\n";
        compact->compaction->edit()->SetSortHeadInfo(check_numb, check_offset);
    }
    return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

void DBImpl::MaybeScheduleClean(bool isManuaClean) {
    // std::cout << "start MaybeScheduleClean\n";
    mutex_.AssertHeld();
    if (bg_clean_scheduled_) {
    } else if (!bg_error_.ok()) {
    } else if (vlog_manager_.HasVlogToClean() || isManuaClean) {
        // bg_clean_scheduled_ = true;
        // full_gc_ = new GarbageCollector(this);
        if (isManuaClean) {
            env_->StartThread(&DBImpl::BGCleanAll, this);
        } else {
            env_->StartThread(&DBImpl::BGClean, this);
        }
    }
    // std::cout << "finish MaybeScheduleClean\n";
}

void DBImpl::BGCleanAll(void* db) { reinterpret_cast<DBImpl*>(db)->BackgroundCleanAll(); }

void DBImpl::BGClean(void* db) { reinterpret_cast<DBImpl*>(db)->BackgroundClean(); }

void DBImpl::BackgroundCleanAll() {
    VersionEdit edit;
    bool save_edit = false;
    bool delete_vlog = false;
    while (vlog_manager_.HasVlogToClean()) {
        uint64_t clean_vlog_number = vlog_manager_.GetVlogToClean();
        // std::cout << "start FullGarbageCollect "<<clean_vlog_number<<"\n";
        full_gc_->SetVlog(clean_vlog_number);
        full_gc_->FullGarbageCollect(&edit, &save_edit, &delete_vlog);
        mutex_.Lock();
        if (save_edit) {
            versions_->LogAndApply(&edit, &mutex_);
        } else if (delete_vlog) {
            while (true) {
                if (bg_compaction_scheduled_) {
                    bg_cv_.SignalAll();
                    bg_cv_.Wait();
                } else {
                    vlog_partial_offset_.erase(clean_vlog_number);
                    vlog_manager_.RemoveVlog(clean_vlog_number);
                    vlog_manager_.RemoveFullVlog(clean_vlog_number);
                    break;
                }
            }
        }
        delete full_gc_;
        full_gc_ = new GarbageCollector(this);
        // std::cout << "finish FullGarbageCollect"<<" "<<save_edit<<" "<<delete_vlog<<"\n";
        mutex_.Unlock();
        if (shutting_down_.Acquire_Load() || !bg_error_.ok()) break;
    }
    std::set<uint32_t> vlogs = vlog_manager_.GetVlogsToClean(options_.min_clean_threshold);
    for (auto iter = vlogs.begin(); iter != vlogs.end(); iter++) {
        if (shutting_down_.Acquire_Load() || !bg_error_.ok()) break;
        // std::cout << "start FullGarbageCollect "<<*iter<<"\n";
        full_gc_->SetVlog(*iter);
        full_gc_->FullGarbageCollect(&edit, &save_edit, &delete_vlog);
        mutex_.Lock();
        if (save_edit) {
            versions_->LogAndApply(&edit, &mutex_);
        } else if (delete_vlog) {
            vlog_delete_.insert(full_gc_->vlog_number_);
            vlog_manager_.RemoveFullVlog(full_gc_->vlog_number_);
            vlog_manager_.manager_[full_gc_->vlog_number_].count_ = 0;
            vlog_partial_offset_.erase(full_gc_->vlog_number_);
        }
        if (full_gc_->garbage_pos_ > full_gc_->initial_pos_) {
            vlog_recycle_[full_gc_->vlog_number_] = std::make_pair(full_gc_->initial_pos_, full_gc_->garbage_pos_);
        }

        delete full_gc_;
        full_gc_ = new GarbageCollector(this);
        // std::cout << "finish FullGarbageCollect"<<" "<<save_edit<<" "<<delete_vlog<<"\n";
        mutex_.Unlock();
    }
    mutex_.Lock();
    bg_clean_scheduled_ = false;
    delete full_gc_;
    full_gc_ = NULL;
    has_cleaned_ = true;
    bg_cv_.SignalAll(); // 要唤醒cleanvlog
    mutex_.Unlock();
} // namespace leveldb

void DBImpl::BackgroundClean() {
    VersionEdit edit;
    bool save_edit = false;
    bool delete_vlog = false;
    uint64_t clean_vlog_number = vlog_manager_.GetVlogToClean();
    // std::cout << "BackgroundClean vlog=" << clean_vlog_number << "\n";

    mutex_.Lock();
    bg_clean_scheduled_ = true;
    full_gc_ = new GarbageCollector(this);
    mutex_.Unlock();

    full_gc_->SetVlog(clean_vlog_number);
    full_gc_->FullGarbageCollect(&edit, &save_edit, &delete_vlog);
    mutex_.Lock();
    if (save_edit) {
        versions_->LogAndApply(&edit, &mutex_);
    } else if (delete_vlog) {
        last_delete_vlog = full_gc_->vlog_number_;
        vlog_delete_.insert(full_gc_->vlog_number_);
        vlog_manager_.RemoveFullVlog(full_gc_->vlog_number_);
        vlog_manager_.manager_[full_gc_->vlog_number_].count_ = 0;
    }
    if (full_gc_->garbage_pos_ > full_gc_->initial_pos_) {
        vlog_recycle_[full_gc_->vlog_number_] = std::make_pair(full_gc_->initial_pos_, full_gc_->garbage_pos_);
    }

    // bg_clean_scheduled_ = false;
    // delete full_gc_;
    // full_gc_ = NULL;
    bg_cv_.SignalAll();
    mutex_.Unlock();
    // std::cout << "finish FullGarbageCollect " << clean_vlog_number <<
    // "------------------------------------------\n";
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
    mutex_.AssertHeld();
    if (compact->builder != NULL) {
        // May happen if we get a shutdown call in the middle of compaction
        compact->builder->Abandon();
        delete compact->builder;
    } else {
        assert(compact->outfile == NULL);
    }
    delete compact->outfile;
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output& out = compact->outputs[i];
        pending_outputs_.erase(out.number);
    }
    delete compact;
}

void DBImpl::MaybeIgnoreError(Status* status) const {
    if (status->ok() || options_.paranoid_checks) {
        // No change needed
    } else {
        Log(options_.info_log, "Ignoring error %s", status->ToString().c_str());
        *status = Status::OK();
    }
}

DBImpl::~DBImpl() {
    // Wait for background work to finish
    mutex_.Lock();
    shutting_down_.Release_Store(this); // Any non-NULL value is ok
    while (bg_compaction_scheduled_) {  // 还得等clean线程退出
        // std::cout << "waiting for compaction or clean finish compaction=" << bg_compaction_scheduled_
        //           << " clean=" << bg_clean_scheduled_ << "  \n";
        bg_cv_.SignalAll();
        bg_cv_.Wait();
    }
    while (bg_clean_scheduled_ && full_gc_->finished_ == false) {
        bg_cv_.SignalAll();
        bg_cv_.Wait();
    }

    if (vlog_recycle_.empty() == false) {
        DeleteObsoleteFiles();
    }

    if (drop_count_ > 0) {
        std::string vloginfo;
        vlog_manager_.Serialize(vloginfo);
        VersionEdit edit;
        edit.SetVlogInfo(vloginfo);
        versions_->LogAndApply(&edit, &mutex_);
    }

    mutex_.Unlock();

    if (db_lock_ != NULL) {
        env_->UnlockFile(db_lock_);
    }

    delete versions_;
    if (mem_ != NULL) mem_->Unref();
    if (imm_ != NULL) imm_->Unref();
    delete tmp_batch_;
    delete vlog_;
    delete vlogfile_;
    delete table_cache_;

    if (owns_info_log_) {
        delete options_.info_log;
    }
    if (owns_cache_) {
        delete options_.block_cache;
    }
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env)
    , internal_comparator_(raw_options.comparator)
    , internal_filter_policy_(raw_options.filter_policy)
    , options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options))
    , owns_info_log_(options_.info_log != raw_options.info_log)
    , owns_cache_(options_.block_cache != raw_options.block_cache)
    , dbname_(dbname)
    , db_lock_(NULL)
    , shutting_down_(NULL)
    , bg_cv_(&mutex_)
    , mem_(NULL)
    , imm_(NULL)
    , full_gc_(NULL)
    , logfile_number_(0)
    , vlog_head_(0)
    , vlog_(NULL)
    , vlogfile_(NULL)
    , sort_logfile_number_(sort_vlog_number_initial)
    , sort_vlog_head_(0)
    , sort_vlog_(NULL)
    , sort_vlogfile_(NULL)
    , check_log_(0)
    , check_point_(0)
    , drop_count_(0)
    , recover_clean_vlog_number_(0)
    , recover_clean_pos_(0)
    , vlog_manager_(options_.clean_threshold)
    , seed_(0)
    , tmp_batch_(new WriteBatch)
    , bg_compaction_scheduled_(false)
    , bg_clean_scheduled_(false)
    , manual_compaction_(NULL)
    , needVisitAllLevel(false)
    , rewrite_count_(0)
    , reorder_rewrite_(0) {
    has_imm_.Release_Store(NULL);
    // Reserve ten files or so for other uses and give the rest to TableCache.
    const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
    table_cache_ = new TableCache(dbname_, &options_, table_cache_size);
    versions_ = new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);
    gc_rewrite_off.resize(10);
    gc_delete_off.resize(10);
    gc_skip_off.resize(10);
}

Status DBImpl::NewDB() {
    VersionEdit new_db;
    new_db.SetComparatorName(user_comparator()->Name());
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    WritableFile* file;
    Status status = env_->NewWritableFile(manifest, &file);
    if (!status.ok()) {
        return status;
    }
    {
        log::Writer log(file);
        std::string record;
        new_db.EncodeTo(&record);
        status = log.AddRecord(record);
        if (status.ok()) {
            status = file->Close();
        }
    }
    delete file;
    if (status.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        status = SetCurrentFile(env_, dbname_, 1);
    } else {
        env_->DeleteFile(manifest);
    }
    return status;
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
    *dbptr = NULL;

    DBImpl* impl = new DBImpl(options, dbname);
    impl->mutex_.Lock();
    VersionEdit edit;
    // Recover handles create_if_missing, error_if_exists
    bool save_manifest = false;
    // std::cout<<"sort vlog="<<impl->sort_logfile_number_<<" head="<<impl->sort_vlog_head_<<"\n";
    Status status = impl->Recover(&edit, &save_manifest);
    // std::cout<<"sort vlog="<<impl->sort_logfile_number_<<" head="<<impl->sort_vlog_head_<<"\n";
    if (status.ok() && impl->mem_ == NULL) {
        // Create new log and a corresponding memtable.
        uint64_t new_log_number = impl->versions_->NewVlogNumber();
        WritableFile* lfile;
        status = options.env->NewWritableFile(VLogFileName(dbname, new_log_number), &lfile);
        if (status.ok()) {
            edit.SetLogNumber(new_log_number);
            impl->vlogfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->vlog_ = new log::VWriter(lfile);
            impl->mem_ = new MemTable(impl->internal_comparator_);
            impl->mem_->Ref();
            SequentialFile* vlr_file;
            status = impl->options_.env->NewSequentialFile(VLogFileName(impl->dbname_, new_log_number), &vlr_file);
            log::VReader* vlog_reader = new log::VReader(vlr_file, true, 0);
            impl->vlog_manager_.AddVlog(new_log_number, vlog_reader);
            impl->vlog_manager_.SetNowVlog(new_log_number);
        }
    }

    if (status.ok()) {
        WritableFile* sort_lfile;
        if (impl->sort_logfile_number_ == sort_vlog_number_initial) {
            impl->versions_->MarkSortVlogNumberUsed(impl->sort_logfile_number_);
            status =
                options.env->NewWritableFile(VLogFileName(dbname, impl->versions_->NewSortVlogNumber()), &sort_lfile);
        } else {
            impl->versions_->ReuseSortVlogNumber(impl->sort_logfile_number_);
            status =
                options.env->NewAppendableFile(VLogFileName(dbname, impl->versions_->NewSortVlogNumber()), &sort_lfile);
        }

        if (status.ok()) {
            impl->sort_vlogfile_ = sort_lfile;
            impl->sort_logfile_number_ = impl->versions_->SortLogNumber();
            impl->sort_vlog_ = new log::VWriter(sort_lfile);
            if (impl->vlog_manager_.GetVlog(impl->sort_logfile_number_) == NULL) {
                SequentialFile* sort_vlr_file;
                status = impl->options_.env->NewSequentialFile(VLogFileName(impl->dbname_, impl->sort_logfile_number_),
                                                               &sort_vlr_file);
                log::VReader* sort_vlog_reader = new log::VReader(sort_vlr_file, true, 0);
                impl->vlog_manager_.AddVlog(impl->sort_logfile_number_, sort_vlog_reader);
            }
        }
    }

    if (status.ok() && save_manifest) {
        edit.SetLogNumber(impl->logfile_number_);
        status = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (status.ok() && impl->recover_clean_vlog_number_ > 0 &&
        impl->vlog_manager_.NeedClean(impl->recover_clean_vlog_number_)) {
        impl->bg_clean_scheduled_ = true;
        impl->env_->StartThread(&DBImpl::BGCleanRecover, impl);
        impl->DeleteObsoleteFiles();
        impl->MaybeScheduleCompaction();
    }
    impl->MaybeScheduleClean(false);
    impl->mutex_.Unlock();
    if (status.ok()) {
        assert(impl->mem_ != NULL);
        *dbptr = impl;
    } else {
        delete impl;
    }
    impl->versions_->InitialFileMap();
    // std::cout<<"finish open "<<impl->logfile_number_<<" "<<impl->vlog_head_<<" "<<impl->sort_logfile_number_<<"
    // "<<impl->sort_vlog_head_<<"\n";
    return status;
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
    mutex_.AssertHeld();

    // Ignore error from CreateDir since the creation of the DB is
    // committedeck_pointonly when the descriptor is created, and this directory
    // may already exist from a previous failed creation attempt.
    env_->CreateDir(dbname_);
    assert(db_lock_ == NULL);
    Status status = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!status.ok()) return status;
    if (!env_->FileExists(CurrentFileName(dbname_))) {
        if (options_.create_if_missing) {
            status = NewDB();
            if (!status.ok()) { // 创建成功并不返回额，失败才返回
                return status;
            }
        } else {
            return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
        }
    } else {
        if (options_.error_if_exists) {
            return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
        }
    }
    // std::cout<<"1 sort vlog="<<sort_logfile_number_<<" head="<<sort_vlog_head_<<"\n";
    uint64_t log_number = 0, head_pos = 0;
    std::string vloginfo;
    status = versions_->Recover(save_manifest, log_number, head_pos, sort_logfile_number_, sort_vlog_head_, vloginfo,
                                recover_clean_vlog_number_,
                                recover_clean_pos_); // ruse manifest文件时save_manifest会继续保持false
    // std::cout<<"2 sort vlog="<<sort_logfile_number_<<" head="<<sort_vlog_head_<<"\n";
    if (!status.ok()) return status;

    if (*save_manifest == true) {
        if (!vloginfo.empty()) edit->SetVlogInfo(vloginfo);
        if (recover_clean_vlog_number_ > 0) edit->SetTailInfo(recover_clean_vlog_number_, recover_clean_pos_);
        if (log_number > 0) edit->SetHeadInfo(log_number, head_pos);
        if (sort_logfile_number_ > sort_vlog_number_initial) {
            edit->SetSortHeadInfo(sort_logfile_number_, sort_vlog_head_);
            // std::cout<<"SetSortHeadInfo 3 vlog="<<sort_logfile_number_<<" head="<<sort_vlog_head_<<"\n";
        }
    }
    SequenceNumber max_sequence(0);
    const uint64_t min_log = versions_->LogNumber();
    std::vector<std::string> filenames;
    status = env_->GetChildren(dbname_, &filenames);
    if (!status.ok()) return status;
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    // std::cout << "recover vlog to reader ";
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            expected.erase(number);
            if (type == kVLogFile) {
                if (number >= min_log && number <= sort_vlog_number_initial) logs.push_back(number);
                std::string vlog_name = VLogFileName(dbname_, number);
                SequentialFile* vlr_file;
                status = options_.env->NewSequentialFile(vlog_name, &vlr_file);
                if (!status.ok()) return status;
                log::VReader* vlog_reader = new log::VReader(vlr_file, true, 0);
                vlog_manager_.AddVlog(number, vlog_reader);
                // std::cout << number << " ";
            }
        }
    }
    // std::cout << "\n";
    if (!expected.empty()) {
        char buf[50];
        snprintf(buf, sizeof(buf), "%d missing files; e.g.", static_cast<int>(expected.size()));
        return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }
    if (!vloginfo.empty()) {
        vlog_manager_.Deserialize(vloginfo);
    }

    std::sort(logs.begin(), logs.end());
    if (log_number > 0 && !logs.empty()) {
        if (log_number == logs[0]) {
            vlog_head_ = head_pos;
        } else {
            vlog_head_ = 0;
            assert(log_number < logs[0]);
            assert(logs.size() == 1);
        }
    } else {
        vlog_head_ = 0;
    }

    for (size_t i = 0; i < logs.size(); i++) {
        status = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit, &max_sequence);
        if (!status.ok()) return status;
        versions_->MarkVlogNumberUsed(logs[i]);
    }

    if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
    }
    return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
    struct LogReporter : public log::VReader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status; // NULL if options_.paranoid_checks==false
        virtual void Corruption(size_t bytes, const Status& status) {
            Log(info_log, "%s%s: dropping %d bytes; %s", (this->status == NULL ? "(ignoring error) " : ""), fname,
                static_cast<int>(bytes), status.ToString().c_str());
            if (this->status != NULL && this->status->ok()) *this->status = status;
        }
    };

    mutex_.AssertHeld();
    // Open the log file
    std::string fname = VLogFileName(dbname_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : NULL);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::VReader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/); // reader析构时会delete掉file
    if (vlog_head_ > 0) {
        if (!reader.SkipToPos(vlog_head_)) return Status::Corruption("reader skip false");
    }
    Log(options_.info_log, "Recovering log #%llu", (unsigned long long)log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable* mem = NULL;
    int head_size = 0;
    while (reader.ReadRecord(&record, &scratch, head_size) && status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(record.size(), Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == NULL) {
            mem = new MemTable(internal_comparator_);
            mem->Ref();
        }
        vlog_head_ += head_size;
        status = WriteBatchInternal::InsertInto(&batch, mem, vlog_head_, log_number);
        MaybeIgnoreError(&status);
        if (!status.ok()) {
            break;
        }
        const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) + WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
            *max_sequence = last_seq;
        }

        if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
            compactions++;
            *save_manifest = true;
            std::cout << "recover 1 log=" << log_number << " file=" << logfile_number_ << "\n";
            status = WriteLevel0Table(mem, edit, NULL);
            mem->Unref();
            mem = NULL;
            if (!status.ok()) {
                // Reflect errors immediately so that conditions like full
                // file-systems cause the DB::Open() to fail.
                break;
            }
        }
    }

    if (!last_log) // vlog文件很大，一般恢复时最多同时跨两个vlog文件，第一个恢复完后，
    {              // 第二个vlog需要从文件头开始恢复
        vlog_head_ = 0;
        if (mem != NULL) {
            // 因为上面的while循环可能剩余的kv对不够刷mem，因为马上要回放下一个vlog了，需要把上一个vlog
            // 剩余的kv刷入sst
            if (status.ok()) {
                *save_manifest = true;
                std::cout << "recover 2 log=" << log_number << " file=" << logfile_number_ << "\n";
                status = WriteLevel0Table(mem, edit, NULL);
            }
            mem->Unref();
        }
    } else { // 回放的是最后一个vlog文件
        if (!(compactions == 0 && mem == NULL)) {
            // 针对的是该vlog文件中一条待恢复的kv记录都没有,只有有待恢复的记录时才会进入该分支，需要
            // 重新设置重启点head
            // 针对 刚好恢复完vlog恰好mem为空 的情况
            if (mem == NULL) {
                mem = new MemTable(internal_comparator_);
                mem->Ref();
            }
            if (status.ok()) {
                *save_manifest = true;
                *max_sequence = *max_sequence + 1;
                std::cout << "recover 3 log=" << log_number << " file=" << logfile_number_ << "\n";
                status = WriteLevel0Table(mem, edit, NULL);
                edit->SetHeadInfo(log_number, vlog_head_);
            }
            mem->Unref();
        }
        if (status.ok()) {
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
        }

        WritableFile* vlfile;
        logfile_number_ = log_number;
        std::string vlog_name = VLogFileName(dbname_, log_number);
        status = options_.env->NewAppendableFile(vlog_name, &vlfile); // 没问题，因为我们是appendablefile
        vlogfile_ = vlfile;
        vlog_ = new log::VWriter(vlfile);
        vlog_manager_.SetNowVlog(log_number);
    }

    return status;
}

void DBImpl::BGCleanRecover(void* db) { reinterpret_cast<DBImpl*>(db)->BackgroundRecoverClean(); }

void DBImpl::BackgroundRecoverClean() {
    std::cout << "start BackgroundRecoverClean " << recover_clean_vlog_number_ << " " << recover_clean_pos_ << "\n";
    VersionEdit edit;
    bool save_edit = false;
    bool delete_vlog = false;
    full_gc_ = new GarbageCollector(this);
    full_gc_->SetVlog(recover_clean_vlog_number_, recover_clean_pos_);
    full_gc_->FullGarbageCollect(&edit, &save_edit, &delete_vlog);

    mutex_.Lock();
    if (save_edit) {
        versions_->LogAndApply(&edit, &mutex_);
    } else if (delete_vlog) {
        while (true) {
            if (bg_compaction_scheduled_) {
                bg_cv_.SignalAll();
                bg_cv_.Wait();
            } else {
                vlog_partial_offset_.erase(recover_clean_vlog_number_);
                vlog_manager_.RemoveVlog(recover_clean_vlog_number_);
                vlog_manager_.RemoveFullVlog(recover_clean_vlog_number_);
                break;
            }
        }
    }
    bg_clean_scheduled_ = false;
    delete full_gc_;
    full_gc_ = NULL;
    bg_cv_.SignalAll();
    mutex_.Unlock();
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
    int max_level_with_files = 1;
    {
        MutexLock l(&mutex_);
        Version* base = versions_->current();
        for (int level = 1; level < config::kNumLevels; level++) {
            if (base->OverlapInLevel(level, begin, end)) {
                max_level_with_files = level;
            }
        }
    }
    TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
    for (int level = 0; level < max_level_with_files; level++) {
        TEST_CompactRange(level, begin, end);
    }
}

Status DBImpl::TEST_CompactMemTable() {
    // NULL batch means just wait for earlier writes to be done
    Status status = Write(WriteOptions(), NULL);
    if (status.ok()) {
        // Wait until the compaction completes
        MutexLock l(&mutex_);
        while (imm_ != NULL && bg_error_.ok()) {
            bg_cv_.Wait();
        }
        if (imm_ != NULL) {
            status = bg_error_;
        }
        // for (int i = 0; i < config::kNumLevels; i++) {
        //     std::cout << i << "\t" << versions_->NumLevelFiles(i) << "\t";
        // }
        // std::cout << "\n";
    }
    return status;
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin, const Slice* end) {
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);

    InternalKey begin_storage, end_storage;

    ManualCompaction manual;
    manual.level = level;
    manual.done = false;
    if (begin == NULL) {
        manual.begin = NULL;
    } else {
        begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
        manual.begin = &begin_storage;
    }
    if (end == NULL) {
        manual.end = NULL;
    } else {
        end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
        manual.end = &end_storage;
    }

    MutexLock l(&mutex_);
    while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
        if (manual_compaction_ == NULL) { // Idle
            manual_compaction_ = &manual;
            MaybeScheduleCompaction();
        } else { // Running either my compaction or another compaction.
            bg_cv_.Wait();
        }
    }
    if (manual_compaction_ == &manual) {
        // Cancel my manual compaction since we aborted early for some reason.
        manual_compaction_ = NULL;
    }
    // PrintFiles();
}

void DBImpl::PrintFiles() {
    mutex_.Lock();
    auto vlog_set = vlog_manager_.GetAllVlog();
    std::cout << "vlog_set=";
    for (auto iit = vlog_set.begin(); iit != vlog_set.end(); iit++) {
        std::cout << *iit << "-" << vlog_refs_[*iit] << " ";
    }
    std::cout << "\n";
    std::cout << "vlog_delete=";
    for (auto iit = vlog_delete_.begin(); iit != vlog_delete_.end(); iit++) {
        std::cout << *iit << " ";
    }
    std::cout << "\n";
    versions_->PrintFileSize();
    mutex_.Unlock();
}

void DBImpl::RecordReadSample(Slice key) {
    MutexLock l(&mutex_);
    if (versions_->current()->RecordReadSample(key)) {
        MaybeScheduleCompaction();
    }
}

namespace {
struct IterState {
    port::Mutex* mu;
    Version* version;
    MemTable* mem;
    MemTable* imm;
    std::tr1::unordered_map<uint64_t, int>* sstable_refs;
    std::tr1::unordered_map<uint64_t, int>* vlog_refs;
    std::tr1::unordered_set<uint64_t> vlog_record;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
    IterState* state = reinterpret_cast<IterState*>(arg1);
    state->mu->Lock();
    state->mem->Unref();
    if (state->imm != NULL) state->imm->Unref();
    // for (int i = 0; i < config::kNumLevels; i++) {
    //     for (size_t j = 0; j < state->version->files_[i].size(); j++) {
    //         (*state->sstable_refs)[state->version->files_[i][j]->number]--;
    //     }
    // }
    // std::cout << "start vlog unref\n";
    for (auto iter = state->vlog_record.begin(); iter != state->vlog_record.end(); iter++) {
        (*state->vlog_refs)[*iter]--;
    }
    state->version->Unref();
    // std::cout << "NewInternalIterator delete cleanup\n";
    state->mu->Unlock();
    delete state;
}
} // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options, SequenceNumber* latest_snapshot, uint32_t* seed) {
    IterState* cleanup = new IterState;
    mutex_.Lock();
    *latest_snapshot = versions_->LastSequence();

    // Collect together all needed child iterators
    std::vector<Iterator*> list;
    list.push_back(mem_->NewIterator());
    list[list.size() - 1]->file_numb_ = 0;
    mem_->Ref();
    if (imm_ != NULL) {
        list.push_back(imm_->NewIterator());
        list[list.size() - 1]->file_numb_ = 1;
        imm_->Ref();
    }
    Version* current = versions_->current();
    current->AddIterators(options, &list);
    Iterator* internal_iter = NewMergingIterator(&internal_comparator_, &list[0], list.size());
    current->Ref();

    // for (int i = 0; i < config::kNumLevels; i++) {
    //     for (size_t j = 0; j < current->files_[i].size(); j++) {
    //         sstable_refs_[current->files_[i][j]->number]++;
    //     }
    // }

    std::set<uint32_t> all_set = vlog_manager_.GetAllVlog();
    for (auto iter = all_set.begin(); iter != all_set.end(); iter++) {
        if (vlog_delete_.count(*iter) == 0) {
            vlog_refs_[*iter]++;
            cleanup->vlog_record.insert(*iter);
        }
    }

    cleanup->mu = &mutex_;
    cleanup->mem = mem_;
    cleanup->imm = imm_;
    cleanup->version = current;
    cleanup->sstable_refs = &sstable_refs_;
    cleanup->vlog_refs = &vlog_refs_;
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

    *seed = ++seed_;
    mutex_.Unlock();
    return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
    SequenceNumber ignored;
    uint32_t ignored_seed;
    return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
    MutexLock l(&mutex_);
    return versions_->MaxNextLevelOverlappingBytes();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    uint32_t seed;
    SequenceNumber latest_snapshot;
    Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
    return NewDBIterator(this, user_comparator(), iter, latest_snapshot, seed);
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
    value->clear();

    MutexLock l(&mutex_);
    Slice in = property;
    Slice prefix("leveldb.");
    if (!in.starts_with(prefix)) return false;
    in.remove_prefix(prefix.size());

    if (in.starts_with("num-files-at-level")) {
        in.remove_prefix(strlen("num-files-at-level"));
        uint64_t level;
        bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
        if (!ok || level >= config::kNumLevels) {
            return false;
        } else {
            char buf[100];
            snprintf(buf, sizeof(buf), "%d", versions_->NumLevelFiles(static_cast<int>(level)));
            *value = buf;
            return true;
        }
    } else if (in == "stats") {
        char buf[200];
        snprintf(buf, sizeof(buf),
                 "                               Compactions\n"
                 "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                 "--------------------------------------------------\n");
        value->append(buf);
        for (int level = 0; level < config::kNumLevels; level++) {
            int files = versions_->NumLevelFiles(level);
            if (stats_[level].micros > 0 || files > 0) {
                snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level, files,
                         versions_->NumLevelBytes(level) / 1048576.0, stats_[level].micros / 1e6,
                         stats_[level].bytes_read / 1048576.0, stats_[level].bytes_written / 1048576.0);
                value->append(buf);
            }
        }
        return true;
    } else if (in == "sstables") {
        *value = versions_->current()->DebugString();
        return true;
    } else if (in == "approximate-memory-usage") {
        size_t total_usage = options_.block_cache->TotalCharge();
        if (mem_) {
            total_usage += mem_->ApproximateMemoryUsage();
        }
        if (imm_) {
            total_usage += imm_->ApproximateMemoryUsage();
        }
        char buf[50];
        snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(total_usage));
        value->append(buf);
        return true;
    }

    return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
    // TODO(opt): better implementation
    Version* v;
    {
        MutexLock l(&mutex_);
        versions_->current()->Ref();
        v = versions_->current();
    }

    for (int i = 0; i < n; i++) {
        // Convert user_key into a corresponding internal key.
        InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
        InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
        uint64_t start = versions_->ApproximateOffsetOf(v, k1);
        uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
        sizes[i] = (limit >= start ? limit - start : 0);
    }

    {
        MutexLock l(&mutex_);
        v->Unref();
    }
}

uint64_t DBImpl::GetVlogNumber() { return logfile_number_; }

uint64_t DBImpl::GetVlogHead() { return vlog_head_; }

int DBImpl::TotalVlogFiles() {
    int res = 0;
    std::vector<std::string> filenames;
    uint64_t number;
    FileType type;
    env_->GetChildren(dbname_, &filenames);
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            if (type == kVLogFile) res++;
        }
    }
    return res;
}

void DBImpl::CleanVlog() { // 不可重入
    mutex_.Lock();
    has_cleaned_ = false; // 会因为它一直阻塞，直到cleanvlog完成
    while (!has_cleaned_ && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
        if (bg_clean_scheduled_) {
            bg_cv_.Wait();
        } else {
            MaybeScheduleClean(true);
        }
    }
    mutex_.Unlock();
}

DB::~DB() {}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string& dbname, const Options& options) {
    Env* env = options.env;
    std::vector<std::string> filenames;
    // Ignore error in case directory does not exist
    env->GetChildren(dbname, &filenames);
    if (filenames.empty()) {
        return Status::OK();
    }

    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    Status result = env->LockFile(lockname, &lock);
    if (result.ok()) {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type) &&
                type != kDBLockFile) { // Lock file will be deleted at end
                Status del = env->DeleteFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                    result = del;
                }
            }
        }
        env->UnlockFile(lock); // Ignore error since state is already gone
        env->DeleteFile(lockname);
        env->DeleteDir(dbname); // Ignore error in case dir contains other files
    }
    return result;
}

} // namespace leveldb
