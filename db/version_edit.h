// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <iostream>
#include <set>
#include <tr1/unordered_set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
    int refs;
    int allowed_seeks; // Seeks allowed until compaction
    uint64_t number;
    uint64_t file_size;   // File size in bytes
    InternalKey smallest; // Smallest internal key served by table
    InternalKey largest;  // Largest internal key served by table

    FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}
};

class VersionEdit {
public:
    VersionEdit() { Clear(); }
    ~VersionEdit() {}

    void Clear();

    void SetComparatorName(const Slice& name) {
        has_comparator_ = true;
        comparator_ = name.ToString();
    }
    void SetLogNumber(uint64_t num) {
        has_log_number_ = true;
        log_number_ = num;
    }
    void SetPrevLogNumber(uint64_t num) {
        has_prev_log_number_ = true;
        prev_log_number_ = num;
    }
    void SetNextFile(uint64_t num) {
        has_next_file_number_ = true;
        next_file_number_ = num;
    }
    void SetFullGcFile(uint64_t num) {
        has_full_gc_file_ = true;
        full_gc_file_ = num;
    }
    void SetLastSequence(SequenceNumber seq) {
        has_last_sequence_ = true;
        last_sequence_ = seq;
    }
    void SetCompactPointer(int level, const InternalKey& key) {
        compact_pointers_.push_back(std::make_pair(level, key));
    }
    void SetHeadInfo(uint64_t logfile_number, uint64_t check_point) {
        EncodeFixed64(head_info_, (check_point << 24) | logfile_number);
        has_head_info_ = true;
    }
    void SetSortHeadInfo(uint64_t sort_logfile_number, uint64_t sort_check_point) {
        EncodeFixed64(sort_head_info_, (sort_check_point << 24) | sort_logfile_number);
        has_sort_head_info_ = true;
    }
    void SetTailInfo(uint64_t logfile_number, uint64_t tail_pos) {
        EncodeFixed64(tail_info_, (tail_pos << 24) | logfile_number);
        has_tail_info_ = true;
    }
    void SetVlogInfo(std::string& vloginfo) {
        vloginfo_ = vloginfo;
        has_vloginfo_ = true;
    }
    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    void AddFile(int level, uint64_t file, uint64_t file_size, const InternalKey& smallest,
                 const InternalKey& largest) {
        FileMetaData f;
        f.number = file;
        f.file_size = file_size;
        f.smallest = smallest;
        f.largest = largest;
        new_files_.push_back(std::make_pair(level, f));
    }

    // Delete the specified "file" from the specified "level".
    void DeleteFile(int level, uint64_t file) { deleted_files_.insert(std::make_pair(level, file)); }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(const Slice& src);

    std::string DebugString() const;

private:
    friend class VersionSet;

    typedef std::set<std::pair<int, uint64_t> > DeletedFileSet;

    bool has_comparator_;
    std::string comparator_;
    bool has_vloginfo_;
    std::string vloginfo_;
    bool has_log_number_;
    uint64_t log_number_;
    bool has_prev_log_number_;
    uint64_t prev_log_number_;
    bool has_next_file_number_;
    uint64_t next_file_number_;
    bool has_last_sequence_;
    SequenceNumber last_sequence_;
    bool has_head_info_;
    char head_info_[8];
    bool has_sort_head_info_;
    char sort_head_info_[8];
    bool has_tail_info_;
    char tail_info_[8];
    bool has_full_gc_file_;
    uint64_t full_gc_file_;

    std::vector<std::pair<int, InternalKey> > compact_pointers_;
    DeletedFileSet deleted_files_;
    std::vector<std::pair<int, FileMetaData> > new_files_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
