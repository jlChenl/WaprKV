// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"
#include <iostream>
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;
static const size_t kSortHeader = 4;

WriteBatch::WriteBatch(bool rewrited) {
    rewrited_ = rewrited;
    Clear();
}

WriteBatch::~WriteBatch() {}

WriteBatch::Handler::~Handler() {}

void WriteBatch::Clear() {
    rep_.clear();
    if (rewrited_) {
        rep_.resize(kSortHeader);
    } else {
        rep_.resize(kHeader);
    }
}

size_t WriteBatch::ApproximateSize() { return rep_.size(); }

Status WriteBatch::ParseRecord(uint64_t& pos, Slice& key, Slice& value, bool& isDel, uint64_t* seq) const {
    Slice input(rep_);
    input.remove_prefix(pos);

    const char* begin_pos = input.data();
    if (rewrited_) {
        *seq = DecodeFixed64(begin_pos);
        input.remove_prefix(8);
    }
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
        case kTypeValue: {
            if (!(GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value))) {
                return Status::Corruption("bad WriteBatch Put");
            }
            isDel = false;
            break;
        }
        case kTypeDeletion: {
            if (!GetLengthPrefixedSlice(&input, &key)) {
                return Status::Corruption("bad WriteBatch Delete");
            }
            isDel = true;
            break;
        }
        default:
            return Status::Corruption("unknown WriteBatch tag");
    }
    pos += (input.data() - begin_pos);
    return Status::OK();
}

Status WriteBatch::Iterate(Handler* handler) const {
    Slice input(rep_);
    if (input.size() < kHeader) {
        return Status::Corruption("malformed WriteBatch (too small)");
    }

    input.remove_prefix(kHeader);
    Slice key, value;
    int found = 0;
    while (!input.empty()) {
        found++;
        char tag = input[0];
        input.remove_prefix(1);
        switch (tag) {
            case kTypeValue:
                if (GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value)) {
                    handler->Put(key, value);
                } else {
                    return Status::Corruption("bad WriteBatch Put");
                }
                break;
            case kTypeDeletion:
                if (GetLengthPrefixedSlice(&input, &key)) {
                    handler->Delete(key);
                } else {
                    return Status::Corruption("bad WriteBatch Delete");
                }
                break;
            default:
                return Status::Corruption("unknown WriteBatch tag");
        }
    }
    if (found != WriteBatchInternal::Count(this)) {
        return Status::Corruption("WriteBatch has wrong count");
    } else {
        return Status::OK();
    }
}

Status WriteBatch::Iterate(Handler* handler, uint64_t& pos, uint64_t file_numb) const { // pos是当前vlog文件的大小
    Slice input(rep_);
    if (input.size() < kHeader) {
        return Status::Corruption("malformed WriteBatch (too small)");
    }
    const char* last_pos = input.data();
    input.remove_prefix(kHeader); //移除掉WriteBatch的头部，它的头部前8个字节表示队首kv对的sequence
    //后4字节代表WriteBatch包含了多少个kv对。
    pos += kHeader;      //因为vlog记录的是WriteBatch，所以这kHeader字节也会被写入vlog
    last_pos += kHeader; // last_pos就是记录上一条记录插入vlog后vlog文件的大小
    Slice key, value;
    int found = 0;
    while (!input.empty()) { //遍历WriteBatch的每一条kv对
        found++;
        char tag = input[0];
        input.remove_prefix(1); //判断kv类型
        switch (tag) {
            case kTypeValue:
                if (GetLengthPrefixedSlice(&input, &key) && GetLengthPrefixedSlice(&input, &value)) {
                    const char* now_pos = input.data(); //如果是插入，解析出k和v
                    size_t len = now_pos - last_pos;    //计算出这条记录的大小
                    last_pos = now_pos;

                    std::string v;
                    PutVarint64(&v, len);
                    PutVarint32(&v, file_numb);
                    PutVarint64(&v, pos);
                    handler->Put(key, v);
                    pos = pos + len; //更新pos
                } else {
                    return Status::Corruption("bad WriteBatch Put");
                }
                break;
            case kTypeDeletion:
                if (GetLengthPrefixedSlice(&input, &key)) {
                    const char* now_pos = input.data();
                    size_t len = now_pos - last_pos;
                    pos = pos + len; //对于删除操作，不需要v值，更新pos
                    last_pos = now_pos;

                    handler->Delete(key); // delete的val是不是要写成文件号？
                } else {
                    return Status::Corruption("bad WriteBatch Delete");
                }
                break;
            default:
                return Status::Corruption("unknown WriteBatch tag");
        }
    }
    if (found != WriteBatchInternal::Count(this)) {
        return Status::Corruption("WriteBatch has wrong count");
    } else {
        return Status::OK();
    }
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(kTypeValue));
    PutLengthPrefixedSlice(&rep_, key);
    PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
    WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
    rep_.push_back(static_cast<char>(kTypeDeletion));
    PutLengthPrefixedSlice(&rep_, key);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
public:
    SequenceNumber sequence_;
    MemTable* mem_;
    virtual void Put(const Slice& key, const Slice& value) {
        mem_->Add(sequence_, kTypeValue, key, value);
        sequence_++;
    }
    virtual void Delete(const Slice& key) {
        mem_->Add(sequence_, kTypeDeletion, key, Slice());
        sequence_++;
    }
};
} // namespace

int WriteBatchInternal::Count(const WriteBatch* b) {
    if (b->rewrited_) {
        return DecodeFixed32(b->rep_.data());
    }
    return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
    if (b->rewrited_) {
        EncodeFixed32(&b->rep_[0], n);
    } else {
        EncodeFixed32(&b->rep_[8], n);
    }
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
    if (b->rewrited_) {
        return 0;
    }
    return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
    if (!b->rewrited_) EncodeFixed64(&b->rep_[0], seq);
}

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
    MemTableInserter inserter;
    inserter.sequence_ = WriteBatchInternal::Sequence(b);
    inserter.mem_ = memtable;
    return b->Iterate(&inserter);
}

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable, uint64_t& pos, uint64_t file_numb) {
    MemTableInserter inserter;
    inserter.sequence_ = WriteBatchInternal::Sequence(b);
    inserter.mem_ = memtable;
    return b->Iterate(&inserter, pos, file_numb);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
    assert(contents.size() >= (b->rewrited_ ? kSortHeader : kHeader));
    b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
    SetCount(dst, Count(dst) + Count(src));
    size_t head_size = dst->rewrited_ ? kSortHeader : kHeader;
    assert(contents.size() >= head_size);
    dst->rep_.append(src->rep_.data() + head_size, src->rep_.size() - head_size);
}

Status WriteBatchInternal::ParseRecord(const WriteBatch* batch, uint64_t& pos, Slice& key, Slice& value, bool& isDel,
                                       uint64_t* seq) {
    return batch->ParseRecord(pos, key, value, isDel, seq);
}
} // namespace leveldb
