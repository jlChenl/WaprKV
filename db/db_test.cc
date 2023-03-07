// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/db.h"

#include <math.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/table.h"
#include "time.h"
#include "unistd.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"
namespace leveldb {

static std::string RandomString(Random* rnd, int len) {
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
}

namespace {
class AtomicCounter {
private:
    port::Mutex mu_;
    int count_;

public:
    AtomicCounter() : count_(0) {}
    void Increment() { IncrementBy(1); }
    void IncrementBy(int count) {
        MutexLock l(&mu_);
        count_ += count;
    }
    int Read() {
        MutexLock l(&mu_);
        return count_;
    }
    void Reset() {
        MutexLock l(&mu_);
        count_ = 0;
    }
};

void DelayMilliseconds(int millis) { Env::Default()->SleepForMicroseconds(millis * 1000); }
} // namespace

// Special Env used to delay background operations
class SpecialEnv : public EnvWrapper {
public:
    // sstable/log Sync() calls are blocked while this pointer is non-NULL.
    port::AtomicPointer delay_data_sync_;

    // sstable/log Sync() calls return an error.
    port::AtomicPointer data_sync_error_;

    // Simulate no-space errors while this pointer is non-NULL.
    port::AtomicPointer no_space_;

    // Simulate non-writable file system while this pointer is non-NULL
    port::AtomicPointer non_writable_;

    // Force sync of manifest files to fail while this pointer is non-NULL
    port::AtomicPointer manifest_sync_error_;

    // Force write to manifest files to fail while this pointer is non-NULL
    port::AtomicPointer manifest_write_error_;

    bool count_random_reads_;
    AtomicCounter random_read_counter_;

    explicit SpecialEnv(Env* base) : EnvWrapper(base) {
        delay_data_sync_.Release_Store(NULL);
        data_sync_error_.Release_Store(NULL);
        no_space_.Release_Store(NULL);
        non_writable_.Release_Store(NULL);
        count_random_reads_ = false;
        manifest_sync_error_.Release_Store(NULL);
        manifest_write_error_.Release_Store(NULL);
    }

    Status NewWritableFile(const std::string& f, WritableFile** r) {
        class DataFile : public WritableFile {
        private:
            SpecialEnv* env_;
            WritableFile* base_;

        public:
            DataFile(SpecialEnv* env, WritableFile* base) : env_(env), base_(base) {}
            ~DataFile() { delete base_; }
            Status Append(const Slice& data) {
                if (env_->no_space_.Acquire_Load() != NULL) {
                    // Drop writes on the floor
                    return Status::OK();
                } else {
                    return base_->Append(data);
                }
            }
            Status Close() { return base_->Close(); }
            Status Flush() { return base_->Flush(); }
            Status Sync() {
                if (env_->data_sync_error_.Acquire_Load() != NULL) {
                    return Status::IOError("simulated data sync error");
                }
                while (env_->delay_data_sync_.Acquire_Load() != NULL) {
                    DelayMilliseconds(100);
                }
                return base_->Sync();
            }
        };
        class ManifestFile : public WritableFile {
        private:
            SpecialEnv* env_;
            WritableFile* base_;

        public:
            ManifestFile(SpecialEnv* env, WritableFile* b) : env_(env), base_(b) {}
            ~ManifestFile() { delete base_; }
            Status Append(const Slice& data) {
                if (env_->manifest_write_error_.Acquire_Load() != NULL) {
                    return Status::IOError("simulated writer error");
                } else {
                    return base_->Append(data);
                }
            }
            Status Close() { return base_->Close(); }
            Status Flush() { return base_->Flush(); }
            Status Sync() {
                if (env_->manifest_sync_error_.Acquire_Load() != NULL) {
                    return Status::IOError("simulated sync error");
                } else {
                    return base_->Sync();
                }
            }
        };

        if (non_writable_.Acquire_Load() != NULL) {
            return Status::IOError("simulated write error");
        }

        Status s = target()->NewWritableFile(f, r);
        if (s.ok()) {
            if (strstr(f.c_str(), ".ldb") != NULL || strstr(f.c_str(), ".log") != NULL) {
                *r = new DataFile(this, *r);
            } else if (strstr(f.c_str(), "MANIFEST") != NULL) {
                *r = new ManifestFile(this, *r);
            }
        }
        return s;
    }
    // for vlog file
    Status NewAppendableFile(const std::string& f, WritableFile** r) {
        class DataFile : public WritableFile {
        private:
            SpecialEnv* env_;
            WritableFile* base_;

        public:
            DataFile(SpecialEnv* env, WritableFile* base) : env_(env), base_(base) {}
            ~DataFile() { delete base_; }
            Status Append(const Slice& data) {
                if (env_->no_space_.Acquire_Load() != NULL) {
                    // Drop writes on the floor
                    return Status::OK();
                } else {
                    return base_->Append(data);
                }
            }
            Status Close() { return base_->Close(); }
            Status Flush() { return base_->Flush(); }
            Status Sync() {
                if (env_->data_sync_error_.Acquire_Load() != NULL) {
                    return Status::IOError("simulated data sync error");
                }
                while (env_->delay_data_sync_.Acquire_Load() != NULL) {
                    DelayMilliseconds(100);
                }
                return base_->Sync();
            }
        };

        if (non_writable_.Acquire_Load() != NULL) {
            return Status::IOError("simulated write error");
        }

        Status s = target()->NewAppendableFile(f, r);
        if (s.ok()) {
            if (strstr(f.c_str(), ".vlog") != NULL) {
                *r = new DataFile(this, *r);
            }
        }
        return s;
    }

    Status NewRandomAccessFile(const std::string& f, RandomAccessFile** r) {
        class CountingFile : public RandomAccessFile {
        private:
            RandomAccessFile* target_;
            AtomicCounter* counter_;

        public:
            CountingFile(RandomAccessFile* target, AtomicCounter* counter) : target_(target), counter_(counter) {}
            virtual ~CountingFile() { delete target_; }
            virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
                counter_->Increment();
                return target_->Read(offset, n, result, scratch);
            }
        };

        Status s = target()->NewRandomAccessFile(f, r);
        if (s.ok() && count_random_reads_) {
            *r = new CountingFile(*r, &random_read_counter_);
        }
        return s;
    }
};

class DBTest {
private:
    const FilterPolicy* filter_policy_;

    // Sequence of option configurations to try
    enum OptionConfig { kDefault, kReuse, kFilter, kUncompressed, kEnd };
    int option_config_;

public:
    std::string dbname_;
    SpecialEnv* env_;
    DB* db_;

    Options last_options_;

    std::map<uint64_t, uint64_t> zipf_map_;

    void LoadZipf(const char* str) {
        std::ifstream infile(str);
        if (!infile.is_open()) {
            std::cout << "Could not open the file \n";
            return;
        }
        uint64_t a, b = 0;
        std::string line;
        while (getline(infile, line)) {
            std::stringstream ss;
            ss << line;
            ss >> a;
            zipf_map_[a] = b++;
        }
        infile.close();
    }

    DBTest() : option_config_(kDefault), env_(new SpecialEnv(Env::Default())) {
        filter_policy_ = NewBloomFilterPolicy(10);
        dbname_ = test::TmpDir() + "/db_test";
        // DestroyDB(dbname_, Options());
        db_ = NULL;
        Reopen();
    }

    ~DBTest() {
        delete db_;
        DestroyDB(dbname_, Options());
        delete env_;
        delete filter_policy_;
    }

    // Switch to a fresh database with the next option configuration to
    // test.  Return false if there are no more configurations to test.
    bool ChangeOptions() {
        option_config_++;
        if (option_config_ >= kEnd) {
            return false;
        } else {
            DestroyAndReopen();
            return true;
        }
    }

    // Return the current option configuration.
    Options CurrentOptions() {
        Options options;
        options.reuse_logs = false;
        switch (option_config_) {
            case kReuse:
                options.reuse_logs = true;
                break;
            case kFilter:
                options.filter_policy = filter_policy_;
                break;
            case kUncompressed:
                options.compression = kNoCompression;
                break;
            default:
                break;
        }
        return options;
    }

    DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_); }

    void Reopen(Options* options = NULL) { ASSERT_OK(TryReopen(options)); }

    void Close() {
        delete db_;
        db_ = NULL;
    }

    void DestroyAndReopen(Options* options = NULL) {
        delete db_;
        db_ = NULL;
        DestroyDB(dbname_, Options());
        ASSERT_OK(TryReopen(options));
    }

    Status TryReopen(Options* options) {
        delete db_;
        db_ = NULL;
        Options opts;
        if (options != NULL) {
            opts = *options;
        } else {
            opts = CurrentOptions();
            opts.create_if_missing = true;
        }
        last_options_ = opts;

        return DB::Open(opts, dbname_, &db_);
    }

    Status Put(const std::string& k, const std::string& v) { return db_->Put(WriteOptions(), k, v); }

    Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

    // std::string Get(const std::string& k, const Snapshot* snapshot = NULL) {
    std::string Get(const std::string& k) {
        ReadOptions options;
        // options.snapshot = snapshot;
        std::string result;
        Status s = db_->Get(options, k, &result);
        if (s.IsNotFound()) {
            result = "NOT_FOUND";
        } else if (!s.ok()) {
            result = s.ToString();
        }
        return result;
    }

    // Return a string that contains all key,value pairs in order,
    // formatted like "(k1->v1)(k2->v2)".
    std::string Contents() {
        std::vector<std::string> forward;
        std::string result;
        Iterator* iter = db_->NewIterator(ReadOptions());
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            std::string s = IterStatus(iter);
            result.push_back('(');
            result.append(s);
            result.push_back(')');
            forward.push_back(s);
        }

        // Check reverse iteration results are the reverse of forward results
        size_t matched = 0;
        for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
            ASSERT_LT(matched, forward.size());
            ASSERT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
            matched++;
        }
        ASSERT_EQ(matched, forward.size());

        delete iter;
        return result;
    }

    std::string AllEntriesFor(const Slice& user_key) {
        Iterator* iter = dbfull()->TEST_NewInternalIterator();
        InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
        iter->Seek(target.Encode());
        std::string result;
        if (!iter->status().ok()) {
            result = iter->status().ToString();
        } else {
            result = "[ ";
            bool first = true;
            while (iter->Valid()) {
                ParsedInternalKey ikey;
                if (!ParseInternalKey(iter->key(), &ikey)) {
                    result += "CORRUPTED";
                } else {
                    if (last_options_.comparator->Compare(ikey.user_key, user_key) != 0) {
                        break;
                    }
                    if (!first) {
                        result += ", ";
                    }
                    first = false;
                    switch (ikey.type) {
                        case kTypeValue: {
                            std::string tmp;
                            uint64_t size;
                            std::pair<uint32_t, uint64_t> file_pos;
                            Slice val_ptr(iter->value());
                            ParsePtr(val_ptr, size, file_pos);
                            dbfull()->RealValue(size, file_pos.first, file_pos.second,
                                                &tmp); // iter->value()拿到的是真正value的地址
                            result += tmp;
                            break;
                        }
                        case kTypeDeletion:
                            result += "DEL";
                            break;
                    }
                }
                iter->Next();
            }
            if (!first) {
                result += " ";
            }
            result += "]";
        }
        delete iter;
        return result;
    }

    int NumTableFilesAtLevel(int level) {
        std::string property;
        ASSERT_TRUE(db_->GetProperty("leveldb.num-files-at-level" + NumberToString(level), &property));
        return atoi(property.c_str());
    }

    int TotalTableFiles() {
        int result = 0;
        for (int level = 0; level < config::kNumLevels; level++) {
            result += NumTableFilesAtLevel(level);
        }
        return result;
    }

    // Return spread of files per level
    std::string FilesPerLevel() {
        std::string result;
        int last_non_zero_offset = 0;
        for (int level = 0; level < config::kNumLevels; level++) {
            int f = NumTableFilesAtLevel(level);
            char buf[100];
            snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
            result += buf;
            if (f > 0) {
                last_non_zero_offset = result.size();
            }
        }
        result.resize(last_non_zero_offset);
        return result;
    }

    int CountFiles() {
        std::vector<std::string> files;
        env_->GetChildren(dbname_, &files);
        return static_cast<int>(files.size());
    }

    uint64_t Size(const Slice& start, const Slice& limit) {
        Range r(start, limit);
        uint64_t size;
        db_->GetApproximateSizes(&r, 1, &size);
        return size;
    }

    void Compact(const Slice& start, const Slice& limit) { db_->CompactRange(&start, &limit); }

    // Do n memtable compactions, each of which produces an sstable
    // covering the range [small,large].
    void MakeTables(int n, const std::string& small, const std::string& large) {
        for (int i = 0; i < n; i++) {
            Put(small, "begin");
            Put(large, "end");
            dbfull()->TEST_CompactMemTable();
        }
    }

    // Prevent pushing of new sstables into deeper levels by adding
    // tables that cover a specified range to all levels.
    void FillLevels(const std::string& smallest, const std::string& largest) {
        MakeTables(config::kNumLevels, smallest, largest);
    }

    void DumpFileCounts(const char* label) {
        fprintf(stderr, "---\n%s:\n", label);
        fprintf(stderr, "maxoverlap: %lld\n", static_cast<long long>(dbfull()->TEST_MaxNextLevelOverlappingBytes()));
        for (int level = 0; level < config::kNumLevels; level++) {
            int num = NumTableFilesAtLevel(level);
            if (num > 0) {
                fprintf(stderr, "  level %3d : %d files\n", level, num);
            }
        }
    }

    std::string DumpSSTableList() {
        std::string property;
        db_->GetProperty("leveldb.sstables", &property);
        return property;
    }

    std::string IterStatus(Iterator* iter) {
        std::string result;
        if (iter->Valid()) {
            result = iter->key().ToString() + "->" + iter->value().ToString();
        } else {
            result = "(invalid)";
        }
        return result;
    }

    bool DeleteAnSSTFile() {
        std::vector<std::string> filenames;
        ASSERT_OK(env_->GetChildren(dbname_, &filenames));
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
                ASSERT_OK(env_->DeleteFile(TableFileName(dbname_, number)));
                return true;
            }
        }
        return false;
    }

    // Returns number of files renamed.
    int RenameLDBToSST() {
        std::vector<std::string> filenames;
        ASSERT_OK(env_->GetChildren(dbname_, &filenames));
        uint64_t number;
        FileType type;
        int files_renamed = 0;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
                const std::string from = TableFileName(dbname_, number);
                const std::string to = SSTTableFileName(dbname_, number);
                ASSERT_OK(env_->RenameFile(from, to));
                files_renamed++;
            }
        }
        return files_renamed;
    }
};

// TEST(DBTest, Empty) {
//     do {
//         ASSERT_TRUE(db_ != NULL);
//         ASSERT_EQ("NOT_FOUND", Get("foo"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, garbageAll) //为了验证CleanVlog不会清理当前vlog，哪怕该vlog垃圾记录条数大于等于阀值
// {
//     Options options = CurrentOptions();
//     options.min_clean_threshold = 1;
//     Reopen(&options);
//     ASSERT_OK(Put("foo", "v1"));
//     ASSERT_OK(Put("bar", "b1"));
//     dbfull()->TEST_CompactMemTable(); // sst1(2层)
//     ASSERT_OK(Put("foo", "v2"));
//     dbfull()->TEST_CompactMemTable(); //生成sst2(1层),只含foo
//     ASSERT_EQ(NumTableFilesAtLevel(1), 1);
//     dbfull()->TEST_CompactRange(1, NULL, NULL); //导致1和2层合并，foo冲突
//     dbfull()->CleanVlog(); //并不会清理log1，虽然log1有一条垃圾信息foo，但因为log1是当前vlog，所以不能被清理
//     ASSERT_EQ("v2", Get("foo"));
//     ASSERT_EQ("b1", Get("bar"));
// }

// TEST(DBTest, garbageAllCleanVlog) //测试CleanVlog()
// {
//     Options options = CurrentOptions();
//     options.clean_threshold = 800;
//     options.max_vlog_size = 1000;
//     options.write_buffer_size = 100000;
//     options.min_clean_threshold = 2;
//     Reopen(&options);
//     ASSERT_OK(Put("foo", "v1"));      // log1
//     ASSERT_OK(Put("la", "l1"));       // log1
//     dbfull()->TEST_CompactMemTable(); // sst1(2层)
//     std::string big(100000, '1');
//     ASSERT_OK(Put(big, "11"));        // log1
//     ASSERT_OK(Put("foo", "v2"));      // log2  会生成log2，同时还生成sst2(只含big它们写在log1里)(2层)
//     dbfull()->TEST_CompactMemTable(); //生成sst3,只含foo(1层)
//     ASSERT_OK(Put(big, "22"));        // log2
//     ASSERT_OK(Put("foo", "v3"));      // log3  生成log3，同时还生成sst2(只含big，它们写在log2里)(1层)
//     dbfull()->TEST_CompactMemTable(); //生成sst4(0层)，只含foo
//     ASSERT_EQ(NumTableFilesAtLevel(0), 1);
//     ASSERT_EQ(NumTableFilesAtLevel(1), 2);
//     dbfull()->TEST_CompactRange(0, NULL, NULL); //导致1和0层合并，冲突,log2的foo失效
//     dbfull()->TEST_CompactRange(1, NULL, NULL); //导致1和2层合并，冲突,log1的foo和big失效
//     dbfull()->CleanVlog(); // 会把log1和log2都删除掉，同时把log1的l1/la以及log2的big/22重新put,并且写入log3中
//     // DelayMilliseconds(100);
//     ASSERT_EQ("v3", Get("foo"));
//     ASSERT_EQ("22", Get(big));
//     ASSERT_EQ("l1", Get("la"));
// }

// TEST(DBTest, garbagerefuse) //检验不重用manifest文件时，vloginfo headinfo tailinfo信息不会丢失
// {
//     Options options = CurrentOptions();
//     options.clean_threshold = 20;
//     options.max_vlog_size = 1000;
//     options.log_dropCount_threshold = 1;
//     Reopen(&options);
//     ASSERT_OK(Put("foo", "v1"));
//     ASSERT_OK(Put("bar", "b1"));
//     ASSERT_OK(Put("la", "l1"));
//     dbfull()->TEST_CompactMemTable(); //生成sst，在第2层
//     ASSERT_OK(Put("foo", "v3"));
//     ASSERT_OK(Put("bar", "b2"));
//     dbfull()->TEST_CompactMemTable(); //生成sst在第1层
//     Reopen(&options);
//     dbfull()->TEST_CompactRange(1, NULL,
//                                 NULL); //导致1和2层合并，不会触发垃圾回收，因为是当前vlog而且只有2条垃圾信息
//     ASSERT_EQ(2, dbfull()->TotalVlogFiles());
//     ASSERT_EQ(NumTableFilesAtLevel(2), 1); //第2层的文件就是big/11  以及前面1和2层合并后的文件
//     ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//     Reopen(&options);
//     ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//     std::string big(1000, '1');
//     ASSERT_OK(Put("big", big));
//     ASSERT_EQ(2, dbfull()->TotalVlogFiles());
//     ASSERT_OK(Put("bar", "b3")); // bar是写在新log文件里
//     ASSERT_EQ(3, dbfull()->TotalVlogFiles());
//     dbfull()->TEST_CompactMemTable();      //生成sst在第1层,生成新的vlog2
//     ASSERT_EQ(NumTableFilesAtLevel(2), 1); //第2层的文件就是big/11  以及前面1和2层合并后的文件
//     ASSERT_EQ(NumTableFilesAtLevel(1), 1);
//     ASSERT_EQ(3, dbfull()->TotalVlogFiles());

//     dbfull()->TEST_CompactRange(1, NULL, NULL); // 1和2层合并
//     DelayMilliseconds(100);                     //等垃圾回收完成
//     ASSERT_EQ(3, dbfull()->TotalVlogFiles());
//     ASSERT_EQ("v3", Get("foo"));
//     ASSERT_EQ(big, Get("big"));
//     ASSERT_EQ("b3", Get("bar"));
//     std::cout << "===================================\n";
//     std::cout << "===================================\n";
// }

// TEST(DBTest, garbage) //检验reopen时能否恢复vloginfo
// {
//     Options options = CurrentOptions();
//     options.clean_threshold = 20;
//     options.max_vlog_size = 1000;
//     options.log_dropCount_threshold = 1;
//     Reopen(&options);
//     ASSERT_OK(Put("foo", "v1"));
//     ASSERT_OK(Put("bar", "b1"));
//     ASSERT_OK(Put("la", "l1"));
//     dbfull()->TEST_CompactMemTable(); //生成sst，在第2层
//     ASSERT_OK(Put("foo", "v3"));
//     ASSERT_OK(Put("bar", "b2"));
//     dbfull()->TEST_CompactMemTable(); //生成sst在第1层
//     dbfull()->TEST_CompactRange(1, NULL,
//                                 NULL); //导致1和2层合并，不会触发垃圾回收，因为是当前vlog而且只有2条垃圾信息
//     ASSERT_EQ(2, dbfull()->TotalVlogFiles());
//     Reopen(&options);
//     std::string big(1000, '1');
//     ASSERT_OK(Put("big", big));
//     ASSERT_EQ(2, dbfull()->TotalVlogFiles());
//     ASSERT_OK(Put("bar", "b3")); // bar是写在新log文件里
//     ASSERT_EQ(3, dbfull()->TotalVlogFiles());
//     dbfull()->TEST_CompactMemTable();      //生成sst在第1层,生成新的vlog2
//     ASSERT_EQ(NumTableFilesAtLevel(2), 1); //第2层的文件就是big/11  以及前面1和2层合并后的文件
//     ASSERT_EQ(NumTableFilesAtLevel(1), 1);

//     dbfull()->TEST_CompactRange(1, NULL,
//                                 NULL); //导致1和2层合并，会触发vlog1的垃圾回收,说明vloginfo数据在reopen时成功恢复了
//     DelayMilliseconds(100); //等垃圾回收完成
//     ASSERT_EQ(3, dbfull()->TotalVlogFiles());
//     ASSERT_EQ("v3", Get("foo"));
//     ASSERT_EQ(big, Get("big"));
//     ASSERT_EQ("b3", Get("bar"));
// }

static std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%06d", i);
    return std::string(buf);
}

// TEST(DBTest, RecoverGarbageByTail) //检验reopen后能否根据tail继续上一次clean未完成的vlog的清理
// {
//     Options options = CurrentOptions();
//     options.clean_threshold = 20;
//     const int N = 60;
//     const int L = 100;
//     options.max_vlog_size = L * N;
//     //不要这句，是为了检验db_impl析构时会判断是否需要持久化当前vloginfo
//     // options.log_dropCount_threshold = 1;

//     Reopen(&options);
//     ASSERT_OK(Put("foo", "v1"));
//     ASSERT_OK(Put("bar", "b1"));
//     ASSERT_OK(Put("tom", "t1"));

//     for (int i = 0; i < N; i++) {
//         ASSERT_OK(Put(Key(i), Key(i) + std::string(L, 'v')));
//     }
//     dbfull()->TEST_CompactMemTable(); //生成sst，在第2层
//     ASSERT_OK(Put("foo", "v2"));
//     ASSERT_OK(Put("bar", "b2"));
//     ASSERT_OK(Put("tom", "t2"));
//     dbfull()->TEST_CompactMemTable();           //生成sst，在第2层
//     dbfull()->TEST_CompactRange(1, NULL, NULL); //导致1和2层合并，触发垃圾回收
//     ASSERT_OK(Put("foo", "v3"));
//     ASSERT_OK(Put("jack", "j1"));
//     Reopen(&options); //检验是否能从tail恢复垃圾回收
//     std::cout << "reopen database--------------------\n";
//     ASSERT_EQ("v3", Get("foo"));
//     ASSERT_EQ("b2", Get("bar"));
//     ASSERT_EQ("j1", Get("jack"));
//     DelayMilliseconds(100);
//     for (int i = 0; i < N; i++) {
//         ASSERT_EQ(Key(i) + std::string(L, 'v'), Get(Key(i)));
//     }

//     // for (int i = 0; i < N; i++) {
//     //     std::cout << Get(Key(i)) << "\n";
//     // }
// }

// TEST(DBTest, VlogManager) {
//     VlogManager vlog_manager(3);
//     Env* env = Env::Default();
//     log::VReader* vlog_reader[10];
//     for (uint32_t i = 0; i < 10; i++) {
//         std::string vlog_name = VLogFileName("db", i);
//         SequentialFile* vlr_file;
//         env->NewSequentialFile(vlog_name, &vlr_file);
//         vlog_reader[i] = new log::VReader(vlr_file, true, 0);
//         vlog_manager.AddVlog(i, vlog_reader[i]);
//         vlog_manager.SetNowVlog(i);
//     }
//     vlog_manager.AddDropCount(7);
//     for (int i = 0; i < 4; i++) {
//         vlog_manager.AddDropCount(8);
//         vlog_manager.AddDropCount(6);
//     }
//     std::string str;
//     ASSERT_TRUE(vlog_manager.Serialize(str));

//     VlogManager vlog_manager1(3);
//     log::VReader* vlog_reader1[10];
//     for (uint32_t i = 0; i < 10; i++) {
//         std::string vlog_name = VLogFileName("db", i);
//         SequentialFile* vlr_file;
//         env->NewSequentialFile(vlog_name, &vlr_file);
//         vlog_reader1[i] = new log::VReader(vlr_file, true, 0);
//         vlog_manager1.AddVlog(i, vlog_reader1[i]);
//         vlog_manager1.SetNowVlog(i);
//     }
//     ASSERT_TRUE(vlog_manager1.Deserialize(str));
//     for (uint32_t i = 0; i < 10; i++) {
//         ASSERT_EQ(vlog_manager.GetDropCount(i), vlog_manager1.GetDropCount(i));
//     }
//     for (int i = 0; i < 2; i++) {
//         ASSERT_TRUE(vlog_manager.HasVlogToClean());
//         ASSERT_TRUE(vlog_manager1.HasVlogToClean());
//         uint64_t numb, numb1;
//         numb = vlog_manager.GetVlogToClean();
//         numb1 = vlog_manager1.GetVlogToClean();
//         ASSERT_EQ(numb1, numb);
//         vlog_manager.RemoveVlog(numb);
//         vlog_manager.RemovePartialVlog(numb);
//         vlog_manager.RemoveFullVlog(numb);
//         vlog_manager1.RemoveVlog(numb1);
//         vlog_manager1.RemovePartialVlog(numb1);
//         vlog_manager1.RemoveFullVlog(numb1);
//     }
//     ASSERT_TRUE(!vlog_manager.HasVlogToClean());
//     ASSERT_TRUE(!vlog_manager1.HasVlogToClean());
// }

// TEST(DBTest, ReadWrite) {
//     do {
//         ASSERT_OK(Put("foo", "v1"));
//         ASSERT_EQ("v1", Get("foo"));
//         ASSERT_OK(Put("bar", "v2"));
//         ASSERT_OK(Put("foo", "v3"));
//         ASSERT_EQ("v3", Get("foo"));
//         ASSERT_EQ("v2", Get("bar"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, PutDeleteGet) {
//     do {
//         ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
//         ASSERT_EQ("v1", Get("foo"));
//         ASSERT_OK(db_->Put(WriteOptions(), "foo", "v2"));
//         ASSERT_EQ("v2", Get("foo"));
//         ASSERT_OK(db_->Delete(WriteOptions(), "foo"));
//         ASSERT_EQ("NOT_FOUND", Get("foo"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, GetFromImmutableLayer) {
//     do {
//         Options options = CurrentOptions();
//         options.env = env_;
//         options.write_buffer_size = 100000; // Small write buffer
//         Reopen(&options);

//         ASSERT_OK(Put("foo", "v1"));
//         ASSERT_EQ("v1", Get("foo"));
//         //加它的是为了增大sync的时间，builder sst的最后会调用sync
//         //也就间接增大了immemtable生成sst的时间，保证后面的get是从imm读的
//         env_->delay_data_sync_.Release_Store(env_); // Block sync calls
//         //没起到效果，因为我们是kv分离的，所以得把k弄大才可以
//         std::string k1(100000, '1');
//         std::string k2(100000, '2');
//         Put(k1, std::string(100000, 'x')); // Fill memtable
//         Put(k2, std::string(100000, 'y')); // Trigger compaction
//         ASSERT_EQ("v1", Get("foo"));
//         env_->delay_data_sync_.Release_Store(NULL); // Release sync calls
//     } while (ChangeOptions());
// }

// TEST(DBTest, GetFromVersions) {
//     do {
//         ASSERT_OK(Put("foo", "v1"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("v1", Get("foo")); //从sst文件获取
//     } while (ChangeOptions());
// }

// TEST(DBTest, GetMemUsage) {
//     do {
//         ASSERT_OK(Put("foo", "v1"));
//         std::string val;
//         ASSERT_TRUE(db_->GetProperty("leveldb.approximate-memory-usage", &val));
//         int mem_usage = atoi(val.c_str());
//         ASSERT_GT(mem_usage, 0);
//         ASSERT_LT(mem_usage, 5 * 1024 * 1024);
//     } while (ChangeOptions());
// }

// TEST(DBTest, GetSnapshot) {
// do {
//// Try with both a short key and a long key
// for (int i = 0; i < 2; i++) {
// std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
// ASSERT_OK(Put(key, "v1"));
// const Snapshot* s1 = db_->GetSnapshot();
// ASSERT_OK(Put(key, "v2"));
// ASSERT_EQ("v2", Get(key));
// ASSERT_EQ("v1", Get(key, s1));
// dbfull()->TEST_CompactMemTable();
// ASSERT_EQ("v2", Get(key));
// ASSERT_EQ("v1", Get(key, s1));
// db_->ReleaseSnapshot(s1);
//}
//} while (ChangeOptions());
/*}*/

// TEST(DBTest, GetLevel0Ordering) {
//     do {
//         // Check that we process level-0 files in correct order.  The code
//         // below generates two level-0 files where the earlier one comes
//         // before the later one in the level-0 file list since the earlier
//         // one has a smaller "smallest" key.
//         ASSERT_OK(Put("bar", "b"));
//         ASSERT_OK(Put("foo", "v1"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_OK(Put("foo", "v2"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("v2", Get("foo"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, GetOrderedByLevels) {
//     do {
//         ASSERT_OK(Put("foo", "v1"));
//         Compact("a", "z");
//         ASSERT_EQ("v1", Get("foo"));
//         ASSERT_OK(Put("foo", "v2"));
//         ASSERT_EQ("v2", Get("foo"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("v2", Get("foo"));
//     } while (ChangeOptions());
// }
// //从immem刷入生成的sst会pick一个合适的层放入，见WriteLevel0Table的PickLevelForMemTableOutput
// TEST(DBTest, GetPicksCorrectFile) {
//     do {
//         // Arrange to have multiple files in a non-level-0 level.
//         ASSERT_OK(Put("a", "va"));
//         Compact("a", "b");
//         ASSERT_OK(Put("x", "vx"));
//         Compact("x", "y");
//         ASSERT_OK(Put("f", "vf"));
//         Compact("f", "g");
//         ASSERT_EQ("va", Get("a"));
//         ASSERT_EQ("vf", Get("f"));
//         ASSERT_EQ("vx", Get("x"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, GetEncountersEmptyLevel) {
//     do {
//         // Arrange for the following to happen:
//         //   * sstable A in level 0
//         //   * nothing in level 1
//         //   * sstable B in level 2
//         // Then do enough Get() calls to arrange for an automatic compaction
//         // of sstable A.  A bug would cause the compaction to be marked as
//         // occurring at level 1 (instead of the correct level 0).

//         // Step 1: First place sstables in levels 0 and 2
//         int compaction_count = 0;
//         while (NumTableFilesAtLevel(0) == 0 || NumTableFilesAtLevel(2) == 0) {
//             ASSERT_LE(compaction_count, 100) << "could not fill levels 0 and 2";
//             compaction_count++;
//             // std::cout << "put kv pair in compaction_count=" << compaction_count << "\n";
//             Put("a", "begin");
//             Put("z", "end");
//             dbfull()->TEST_CompactMemTable();
//         }
//         // dynamic_cast<DBImpl*>(db_)->PrintFiles();

//         // Step 2: clear level 1 if necessary.
//         dbfull()->TEST_CompactRange(1, NULL, NULL);
//         ASSERT_EQ(NumTableFilesAtLevel(0), 1);
//         ASSERT_EQ(NumTableFilesAtLevel(1), 0);
//         ASSERT_EQ(NumTableFilesAtLevel(2), 1);

//         // Step 3: read a bunch of times
//         for (int i = 0; i < 1000; i++) {
//             ASSERT_EQ("NOT_FOUND", Get("missing"));
//         }

//         // Step 4: Wait for compaction to finish
//         DelayMilliseconds(1000);
//         ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//     } while (ChangeOptions());
// }

// TEST(DBTest, IterEmpty) {
//     Iterator* iter = db_->NewIterator(ReadOptions());

//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->Seek("foo");
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     delete iter;
// }

// TEST(DBTest, IterSingle) {
//     ASSERT_OK(Put("a", "va"));
//     Iterator* iter = db_->NewIterator(ReadOptions());

//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");
//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");
//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->Seek("");
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->Seek("a");
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->Seek("b");
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     delete iter;
// }

// TEST(DBTest, IterMulti) {
//     ASSERT_OK(Put("a", "va"));
//     ASSERT_OK(Put("b", "vb"));
//     ASSERT_OK(Put("c", "vc"));
//     Iterator* iter = db_->NewIterator(ReadOptions());

//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "b->vb");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");
//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "b->vb");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");
//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->Seek("");
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Seek("a");
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Seek("ax");
//     ASSERT_EQ(IterStatus(iter), "b->vb");
//     iter->Seek("b");
//     ASSERT_EQ(IterStatus(iter), "b->vb");
//     iter->Seek("z");
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     // Switch from reverse to forward
//     iter->SeekToLast();
//     iter->Prev();
//     iter->Prev();
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "b->vb");

//     // Switch from forward to reverse
//     iter->SeekToFirst();
//     iter->Next();
//     iter->Next();
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "b->vb");

//     // Make sure iter stays at snapshot
//     ASSERT_OK(Put("a", "va2"));
//     ASSERT_OK(Put("a2", "va3"));
//     ASSERT_OK(Put("b", "vb2"));
//     ASSERT_OK(Put("c", "vc2"));
//     ASSERT_OK(Delete("b"));
//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "b->vb");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");
//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "b->vb");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     delete iter;
// }

// TEST(DBTest, IterSmallAndLargeMix) {
//     ASSERT_OK(Put("a", "va"));
//     ASSERT_OK(Put("b", std::string(100000, 'b')));
//     ASSERT_OK(Put("c", "vc"));
//     ASSERT_OK(Put("d", std::string(100000, 'd')));
//     ASSERT_OK(Put("e", std::string(100000, 'e')));

//     Iterator* iter = db_->NewIterator(ReadOptions());

//     iter->SeekToFirst();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
//     iter->Next();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     iter->SeekToLast();
//     ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "c->vc");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "a->va");
//     iter->Prev();
//     ASSERT_EQ(IterStatus(iter), "(invalid)");

//     delete iter;
// }

// TEST(DBTest, IterMultiWithDelete) {
//     do {
//         ASSERT_OK(Put("a", "va"));
//         ASSERT_OK(Put("b", "vb"));
//         ASSERT_OK(Put("c", "vc"));
//         ASSERT_OK(Delete("b"));
//         ASSERT_EQ("NOT_FOUND", Get("b"));

//         Iterator* iter = db_->NewIterator(ReadOptions());
//         iter->Seek("c");
//         ASSERT_EQ(IterStatus(iter), "c->vc");
//         iter->Prev();
//         ASSERT_EQ(IterStatus(iter), "a->va");
//         delete iter;
//     } while (ChangeOptions());
// }

// TEST(DBTest, Recover) {
//     do {
//         ASSERT_OK(Put("foo", "v1"));
//         ASSERT_OK(Put("baz", "v5")); //在mem中

//         Reopen(); //生成了sst
//         ASSERT_EQ("v1", Get("foo"));

//         ASSERT_EQ("v1", Get("foo"));
//         ASSERT_EQ("v5", Get("baz"));
//         ASSERT_OK(Put("bar", "v2"));
//         ASSERT_OK(Put("foo", "v3"));

//         Reopen(); //生成第二个sst
//         ASSERT_EQ("v3", Get("foo"));
//         ASSERT_OK(Put("foo", "v4"));
//         ASSERT_EQ("v4", Get("foo"));
//         ASSERT_EQ("v2", Get("bar"));
//         ASSERT_EQ("v5", Get("baz"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, RecoveryWithEmptyLog) {
//     do {
//         ASSERT_OK(Put("foo", "v1"));
//         ASSERT_OK(Put("foo", "v2"));
//         Reopen();
//         Reopen(); //相当没啥恢复的，对应db_impl.cc：(compactions == 0 && mem == NULL)
//         ASSERT_OK(Put("foo", "v3"));
//         Reopen();
//         ASSERT_EQ("v3", Get("foo"));
//     } while (ChangeOptions());
// }

// // Check that writes done during a memtable compaction are recovered
// // if the database is shutdown during the memtable compaction.
// TEST(DBTest, RecoverDuringMemtableCompaction) {
//     do {
//         Options options = CurrentOptions();
//         options.env = env_;
//         options.write_buffer_size = 1000000;
//         options.max_vlog_size = 1000000;
//         Reopen(&options);

//         // Trigger a long memtable compaction and reopen the database during it
//         ASSERT_OK(Put("foo", "v1"));    // Goes to 1st log file
//         std::string big1(1000000, '1'); //因为我们是kv分离的，大v对触发合并无效，因此应大k
//         std::string big2(1000, '2');
//         ASSERT_OK(Put(big1, std::string(1000000, 'x'))); // Fills memtable
//         ASSERT_OK(Put(big2, std::string(1000, 'y')));    // Triggers compaction
//         ASSERT_OK(Put("bar", "v2"));                     // big2和bar都会记录在新日志Goes to new log file

//         Reopen(&options); //太快以至于上次合并没完成，会在recovervlog回放日志时mem超过size而生成sst文件
//         ASSERT_EQ("v1", Get("foo"));
//         ASSERT_EQ("v2", Get("bar"));
//         ASSERT_EQ(std::string(1000000, 'x'), Get(big1));
//         ASSERT_EQ(std::string(1000, 'y'), Get(big2));
//     } while (ChangeOptions());
// }

// TEST(DBTest, MinorCompactionsHappen) {
//     Options options = CurrentOptions();
//     options.write_buffer_size = 10000;
//     Reopen(&options);

//     const int N = 500;

//     int starting_num_tables = TotalTableFiles();
//     for (int i = 0; i < N; i++) {
//         ASSERT_OK(Put(Key(i) + std::string(1000, 'v'), Key(i)));
//     } // k变大，才会触发minor合并
//     int ending_num_tables = TotalTableFiles();
//     ASSERT_GT(ending_num_tables, starting_num_tables);

//     for (int i = 0; i < N; i++) {
//         ASSERT_EQ(Get(Key(i) + std::string(1000, 'v')), Key(i));
//     }

//     Reopen();

//     for (int i = 0; i < N; i++) {
//         ASSERT_EQ(Get(Key(i) + std::string(1000, 'v')), Key(i));
//     }
// }

// TEST(DBTest, RecoverWithLargeLog) { // reopen恢复时减小了write_buffer_size，会在恢复过程中生成多个sst
//     {
//         Options options = CurrentOptions();
//         Reopen(&options); // kv分离测试用例中，k大才会触发合并，所以需要大k
//         ASSERT_OK(Put(std::string(200000, '1'), "big1"));
//         ASSERT_OK(Put(std::string(200000, '2'), "big2"));
//         ASSERT_OK(Put("small3", std::string(10, '3')));
//         ASSERT_OK(Put("small4", std::string(10, '4')));
//         ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//     }

//     // Make sure that if we re-open with a small write buffer size that
//     // we flush table files in the middle of a large log file.
//     Options options = CurrentOptions();
//     options.write_buffer_size = 100000;
//     Reopen(&options);
//     ASSERT_EQ(NumTableFilesAtLevel(0), 3);
//     ASSERT_EQ(Get(std::string(200000, '1')), "big1");
//     ASSERT_EQ(Get(std::string(200000, '2')), "big2");
//     ASSERT_EQ(std::string(10, '3'), Get("small3"));
//     ASSERT_EQ(std::string(10, '4'), Get("small4"));
//     ASSERT_GT(NumTableFilesAtLevel(0), 1);
// }

// TEST(DBTest, CompactionsGenerateMultipleFiles) {
//     Options options = CurrentOptions();
//     options.write_buffer_size = 100000000; // Large write buffer
//     Reopen(&options);

//     Random rnd(301);

//     // Write 8MB (80 values, each 100K)
//     ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//     //  std::vector<std::string> values;
//     std::vector<std::string> keys;
//     for (int i = 0; i < 80; i++) {
//         //    values.push_back(RandomString(&rnd, 100000));
//         //    ASSERT_OK(Put(Key(i), values[i]));
//         keys.push_back(RandomString(&rnd, 100000));
//         ASSERT_OK(Put(keys[i], Key(i)));
//     }

//     // Reopening moves updates to level-0
//     Reopen(&options); //会在level0生成一个远超sst文件大小上限的大sst文件
//     dbfull()->TEST_CompactRange(0, NULL, NULL);

//     ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//     ASSERT_GT(NumTableFilesAtLevel(1), 1); //合并过程会拆分成多个sst文件，因为前面write_buffer_size太大
//     for (int i = 0; i < 80; i++) {
//         //    ASSERT_EQ(Get(Key(i)), values[i]);
//         ASSERT_EQ(Get(keys[i]), Key(i));
//     }
// }

// TEST(DBTest, RepeatedWritesToSameKey) { //验证level0层可以文件冲突，其余层不允许文件间范围冲突
//     Options options = CurrentOptions();
//     options.env = env_;
//     options.write_buffer_size = 100000; // Small write buffer
//     Reopen(&options);

//     // We must have at most one file per level except for level-0,
//     // which may have up to kL0_StopWritesTrigger files.
//     const int kMaxFiles = config::kNumLevels + config::kL0_StopWritesTrigger;
//     //因为key都一样，所以大于0层的各层最多只能有1个文件，因此db的所有sst文件数小于等于config::kNumLevels +
//     // config::kL0_StopWritesTrigger;
//     //  Random rnd(301);
//     //  std::string value = RandomString(&rnd, 2 * options.write_buffer_size);
//     std::string key(2 * options.write_buffer_size, 'k'); // kv分离版本，key大才有用
//     for (int i = 0; i < 5 * kMaxFiles; i++) {
//         // Put("key", value);
//         Put(key, Key(i));
//         ASSERT_LE(TotalTableFiles(), kMaxFiles);
//         // fprintf(stderr, "after %d: %d files\n", int(i + 1), TotalTableFiles());
//     }
// }

// TEST(DBTest, SparseMerge) {
//     Options options = CurrentOptions();
//     options.compression = kNoCompression;
//     Reopen(&options);

//     FillLevels("A", "Z");

//     // Suppose there is:
//     //    small amount of data with prefix A
//     //    large amount of data with prefix B
//     //    small amount of data with prefix C
//     // and that recent updates have made small changes to all three prefixes.
//     // Check that we do not do a compaction that merges all of B in one shot.
//     const std::string value(1000, 'x');
//     Put("A", "va");
//     // Write approximately 100MB of "B" values
//     for (int i = 0; i < 100000; i++) {
//         char key[100];
//         snprintf(key, sizeof(key), "B%010d", i);
//         Put(key, value);
//     }
//     Put("C", "vc");
//     dbfull()->TEST_CompactMemTable();
//     dbfull()->TEST_CompactRange(0, NULL, NULL);

//     // Make sparse update
//     Put("A", "va2");
//     Put("B100", "bvalue2");
//     Put("C", "vc2");
//     dbfull()->TEST_CompactMemTable();

//     // Compactions should not cause us to create a situation where
//     // a file overlaps too much data at the next level.
//     ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20 * 1048576);
//     dbfull()->TEST_CompactRange(0, NULL, NULL);
//     ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20 * 1048576);
//     dbfull()->TEST_CompactRange(1, NULL, NULL);
//     ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20 * 1048576);
// }

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
    bool result = (val >= low) && (val <= high);
    if (!result) {
        fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n", (unsigned long long)(val),
                (unsigned long long)(low), (unsigned long long)(high));
    }
    return result;
}

// TEST(DBTest, ApproximateSizes) {
//     do {
//         Options options = CurrentOptions();
//         options.write_buffer_size = 100000000; // Large write buffer
//         options.compression = kNoCompression;
//         DestroyAndReopen();

//         ASSERT_TRUE(Between(Size("", "xyz"), 0, 0));
//         Reopen(&options);
//         ASSERT_TRUE(Between(Size("", "xyz"), 0, 0));

//         // Write 8MB (80 values, each 100K)
//         ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//         const int N = 80;
//         static const int S1 = 100000;
//         static const int S2 = 105000; // Allow some expansion from metadata
//         Random rnd(301);
//         for (int i = 0; i < N; i++) {
//             ASSERT_OK(Put(Key(i), RandomString(&rnd, S1)));
//         }

//         // 0 because GetApproximateSizes() does not account for memtable space
//         ASSERT_TRUE(Between(Size("", Key(50)), 0, 0));

//         if (options.reuse_logs) {
//             // Recovery will reuse memtable, and GetApproximateSizes() does not
//             // account for memtable usage;
//             Reopen(&options);
//             ASSERT_TRUE(Between(Size("", Key(50)), 0, 0));
//             continue;
//         }

//         // Check sizes across recovery by reopening a few times
//         for (int run = 0; run < 3; run++) {
//             Reopen(&options);

//             for (int compact_start = 0; compact_start < N; compact_start += 10) {
//                 for (int i = 0; i < N; i += 10) {
//                     ASSERT_TRUE(Between(Size("", Key(i)), S1 * i, S2 * i));
//                     ASSERT_TRUE(Between(Size("", Key(i) + ".suffix"), S1 * (i + 1), S2 * (i + 1)));
//                     ASSERT_TRUE(Between(Size(Key(i), Key(i + 10)), S1 * 10, S2 * 10));
//                 }
//                 ASSERT_TRUE(Between(Size("", Key(50)), S1 * 50, S2 * 50));
//                 ASSERT_TRUE(Between(Size("", Key(50) + ".suffix"), S1 * 50, S2 * 50));

//                 std::string cstart_str = Key(compact_start);
//                 std::string cend_str = Key(compact_start + 9);
//                 Slice cstart = cstart_str;
//                 Slice cend = cend_str;
//                 dbfull()->TEST_CompactRange(0, &cstart, &cend);
//             }

//             ASSERT_EQ(NumTableFilesAtLevel(0), 0);
//             ASSERT_GT(NumTableFilesAtLevel(1), 0);
//         }
//     } while (ChangeOptions());
// }

// TEST(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
//     do {
//         Options options = CurrentOptions();
//         options.compression = kNoCompression;
//         Reopen();

//         Random rnd(301);
//         std::string big1 = RandomString(&rnd, 100000);
//         ASSERT_OK(Put(Key(0), RandomString(&rnd, 10000)));
//         ASSERT_OK(Put(Key(1), RandomString(&rnd, 10000)));
//         ASSERT_OK(Put(Key(2), big1));
//         ASSERT_OK(Put(Key(3), RandomString(&rnd, 10000)));
//         ASSERT_OK(Put(Key(4), big1));
//         ASSERT_OK(Put(Key(5), RandomString(&rnd, 10000)));
//         ASSERT_OK(Put(Key(6), RandomString(&rnd, 300000)));
//         ASSERT_OK(Put(Key(7), RandomString(&rnd, 10000)));

//         if (options.reuse_logs) {
//             // Need to force a memtable compaction since recovery does not do so.
//             ASSERT_OK(dbfull()->TEST_CompactMemTable());
//         }

//         // Check sizes across recovery by reopening a few times
//         for (int run = 0; run < 3; run++) {
//             Reopen(&options);

//             ASSERT_TRUE(Between(Size("", Key(0)), 0, 0));
//             ASSERT_TRUE(Between(Size("", Key(1)), 10000, 11000));
//             ASSERT_TRUE(Between(Size("", Key(2)), 20000, 21000));
//             ASSERT_TRUE(Between(Size("", Key(3)), 120000, 121000));
//             ASSERT_TRUE(Between(Size("", Key(4)), 130000, 131000));
//             ASSERT_TRUE(Between(Size("", Key(5)), 230000, 231000));
//             ASSERT_TRUE(Between(Size("", Key(6)), 240000, 241000));
//             ASSERT_TRUE(Between(Size("", Key(7)), 540000, 541000));
//             ASSERT_TRUE(Between(Size("", Key(8)), 550000, 560000));

//             ASSERT_TRUE(Between(Size(Key(3), Key(5)), 110000, 111000));

//             dbfull()->TEST_CompactRange(0, NULL, NULL);
//         }
//     } while (ChangeOptions());
// }

// TEST(DBTest, IteratorPinsRef) {
//     Put("foo", "hello");

//     // Get iterator that will yield the current contents of the DB.
//     Iterator* iter = db_->NewIterator(ReadOptions());

//     // Write to force compactions
//     Put("foo", "newvalue1");
//     for (int i = 0; i < 100; i++) {
//         ASSERT_OK(Put(Key(i), Key(i) + std::string(100000, 'v'))); // 100K values
//     }
//     Put("foo", "newvalue2");

//     iter->SeekToFirst();
//     ASSERT_TRUE(iter->Valid());
//     ASSERT_EQ("foo", iter->key().ToString());
//     ASSERT_EQ("hello", iter->value().ToString());
//     iter->Next();
//     ASSERT_TRUE(!iter->Valid());
//     delete iter;
// }

// TEST(DBTest, HiddenValuesAreRemoved) {
// do {
// Random rnd(301);
// FillLevels("a", "z");

// std::string big = RandomString(&rnd, 50000);
// Put("foo", big);
// Put("pastfoo", "v");
// const Snapshot* snapshot = db_->GetSnapshot();
// Put("foo", "tiny");
// Put("pastfoo2", "v2");        // Advance sequence number one more

// ASSERT_OK(dbfull()->TEST_CompactMemTable());
// ASSERT_GT(NumTableFilesAtLevel(0), 0);

// ASSERT_EQ(big, Get("foo", snapshot));
////kv分离，big存在vlog中，因此big不会在sst文件里，Size不会包含5000big的
////    ASSERT_TRUE(Between(Size("", "pastfoo"), 50000, 60000));
// db_->ReleaseSnapshot(snapshot);
// ASSERT_EQ(AllEntriesFor("foo"), "[ tiny, " + big + " ]");
// Slice x("x");
// dbfull()->TEST_CompactRange(0, NULL, &x);
// ASSERT_EQ(AllEntriesFor("foo"), "[ tiny ]");
// ASSERT_EQ(NumTableFilesAtLevel(0), 0);
// ASSERT_GE(NumTableFilesAtLevel(1), 1);
// dbfull()->TEST_CompactRange(1, NULL, &x);
// ASSERT_EQ(AllEntriesFor("foo"), "[ tiny ]");

// ASSERT_TRUE(Between(Size("", "pastfoo"), 0, 1000));
//} while (ChangeOptions());
//}

// TEST(DBTest, DeletionMarkers1) {
//     Put("foo", "v1");
//     ASSERT_OK(dbfull()->TEST_CompactMemTable());
//     const int last = config::kMaxMemCompactLevel;
//     ASSERT_EQ(NumTableFilesAtLevel(last), 1); // foo => v1 is now in last level

//     // Place a table at level last-1 to prevent merging with preceding mutation
//     Put("a", "begin");
//     Put("z", "end");
//     dbfull()->TEST_CompactMemTable();
//     ASSERT_EQ(NumTableFilesAtLevel(last), 1);
//     ASSERT_EQ(NumTableFilesAtLevel(last - 1), 1);

//     Delete("foo");
//     Put("foo", "v2");
//     ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
//     ASSERT_OK(dbfull()->TEST_CompactMemTable()); // Moves to level last-2
//     ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
//     Slice z("z");
//     dbfull()->TEST_CompactRange(last - 2, NULL, &z); // foo/v2和foo/del冲突，后者会消失在sst中
//     // DEL eliminated, but v1 remains because we aren't compacting that level
//     // (DEL can be eliminated because v2 hides v1).
//     ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");
//     dbfull()->TEST_CompactRange(last - 1, NULL, NULL); // foo/v2和foo/v1冲突，后者会消失在sst中
//     // Merging last-1 w/ last, so we are the base level for "foo", so
//     // DEL is removed.  (as is v1).
//     ASSERT_EQ(AllEntriesFor("foo"), "[ v2 ]");
// }

// TEST(DBTest,
//      DeletionMarkers2) { //验证del只有在合并时候才会真正删除记录，同时若其他层均无该k对应的记录，该del记录也可以删掉
//     Put("foo", "v1");
//     ASSERT_OK(dbfull()->TEST_CompactMemTable());
//     const int last = config::kMaxMemCompactLevel;
//     ASSERT_EQ(NumTableFilesAtLevel(last), 1); // foo => v1 is now in last level

//     // Place a table at level last-1 to prevent merging with preceding mutation
//     Put("a", "begin");
//     Put("z", "end");
//     dbfull()->TEST_CompactMemTable();
//     ASSERT_EQ(NumTableFilesAtLevel(last), 1);
//     ASSERT_EQ(NumTableFilesAtLevel(last - 1), 1);

//     Delete("foo");
//     ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
//     ASSERT_OK(dbfull()->TEST_CompactMemTable()); // Moves to level last-2
//     ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
//     dbfull()->TEST_CompactRange(last - 2, NULL, NULL);
//     // DEL kept: "last" file overlaps
//     ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
//     dbfull()->TEST_CompactRange(last - 1, NULL, NULL);
//     // Merging last-1 w/ last, so we are the base level for "foo", so
//     // DEL is removed.  (as is v1).
//     ASSERT_EQ(AllEntriesFor("foo"), "[ ]");
// }

// TEST(DBTest, OverlapInLevel0) { // level0层不同sst文件中同时包含一条相同k的put与del操作,get会返回not find
//     do {
//         ASSERT_EQ(config::kMaxMemCompactLevel, 2) << "Fix test to match config";

//         // Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
//         ASSERT_OK(Put("100", "v100"));
//         ASSERT_OK(Put("999", "v999"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_OK(Delete("100"));
//         ASSERT_OK(Delete("999"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("0,1,1", FilesPerLevel());

//         // Make files spanning the following ranges in level-0:
//         //  files[0]  200 .. 900
//         //  files[1]  300 .. 500
//         // Note that files are sorted by smallest key.
//         ASSERT_OK(Put("300", "v300"));
//         ASSERT_OK(Put("500", "v500"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_OK(Put("200", "v200"));
//         ASSERT_OK(Put("600", "v600"));
//         ASSERT_OK(Put("900", "v900"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("2,1,1", FilesPerLevel());

//         ASSERT_EQ("2,1,1", FilesPerLevel());
//         // Compact away the placeholder files we created initially
//         dbfull()->TEST_CompactRange(1, NULL, NULL);
//         ASSERT_EQ("2", FilesPerLevel());
//         dbfull()->TEST_CompactRange(2, NULL, NULL);
//         ASSERT_EQ("2", FilesPerLevel());

//         // Do a memtable compaction.  Before bug-fix, the compaction would
//         // not detect the overlap with level-0 files and would incorrectly place
//         // the deletion in a deeper level.
//         ASSERT_OK(Delete("600"));
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("3", FilesPerLevel());
//         ASSERT_EQ("NOT_FOUND", Get("600"));
//     } while (ChangeOptions());
// }

// TEST(DBTest, L0_CompactionBug_Issue44_a) {
//     Reopen();
//     ASSERT_OK(Put("b", "v"));
//     Reopen();
//     ASSERT_OK(Delete("b"));
//     ASSERT_OK(Delete("a"));
//     Reopen();
//     ASSERT_OK(Delete("a"));
//     Reopen();
//     ASSERT_OK(Put("a", "v"));
//     Reopen();
//     Reopen();
//     ASSERT_EQ("(a->v)", Contents());
//     DelayMilliseconds(1000); // Wait for compaction to finish
//     ASSERT_EQ("(a->v)", Contents());
// }

// TEST(DBTest, L0_CompactionBug_Issue44_b) {
//     Reopen();
//     Put("", "");
//     Reopen();
//     Delete("e");
//     Put("", "");
//     Reopen();
//     Put("c", "cv");
//     Reopen();
//     Put("", "");
//     Reopen();
//     Put("", "");
//     DelayMilliseconds(1000); // Wait for compaction to finish
//     Reopen();
//     Put("d", "dv");
//     Reopen();
//     Put("", "");
//     Reopen();
//     Delete("d");
//     Delete("b");
//     Reopen();
//     ASSERT_EQ("(->)(c->cv)", Contents());
//     DelayMilliseconds(1000); // Wait for compaction to finish
//     ASSERT_EQ("(->)(c->cv)", Contents());
// }

// TEST(DBTest, ComparatorCheck) {
//     class NewComparator : public Comparator {
//     public:
//         virtual const char* Name() const { return "leveldb.NewComparator"; }
//         virtual int Compare(const Slice& a, const Slice& b) const { return BytewiseComparator()->Compare(a, b); }
//         virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
//             BytewiseComparator()->FindShortestSeparator(s, l);
//         }
//         virtual void FindShortSuccessor(std::string* key) const { BytewiseComparator()->FindShortSuccessor(key); }
//     };
//     NewComparator cmp;
//     Options new_options = CurrentOptions();
//     new_options.comparator = &cmp;
//     Status s = TryReopen(&new_options);
//     ASSERT_TRUE(!s.ok());
//     ASSERT_TRUE(s.ToString().find("comparator") != std::string::npos) << s.ToString();
// }

// TEST(DBTest, CustomComparator) { //使用自定义的Comparator作为userComparator
//     class NumberComparator : public Comparator {
//     public:
//         virtual const char* Name() const { return "test.NumberComparator"; }
//         virtual int Compare(const Slice& a, const Slice& b) const { return ToNumber(a) - ToNumber(b); }
//         virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
//             ToNumber(*s); // Check format
//             ToNumber(l);  // Check format
//         }
//         virtual void FindShortSuccessor(std::string* key) const {
//             ToNumber(*key); // Check format
//         }

//     private:
//         static int ToNumber(const Slice& x) {
//             // Check that there are no extra characters.
//             ASSERT_TRUE(x.size() >= 2 && x[0] == '[' && x[x.size() - 1] == ']') << EscapeString(x);
//             int val;
//             char ignored;
//             ASSERT_TRUE(sscanf(x.ToString().c_str(), "[%i]%c", &val, &ignored) == 1) << EscapeString(x);
//             return val;
//         }
//     };
//     NumberComparator cmp;
//     Options new_options = CurrentOptions();
//     new_options.create_if_missing = true;
//     new_options.comparator = &cmp;
//     new_options.filter_policy = NULL;     // Cannot use bloom filters
//     new_options.write_buffer_size = 1000; // Compact more often
//     DestroyAndReopen(&new_options);
//     ASSERT_OK(Put("[10]", "ten"));
//     ASSERT_OK(Put("[0x14]", "twenty"));
//     for (int i = 0; i < 2; i++) {
//         ASSERT_EQ("ten", Get("[10]"));
//         ASSERT_EQ("ten", Get("[0xa]"));
//         ASSERT_EQ("twenty", Get("[20]"));
//         ASSERT_EQ("twenty", Get("[0x14]"));
//         ASSERT_EQ("NOT_FOUND", Get("[15]"));
//         ASSERT_EQ("NOT_FOUND", Get("[0xf]"));
//         Compact("[0]", "[9999]");
//     }
//     for (int run = 0; run < 2; run++) {
//         for (int i = 0; i < 1000; i++) {
//             char buf[100];
//             snprintf(buf, sizeof(buf), "[%d]", i * 10);
//             ASSERT_OK(Put(buf, buf));
//         }
//         Compact("[0]", "[1000000]");
//     }
// }

// TEST(DBTest, ManualCompaction) {
//     ASSERT_EQ(config::kMaxMemCompactLevel, 2) << "Need to update this test to match kMaxMemCompactLevel";

//     MakeTables(3, "p", "q");
//     ASSERT_EQ("1,1,1", FilesPerLevel());

//     // Compaction range falls before files
//     Compact("", "c");
//     ASSERT_EQ("1,1,1", FilesPerLevel());

//     // Compaction range falls after files
//     Compact("r", "z");
//     ASSERT_EQ("1,1,1", FilesPerLevel());

//     // Compaction range overlaps files
//     Compact("p1", "p9");
//     ASSERT_EQ("0,0,1", FilesPerLevel());

//     // Populate a different range
//     MakeTables(3, "c", "e");
//     ASSERT_EQ("1,1,2", FilesPerLevel());

//     // Compact just the new range
//     Compact("b", "f");
//     ASSERT_EQ("0,0,2", FilesPerLevel());

//     // Compact all
//     MakeTables(1, "a", "z");
//     ASSERT_EQ("0,1,2", FilesPerLevel());
//     db_->CompactRange(NULL, NULL);
//     ASSERT_EQ("0,0,1", FilesPerLevel());
// }

// TEST(DBTest, DBOpen_Options) {
//     std::string dbname = test::TmpDir() + "/db_options_test";
//     DestroyDB(dbname, Options());

//     // Does not exist, and create_if_missing == false: error
//     DB* db = NULL;
//     Options opts;
//     opts.create_if_missing = false;
//     Status s = DB::Open(opts, dbname, &db);
//     ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != NULL);
//     ASSERT_TRUE(db == NULL);

//     // Does not exist, and create_if_missing == true: OK
//     opts.create_if_missing = true;
//     s = DB::Open(opts, dbname, &db);
//     ASSERT_OK(s);
//     ASSERT_TRUE(db != NULL);

//     delete db;
//     db = NULL;

//     // Does exist, and error_if_exists == true: error
//     opts.create_if_missing = false;
//     opts.error_if_exists = true;
//     s = DB::Open(opts, dbname, &db);
//     ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != NULL);
//     ASSERT_TRUE(db == NULL);

//     // Does exist, and error_if_exists == false: OK
//     opts.create_if_missing = true;
//     opts.error_if_exists = false;
//     s = DB::Open(opts, dbname, &db);
//     ASSERT_OK(s);
//     ASSERT_TRUE(db != NULL);

//     delete db;
//     db = NULL;
// }

// TEST(DBTest, Locking) {
//     DB* db2 = NULL;
//     Status s = DB::Open(CurrentOptions(), dbname_, &db2);
//     ASSERT_TRUE(!s.ok()) << "Locking did not prevent re-opening db";
// }

// // Check that number of files does not grow when we are out of space
// TEST(DBTest, NoSpace) {
//     Options options = CurrentOptions();
//     options.env = env_;
//     Reopen(&options);

//     ASSERT_OK(Put("foo", "v1"));
//     ASSERT_EQ("v1", Get("foo"));
//     Compact("a", "z");
//     const int num_files = CountFiles();
//     env_->no_space_.Release_Store(env_); // Force out-of-space errors
//     for (int i = 0; i < 10; i++) {
//         for (int level = 0; level < config::kNumLevels - 1; level++) {
//             dbfull()->TEST_CompactRange(level, NULL, NULL);
//         }
//     }
//     env_->no_space_.Release_Store(NULL);
//     ASSERT_LT(CountFiles(), num_files + 3);
// }

// TEST(DBTest, NonWritableFileSystem) {
//     // dynamic_cast<DBImpl*>(db_)->PrintFiles();
//     Options options = CurrentOptions();
//     options.write_buffer_size = 1000;
//     options.env = env_;
//     Reopen(&options);
//     ASSERT_OK(Put("foo", "v1"));

//     env_->non_writable_.Release_Store(env_); // Force errors for new files
//     std::string big(100000, 'x');
//     int errors = 0;
//     for (int i = 0; i < 20; i++) {
//         fprintf(stderr, "iter %d; errors %d\n", i, errors);
//         //    if (!Put("foo", big).ok()) {
//         if (!Put(big, big).ok()) { //对于kv分离，大k才会产生新sst文件
//             errors++;
//             DelayMilliseconds(100);
//         }
//     }
//     ASSERT_GT(errors, 0);
//     env_->non_writable_.Release_Store(NULL);
// }

// TEST(DBTest, WriteSyncError) {
//     // Check that log sync errors cause the DB to disallow future writes.

//     // (a) Cause log sync calls to fail
//     Options options = CurrentOptions();
//     options.env = env_;
//     Reopen(&options);
//     env_->data_sync_error_.Release_Store(env_);

//     // (b) Normal write should succeed
//     WriteOptions w;
//     ASSERT_OK(db_->Put(w, "k1", "v1"));
//     ASSERT_EQ("v1", Get("k1"));

//     // (c) Do a sync write; should fail
//     w.sync = true;
//     ASSERT_TRUE(!db_->Put(w, "k2", "v2").ok());
//     ASSERT_EQ("v1", Get("k1"));
//     ASSERT_EQ("NOT_FOUND", Get("k2"));

//     // (d) make sync behave normally
//     env_->data_sync_error_.Release_Store(NULL);

//     // (e) Do a non-sync write; should fail
//     w.sync = false;
//     ASSERT_TRUE(!db_->Put(w, "k3", "v3").ok());
//     ASSERT_EQ("v1", Get("k1"));
//     ASSERT_EQ("NOT_FOUND", Get("k2"));
//     ASSERT_EQ("NOT_FOUND", Get("k3"));
// }

// TEST(DBTest, ManifestWriteError) {
//     // Test for the following problem:
//     // (a) Compaction produces file F
//     // (b) Log record containing F is written to MANIFEST file, but Sync() fails
//     // (c) GC deletes F
//     // (d) After reopening DB, reads fail since deleted F is named in log record

//     // We iterate twice.  In the second iteration, everything is the
//     // same except the log record never makes it to the MANIFEST file.
//     for (int iter = 0; iter < 2; iter++) {
//         port::AtomicPointer* error_type = (iter == 0) ? &env_->manifest_sync_error_ : &env_->manifest_write_error_;

//         // Insert foo=>bar mapping
//         Options options = CurrentOptions();
//         options.env = env_;
//         options.create_if_missing = true;
//         options.error_if_exists = false;
//         DestroyAndReopen(&options);
//         ASSERT_OK(Put("foo", "bar"));
//         ASSERT_EQ("bar", Get("foo"));

//         // Memtable compaction (will succeed)
//         dbfull()->TEST_CompactMemTable();
//         ASSERT_EQ("bar", Get("foo"));
//         const int last = config::kMaxMemCompactLevel;
//         ASSERT_EQ(NumTableFilesAtLevel(last), 1); // foo=>bar is now in last level

//         // Merging compaction (will fail)
//         error_type->Release_Store(env_);
//         dbfull()->TEST_CompactRange(last, NULL, NULL); // Should fail
//         ASSERT_EQ("bar", Get("foo"));

//         // Recovery: should not lose data
//         error_type->Release_Store(NULL);
//         Reopen(&options);
//         ASSERT_EQ("bar", Get("foo"));
//     }
// }

// TEST(DBTest, MissingSSTFile) {
//     ASSERT_OK(Put("foo", "bar"));
//     ASSERT_EQ("bar", Get("foo"));

//     // Dump the memtable to disk.
//     dbfull()->TEST_CompactMemTable();
//     ASSERT_EQ("bar", Get("foo"));

//     Close();
//     ASSERT_TRUE(DeleteAnSSTFile());
//     Options options = CurrentOptions();
//     options.paranoid_checks = true;
//     Status s = TryReopen(&options);
//     ASSERT_TRUE(!s.ok());
//     ASSERT_TRUE(s.ToString().find("issing") != std::string::npos) << s.ToString();
// }

// TEST(DBTest, StillReadSST) {
//     ASSERT_OK(Put("foo", "bar"));
//     ASSERT_EQ("bar", Get("foo"));

//     // Dump the memtable to disk.
//     dbfull()->TEST_CompactMemTable();
//     ASSERT_EQ("bar", Get("foo"));
//     Close();
//     ASSERT_GT(RenameLDBToSST(), 0);
//     Options options = CurrentOptions();
//     options.paranoid_checks = true;
//     Status s = TryReopen(&options);
//     ASSERT_TRUE(s.ok());
//     ASSERT_EQ("bar", Get("foo"));
// }

// TEST(DBTest, FilesDeletedAfterCompaction) {
//     ASSERT_OK(Put("foo", "v2"));
//     Compact("a", "z");
//     const int num_files = CountFiles();
//     for (int i = 0; i < 10; i++) {
//         ASSERT_OK(Put("foo", "v2"));
//         Compact("a", "z");
//     }
//     ASSERT_EQ(CountFiles(), num_files);
// }

// TEST(DBTest, BloomFilter) {
//     env_->count_random_reads_ = true;
//     Options options = CurrentOptions();
//     options.env = env_;
//     options.block_cache = NewLRUCache(0); // Prevent cache hits
//     options.filter_policy = NewBloomFilterPolicy(10);
//     Reopen(&options);

//     // Populate multiple layers
//     const int N = 10000;
//     for (int i = 0; i < N; i++) {
//         ASSERT_OK(Put(Key(i), Key(i)));
//     }
//     Compact("a", "z");
//     for (int i = 0; i < N; i += 100) {
//         ASSERT_OK(Put(Key(i), Key(i)));
//     }
//     dbfull()->TEST_CompactMemTable();
//     dynamic_cast<DBImpl*>(db_)->PrintFiles();

//     // Prevent auto compactions triggered by seeks
//     env_->delay_data_sync_.Release_Store(env_);

//     // Lookup present keys.  Should rarely read from small sstable.
//     env_->random_read_counter_.Reset();
//     for (int i = 0; i < N; i++) {
//         ASSERT_EQ(Key(i), Get(Key(i)));
//     }
//     int reads = env_->random_read_counter_.Read();
//     fprintf(stderr, "%d present => %d reads\n", N, reads);
//     ASSERT_GE(reads, N);
//     ASSERT_LE(reads, N + 2 * N / 100);

//     // Lookup present keys.  Should rarely read from either sstable.
//     env_->random_read_counter_.Reset();
//     for (int i = 0; i < N; i++) {
//         ASSERT_EQ("NOT_FOUND", Get(Key(i) + ".missing"));
//     }
//     reads = env_->random_read_counter_.Read();
//     fprintf(stderr, "%d missing => %d reads\n", N, reads);
//     ASSERT_LE(reads, 3 * N / 100);

//     env_->delay_data_sync_.Release_Store(NULL);
//     Close();
//     delete options.block_cache;
//     delete options.filter_policy;
// }

// Multi-threaded test:
namespace {

static const int kTestSeconds = 90;
static const int kNumThreads = 1;
static const int kNumRange = 1000000;
static const int kNumKeys = kNumRange * kNumThreads;
static const int OPcount = kNumRange * 10;
static const int ScanOPcount = kNumRange * 5;
static const int ScanInterval = 10;
static const int KNumScanThreads = 1;
static std::vector<std::vector<double>> thread_latency(kNumThreads, std::vector<double>());
static std::vector<std::vector<std::vector<double>>> hot_range_latency(
    kNumThreads, std::vector<std::vector<double>>(5, std::vector<double>()));
static std::vector<std::unordered_map<int, int>> key_seq(kNumThreads);
static std::vector<std::vector<clock_t>> thread_time(kNumThreads, std::vector<clock_t>());
static std::vector<std::vector<int>> range_block(kNumThreads, std::vector<int>());
static std::vector<std::vector<int>> range_count(kNumThreads, std::vector<int>());
static std::vector<std::vector<clock_t>> range_min_time(kNumThreads, std::vector<clock_t>());
static std::vector<std::vector<clock_t>> range_time(kNumThreads, std::vector<clock_t>());
static std::vector<std::vector<clock_t>> range_max_time(kNumThreads, std::vector<clock_t>());

static std::vector<double> read_latency(kNumThreads);
static std::vector<double> write_latency(kNumThreads);
static std::vector<double> scan_latency(kNumThreads);
static std::vector<int> read_ops(kNumThreads);
static std::vector<int> write_ops(kNumThreads);
static std::vector<int> scan_ops(kNumThreads);

struct MTState {
    DBTest* test;
    port::AtomicPointer stop;
    int counter[kNumThreads];
    port::AtomicPointer thread_done[kNumThreads];
};

struct MTThread {
    MTState* state;
    int id;
};

static void MTThreadBody(void* arg) {
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(1000 + id);
    std::string value;
    char valbuf[1500];

    int counter = 0;
    while (t->state->stop.Acquire_Load() == NULL) {
        int key = rnd.Uniform(kNumKeys);
        char keybuf[20];
        snprintf(keybuf, sizeof(keybuf), "%016d", key);

        if (rnd.OneIn(2)) {
            // Write values of the form <key, my id, counter>.
            // We add some padding for force compactions.
            snprintf(valbuf, sizeof(valbuf), "%d.%d.%-1000d", key, id, counter);
            ASSERT_OK(db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf)));
        } else {
            // Read a value and verify that it matches the pattern written above.
            Status s = db->Get(ReadOptions(), Slice(keybuf), &value);
            if (s.IsNotFound()) {
                // Key has not yet been written
            } else {
                ASSERT_OK(s);
                int k, w, c;
                ASSERT_EQ(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;
                ASSERT_EQ(k, key);
                ASSERT_GE(w, 0);
                ASSERT_LT(w, kNumThreads);
                ASSERT_LE(c, counter);
            }
        }
        counter++;
    }
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MTPutUnfixedThreadBody(void* arg) {
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(1000 + id);
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size];
    int start_key = t->id * kNumRange;

    int counter = 0;
    while (t->state->stop.Acquire_Load() == NULL) {
        int key = start_key + rnd.Uniform(kNumRange);

        std::string key_in_format = "%0" + std::to_string(rnd.Uniform(test_key_size - 1) + 1) + "d";
        std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 10) + "d";
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        ASSERT_OK(db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf)));
        counter++;
    }
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void FullGCThreadBody(void* arg) {
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    int start_key = t->id * kNumRange;
    int key = 0;
    std::string value;

    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    Random rnd(100 + id);
    Status status;

    int counter = 0;
    clock_t start_t, end_t;
    start_t = clock();
    while (counter < OPcount) {
        key = start_key + rnd.Uniform(kNumRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
    }
    end_t = clock();

    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void ReorderThreadBody_1(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int keyRange = 100 * 1000 * 1000;
    int initialCount = 100 * 1000 * 1000;
    int keyCount = 1 * 1000 * 1000;
    int scanCount = keyCount;
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    size_t zipf_count = t->state->test->zipf_map_.size();
    std::vector<int> zipf_vec(zipf_count);
    std::tr1::unordered_set<int> zipf_set;
    for (size_t i = 0; i < zipf_count; i++) {
        key = rnd.Uniform(keyRange - scan_len);
        while (zipf_set.count(key) > 0) {
            key = rnd.Uniform(keyRange - scan_len);
        }
        zipf_vec[i] = key;
    }

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();
    range_block[id].resize(zipf_count);
    range_count[id].resize(zipf_count);
    range_max_time[id].resize(zipf_count);
    range_min_time[id].resize(zipf_count);
    range_time[id].resize(zipf_count);
    for (size_t i = 0; i < zipf_count; i++) {
        range_block[id][i] = scan_len;
        range_min_time[id][i] = 10000;
        range_max_time[id][i] = 0;
        range_count[id][i] = 0;
        range_time[id][i] = 0;
    }

    std::vector<double> thrpt_time(5, 0);
    std::vector<int> thrpt_ops(5, 0);
    for (int i = 0; i < 5; i++) {
        hot_range_latency[id][i].clear();
    }

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        counter++;
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        key = rnd.Uniform(keyRange);

        size_t idx = t->state->test->zipf_map_.lower_bound(rnd.Uniform(scanCount))->second;
        key = start_key + zipf_vec[idx];
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(tmp_keybuf, sizeof(tmp_keybuf), key_in_format.c_str(), key + scan_len);

        int tmp_count = 0;
        start_t = clock();
        status = db->Scan(ReadOptions(), Slice(keybuf), Slice(tmp_keybuf), tmp_key, tmp_value, &tmp_count);
        if (!status.ok()) {
            std::cout << "error scan : thread=" << id << " counter=" << counter << " status=" << status.ToString()
                      << "\n";
            break;
        }
        end_t = clock();
        if (end_t - start_t < time_bound) {
            range_block[id][idx] = std::min(range_block[id][idx], tmp_count);
            range_count[id][idx]++;
            range_min_time[id][idx] = std::min(range_min_time[id][idx], end_t - start_t);
            range_max_time[id][idx] = std::max(range_max_time[id][idx], end_t - start_t);
            range_time[id][idx] += end_t - start_t;
            if (idx < 5) {
                thrpt_time[idx] += end_t - start_t;
                thrpt_ops[idx]++;
            }
        } else {
            ops++;
            sub_time += end_t - start_t;
        }

        bool scan_error = false;
        for (size_t i = 0; i < !scan_error && tmp_value.size(); i++) {
            int k, w, c;
            ASSERT_EQ(3, sscanf(tmp_value[i].c_str(), "%d.%d.%d", &k, &w, &c)) << tmp_value[i];
            if (key_seq[id].count(k) > 0) {
                if (w != id || c != key_seq[id][k]) {
                    std::cout << "error scan : thread=" << id << " counter=" << counter << " not equal! " << w << " "
                              << id << " " << c << " " << key_seq[id][k] << "\n";
                    scan_error = true;
                }
            } else {
                std::cout << "error scan : thread=" << id << " counter=" << counter << " not found\n";
                scan_error = true;
            }
        }
        if (scan_error) {
            break;
        }

        tmp_value.clear();
        tmp_key.clear();

        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            sub_time = 0;
            ops = 0;
            for (int i = 0; i < 5; i++) {
                hot_range_latency[id][i].push_back((thrpt_ops[i] ? thrpt_time[i] / thrpt_ops[i] : 0));
                thrpt_time[i] = 0;
                thrpt_ops[i] = 0;
            }
            total_start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void ReorderThreadBody_2(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int read_type = 7, write_type = 7, scan_type = 10;
    int keyRange = 100 * 1000 * 1000;
    int initialCount = keyRange * 1;
    int keyCount = 10 * 1000 * 1000;
    int scanCount = keyCount / scan_type * (scan_type - read_type);
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    size_t zipf_count = t->state->test->zipf_map_.size();
    std::vector<int> zipf_vec(zipf_count);
    std::tr1::unordered_set<int> zipf_set;
    for (size_t i = 0; i < zipf_count; i++) {
        key = rnd.Uniform(keyRange - scan_len);
        while (zipf_set.count(key) > 0) {
            key = rnd.Uniform(keyRange - scan_len);
        }
        zipf_vec[i] = key;
    }

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();
    range_block[id].resize(zipf_count);
    range_count[id].resize(zipf_count);
    range_max_time[id].resize(zipf_count);
    range_min_time[id].resize(zipf_count);
    range_time[id].resize(zipf_count);
    for (size_t i = 0; i < zipf_count; i++) {
        range_block[id][i] = scan_len;
        range_min_time[id][i] = 10000;
        range_max_time[id][i] = 0;
        range_count[id][i] = 0;
        range_time[id][i] = 0;
    }

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        counter++;
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        uint32_t op_type = rnd.Uniform(scan_type);
        key = rnd.Uniform(keyRange);

        if (op_type < write_type) {
            key += start_key;
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
            start_t = clock();
            status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
            end_t = clock();
            write_latency[id] += end_t - start_t;
            write_ops[id]++;
            if (!status.ok()) {
                std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
                break;
            }
            key_seq[id][key] = counter;
        } // write operator
        else {
            size_t idx = t->state->test->zipf_map_.lower_bound(rnd.Uniform(scanCount))->second;
            key = start_key + zipf_vec[idx];
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(tmp_keybuf, sizeof(tmp_keybuf), key_in_format.c_str(), key + scan_len);

            int tmp_count = 0;
            start_t = clock();
            status = db->Scan(ReadOptions(), Slice(keybuf), Slice(tmp_keybuf), tmp_key, tmp_value, &tmp_count);
            if (!status.ok()) {
                std::cout << "error scan : thread=" << id << " counter=" << counter << " status=" << status.ToString()
                          << "\n";
                break;
            }
            end_t = clock();
            if (end_t - start_t < time_bound) {
                range_block[id][idx] = std::min(range_block[id][idx], tmp_count);
                range_count[id][idx]++;
                range_min_time[id][idx] = std::min(range_min_time[id][idx], end_t - start_t);
                range_max_time[id][idx] = std::max(range_max_time[id][idx], end_t - start_t);
                range_time[id][idx] += end_t - start_t;
                scan_latency[id] += end_t - start_t;
                scan_ops[id]++;
            } else {
                ops++;
                sub_time += end_t - start_t;
            }

            bool scan_error = false;
            for (size_t i = 0; i < !scan_error && tmp_value.size(); i++) {
                int k, w, c;
                ASSERT_EQ(3, sscanf(tmp_value[i].c_str(), "%d.%d.%d", &k, &w, &c)) << tmp_value[i];
                if (key_seq[id].count(k) > 0) {
                    if (w != id || c != key_seq[id][k]) {
                        std::cout << "error scan : thread=" << id << " counter=" << counter << " not equal! " << w
                                  << " " << id << " " << c << " " << key_seq[id][k] << "\n";
                        scan_error = true;
                    }
                } else {
                    std::cout << "error scan : thread=" << id << " counter=" << counter << " not found\n";
                    scan_error = true;
                }
            }
            if (scan_error) {
                break;
            }

            tmp_value.clear();
            tmp_key.clear();
        }

        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            ops = 0;
            sub_time = 0;
            total_start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void ReorderThreadBody_3(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int read_type = 5, write_type = 5, scan_type = 10;
    int keyRange = 100 * 1000 * 1000;
    int initialCount = keyRange * 1;
    int keyCount = 10 * 1000 * 1000;
    int scanCount = keyCount / scan_type * (scan_type - read_type);
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    size_t zipf_count = t->state->test->zipf_map_.size();
    std::vector<int> zipf_vec(zipf_count);
    std::tr1::unordered_set<int> zipf_set;
    for (size_t i = 0; i < zipf_count; i++) {
        key = rnd.Uniform(keyRange - scan_len);
        while (zipf_set.count(key) > 0) {
            key = rnd.Uniform(keyRange - scan_len);
        }
        zipf_vec[i] = key;
    }

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();
    range_block[id].resize(zipf_count);
    range_count[id].resize(zipf_count);
    range_max_time[id].resize(zipf_count);
    range_min_time[id].resize(zipf_count);
    range_time[id].resize(zipf_count);
    for (size_t i = 0; i < zipf_count; i++) {
        range_block[id][i] = scan_len;
        range_min_time[id][i] = 10000;
        range_max_time[id][i] = 0;
        range_count[id][i] = 0;
        range_time[id][i] = 0;
    }

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        counter++;
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        uint32_t op_type = rnd.Uniform(scan_type);
        key = rnd.Uniform(keyRange);

        if (op_type < write_type) {
            key += start_key;
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
            start_t = clock();
            status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
            end_t = clock();
            write_latency[id] += end_t - start_t;
            write_ops[id]++;
            if (!status.ok()) {
                std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
                break;
            }
            key_seq[id][key] = counter;
        } // write operator
        else {
            size_t idx = t->state->test->zipf_map_.lower_bound(rnd.Uniform(scanCount))->second;
            key = start_key + zipf_vec[idx];
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(tmp_keybuf, sizeof(tmp_keybuf), key_in_format.c_str(), key + scan_len);

            int tmp_count = 0;
            start_t = clock();
            status = db->Scan(ReadOptions(), Slice(keybuf), Slice(tmp_keybuf), tmp_key, tmp_value, &tmp_count);
            if (!status.ok()) {
                std::cout << "error scan : thread=" << id << " counter=" << counter << " status=" << status.ToString()
                          << "\n";
                break;
            }
            end_t = clock();
            if (end_t - start_t < time_bound) {
                range_block[id][idx] = std::min(range_block[id][idx], tmp_count);
                range_count[id][idx]++;
                range_min_time[id][idx] = std::min(range_min_time[id][idx], end_t - start_t);
                range_max_time[id][idx] = std::max(range_max_time[id][idx], end_t - start_t);
                range_time[id][idx] += end_t - start_t;
                scan_latency[id] += end_t - start_t;
                scan_ops[id]++;
            } else {
                ops++;
                sub_time += end_t - start_t;
            }

            bool scan_error = false;
            for (size_t i = 0; i < !scan_error && tmp_value.size(); i++) {
                int k, w, c;
                ASSERT_EQ(3, sscanf(tmp_value[i].c_str(), "%d.%d.%d", &k, &w, &c)) << tmp_value[i];
                if (key_seq[id].count(k) > 0) {
                    if (w != id || c != key_seq[id][k]) {
                        std::cout << "error scan : thread=" << id << " counter=" << counter << " not equal! " << w
                                  << " " << id << " " << c << " " << key_seq[id][k] << "\n";
                        scan_error = true;
                    }
                } else {
                    std::cout << "error scan : thread=" << id << " counter=" << counter << " not found\n";
                    scan_error = true;
                }
            }
            if (scan_error) {
                break;
            }

            tmp_value.clear();
            tmp_key.clear();
        }

        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            ops = 0;
            sub_time = 0;
            total_start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void GCThreadBody_1(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int keyRange = 1 * 1000 * 1000;
    int initialCount = keyRange * 3;
    int keyCount = keyRange * 10;
    int counter_interval = 10 * 1000;
    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size];
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
        if (counter % (counter_interval * 100) == 0) {
            end_t = clock();
            thread_time[id].push_back(end_t - start_t);
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
            start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MWThreadBody_1(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int keyRange = 100 * 1000 * 1000;
    int keyCount = 10 * 1000 * 1000;
    int counter_interval = 10 * 1000;

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t start_t, end_t;
    start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount) {
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
        if (counter % (counter_interval * 100) == 0) {
            end_t = clock();
            thread_time[id].push_back(end_t - start_t);
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
            start_t = clock();
        }
    }
    if (counter != keyCount) t->state->stop.Release_Store(t);

    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MWThreadBody_2(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int keyRange = 1 * 1000 * 1000;
    int initialCount = keyRange * 3;
    int keyCount = keyRange * 10;
    int counter_interval = 10 * 1000;
    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size];
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
        if (counter % (counter_interval * 100) == 0) {
            end_t = clock();
            thread_time[id].push_back(end_t - start_t);
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
            start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MWThreadBody_3(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int keyRange = 100 * 1000 * 1000;
    int keyCount = 10 * 1000 * 1000;
    int initialCount = 100 * 1000 * 1000;
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        counter++;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        key = rnd.Uniform(keyRange);

        key += start_key;
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        std::vector<uint64_t> visited_file;
        start_t = clock();
        Status s = db->Get(ReadOptions(), Slice(keybuf), &value, &visited_file);
        end_t = clock();
        if (s.IsNotFound()) {
            if (key_seq[id].count(key) > 0) {
                std::cout << "error read : thread=" << id << " counter=" << counter << " not found\n";
                std::cout << "visit_file = ";
                for (int i = 0; i < visited_file.size(); i++) {
                    std::cout << visited_file[i] << " ";
                }
                std::cout << "\n";
                dynamic_cast<DBImpl*>(db)->PrintFiles();
                break;
            }
        } else {
            int k, w, c;
            ASSERT_EQ(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;
            if (k != key || w != id || c != key_seq[id][key]) {
                std::cout << "error read : thread=" << id << " counter=" << counter << " not equal! " << k << " " << key
                          << " " << w << " " << id << " " << c << " " << key_seq[id][key] << "\n";
                std::cout << "visit_file = ";
                for (int i = 0; i < visited_file.size(); i++) {
                    std::cout << visited_file[i] << " ";
                }
                std::cout << "\n";
                dynamic_cast<DBImpl*>(db)->PrintFiles();
                break;
            }
        }
        if (end_t - start_t >= time_bound) {
            ops++;
            sub_time += end_t - start_t;
        }

        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            ops = 0;
            sub_time = 0;
            total_start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MWThreadBody_4(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int read_type = 9, write_type = 8, scan_type = 10;
    int keyRange = 100 * 1000 * 1000;
    int initialCount = keyRange * 1;
    int keyCount = 10 * 1000 * 1000;
    int scanCount = keyCount / scan_type * (scan_type - read_type);
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    size_t zipf_count = t->state->test->zipf_map_.size();
    std::vector<int> zipf_vec(zipf_count);
    std::tr1::unordered_set<int> zipf_set;
    for (size_t i = 0; i < zipf_count; i++) {
        key = rnd.Uniform(keyRange - scan_len);
        while (zipf_set.count(key) > 0) {
            key = rnd.Uniform(keyRange - scan_len);
        }
        zipf_vec[i] = key;
    }

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();
    range_block[id].resize(zipf_count);
    range_count[id].resize(zipf_count);
    range_max_time[id].resize(zipf_count);
    range_min_time[id].resize(zipf_count);
    range_time[id].resize(zipf_count);
    for (size_t i = 0; i < zipf_count; i++) {
        range_block[id][i] = scan_len;
        range_min_time[id][i] = 10000;
        range_max_time[id][i] = 0;
        range_count[id][i] = 0;
        range_time[id][i] = 0;
    }

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        counter++;
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        uint32_t op_type = rnd.Uniform(scan_type);
        key = rnd.Uniform(keyRange);

        if (op_type < write_type) {
            key += start_key;
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
            start_t = clock();
            status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
            end_t = clock();
            write_latency[id] += end_t - start_t;
            write_ops[id]++;
            if (!status.ok()) {
                std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
                break;
            }
            key_seq[id][key] = counter;
        } // write operator
        else if (op_type < read_type) {
            key += start_key;
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            std::vector<uint64_t> visited_file;
            start_t = clock();
            Status s = db->Get(ReadOptions(), Slice(keybuf), &value, &visited_file);
            end_t = clock();
            read_latency[id] += end_t - start_t;
            read_ops[id]++;

            if (s.IsNotFound()) {
                if (key_seq[id].count(key) > 0) {
                    std::cout << "error read : thread=" << id << " counter=" << counter << " not found\n";
                    std::cout << "visit_file = ";
                    for (int i = 0; i < visited_file.size(); i++) {
                        std::cout << visited_file[i] << " ";
                    }
                    std::cout << "\n";
                    dynamic_cast<DBImpl*>(db)->PrintFiles();
                    break;
                }
            } else {
                int k, w, c;
                ASSERT_EQ(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;
                if (k != key || w != id || c != key_seq[id][key]) {
                    std::cout << "error read : thread=" << id << " counter=" << counter << " not equal! " << k << " "
                              << key << " " << w << " " << id << " " << c << " " << key_seq[id][key] << "\n";
                    std::cout << "visit_file = ";
                    for (int i = 0; i < visited_file.size(); i++) {
                        std::cout << visited_file[i] << " ";
                    }
                    std::cout << "\n";
                    dynamic_cast<DBImpl*>(db)->PrintFiles();
                    break;
                }
            }
        } // read operator
        else {
            size_t idx = t->state->test->zipf_map_.lower_bound(rnd.Uniform(scanCount))->second;
            key = start_key + zipf_vec[idx];
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(tmp_keybuf, sizeof(tmp_keybuf), key_in_format.c_str(), key + scan_len);

            int tmp_count = 0;
            start_t = clock();
            status = db->Scan(ReadOptions(), Slice(keybuf), Slice(tmp_keybuf), tmp_key, tmp_value, &tmp_count);
            if (!status.ok()) {
                std::cout << "error scan : thread=" << id << " counter=" << counter << " status=" << status.ToString()
                          << "\n";
                break;
            }
            end_t = clock();
            if (end_t - start_t < time_bound) {
                range_block[id][idx] = std::min(range_block[id][idx], tmp_count);
                range_count[id][idx]++;
                range_min_time[id][idx] = std::min(range_min_time[id][idx], end_t - start_t);
                range_max_time[id][idx] = std::max(range_max_time[id][idx], end_t - start_t);
                range_time[id][idx] += end_t - start_t;
                scan_latency[id] += end_t - start_t;
                scan_ops[id]++;
            } else {
                ops++;
                sub_time += end_t - start_t;
            }

            bool scan_error = false;
            for (size_t i = 0; i < !scan_error && tmp_value.size(); i++) {
                int k, w, c;
                ASSERT_EQ(3, sscanf(tmp_value[i].c_str(), "%d.%d.%d", &k, &w, &c)) << tmp_value[i];
                if (key_seq[id].count(k) > 0) {
                    if (w != id || c != key_seq[id][k]) {
                        std::cout << "error scan : thread=" << id << " counter=" << counter << " not equal! " << w
                                  << " " << id << " " << c << " " << key_seq[id][k] << "\n";
                        scan_error = true;
                    }
                } else {
                    std::cout << "error scan : thread=" << id << " counter=" << counter << " not found\n";
                    scan_error = true;
                }
            }
            if (scan_error) {
                break;
            }

            tmp_value.clear();
            tmp_key.clear();
        } // scan operator

        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            ops = 0;
            sub_time = 0;
            total_start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MWThreadBody_5(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int read_type = 7, write_type = 7, scan_type = 10;
    int keyRange = 100 * 1000 * 1000;
    int initialCount = 100 * 1000 * 1000;
    int keyCount = 10 * 1000 * 1000;
    int scanCount = keyCount / scan_type * (scan_type - read_type);
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    size_t zipf_count = t->state->test->zipf_map_.size();
    std::vector<int> zipf_vec(zipf_count);
    std::tr1::unordered_set<int> zipf_set;
    for (size_t i = 0; i < zipf_count; i++) {
        key = rnd.Uniform(keyRange - scan_len);
        while (zipf_set.count(key) > 0) {
            key = rnd.Uniform(keyRange - scan_len);
        }
        zipf_vec[i] = key;
    }

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();

    range_block[id].resize(zipf_count);
    range_count[id].resize(zipf_count);
    range_max_time[id].resize(zipf_count);
    range_min_time[id].resize(zipf_count);
    range_time[id].resize(zipf_count);
    for (size_t i = 0; i < zipf_count; i++) {
        range_block[id][i] = scan_len;
        range_min_time[id][i] = 10000;
        range_max_time[id][i] = 0;
        range_count[id][i] = 0;
        range_time[id][i] = 0;
    }

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        counter++;
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        // std::cout << "counter =" << counter << "\n";
        uint32_t op_type = rnd.Uniform(scan_type);
        key = rnd.Uniform(keyRange);

        if (op_type < write_type) {
            key += start_key;
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
            start_t = clock();
            status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
            end_t = clock();
            write_latency[id] += end_t - start_t;
            write_ops[id]++;
            if (!status.ok()) {
                std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
                break;
            }
            key_seq[id][key] = counter;
        } // write operator
        else {
            size_t idx = t->state->test->zipf_map_.lower_bound(rnd.Uniform(scanCount))->second;
            key = start_key + zipf_vec[idx];
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(tmp_keybuf, sizeof(tmp_keybuf), key_in_format.c_str(), key + scan_len);

            int tmp_count = 0;
            start_t = clock();
            status = db->Scan(ReadOptions(), Slice(keybuf), Slice(tmp_keybuf), tmp_key, tmp_value, &tmp_count);
            if (!status.ok()) {
                std::cout << "error scan : thread=" << id << " counter=" << counter << " status=" << status.ToString()
                          << "\n";
                break;
            }
            end_t = clock();
            if (end_t - start_t < time_bound) {
                range_block[id][idx] = std::min(range_block[id][idx], tmp_count);
                range_count[id][idx]++;
                range_min_time[id][idx] = std::min(range_min_time[id][idx], end_t - start_t);
                range_max_time[id][idx] = std::max(range_max_time[id][idx], end_t - start_t);
                range_time[id][idx] += end_t - start_t;
                scan_latency[id] += end_t - start_t;
                scan_ops[id]++;
            } else {
                ops++;
                sub_time += end_t - start_t;
            }

            bool scan_error = false;
            for (size_t i = 0; i < !scan_error && tmp_value.size(); i++) {
                int k, w, c;
                ASSERT_EQ(3, sscanf(tmp_value[i].c_str(), "%d.%d.%d", &k, &w, &c)) << tmp_value[i];
                if (key_seq[id].count(k) > 0) {
                    if (w != id || c != key_seq[id][k]) {
                        std::cout << "error scan : thread=" << id << " counter=" << counter << " not equal! " << w
                                  << " " << id << " " << c << " " << key_seq[id][k] << "\n";
                        scan_error = true;
                    }
                } else {
                    std::cout << "error scan : thread=" << id << " counter=" << counter << " not found\n";
                    scan_error = true;
                }
            }
            if (scan_error) {
                break;
            }

            tmp_value.clear();
            tmp_key.clear();
        } // scan operator

        // std::cout << "counter =" << counter << "\n";
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            ops = 0;
            sub_time = 0;
            total_start_t = clock();
        }
        // std::cout << "counter =" << counter << "\n";
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

static void MWThreadBody_6(void* arg) {
    Status status;
    MTThread* t = reinterpret_cast<MTThread*>(arg);
    int id = t->id;
    DB* db = t->state->test->db_;
    fprintf(stderr, "... starting thread %d\n", id);
    Random rnd(100 + id);

    int read_type = 5, write_type = 5, scan_type = 10;
    int keyRange = 100 * 1000 * 1000;
    int initialCount = keyRange * 1;
    int keyCount = 10 * 1000 * 1000;
    int scanCount = keyCount / scan_type * (scan_type - read_type);
    int counter_interval = 10 * 1000;

    int key, start_key = t->id * keyRange;
    std::string value;
    char keybuf[test_key_size], valbuf[test_value_size], tmp_keybuf[test_key_size];
    std::vector<std::string> tmp_value, tmp_key;
    std::string key_in_format = "%0" + std::to_string(test_key_size - 1) + "d";
    std::string value_in_format = "%d.%d.%-" + std::to_string(test_value_size - 15) + "d";

    size_t zipf_count = t->state->test->zipf_map_.size();
    std::vector<int> zipf_vec(zipf_count);
    std::tr1::unordered_set<int> zipf_set;
    for (size_t i = 0; i < zipf_count; i++) {
        key = rnd.Uniform(keyRange - scan_len);
        while (zipf_set.count(key) > 0) {
            key = rnd.Uniform(keyRange - scan_len);
        }
        zipf_vec[i] = key;
    }

    key_seq[id].clear();
    thread_latency[id].clear();
    thread_time[id].clear();
    range_block[id].resize(zipf_count);
    range_count[id].resize(zipf_count);
    range_max_time[id].resize(zipf_count);
    range_min_time[id].resize(zipf_count);
    range_time[id].resize(zipf_count);
    for (size_t i = 0; i < zipf_count; i++) {
        range_block[id][i] = scan_len;
        range_min_time[id][i] = 10000;
        range_max_time[id][i] = 0;
        range_count[id][i] = 0;
        range_time[id][i] = 0;
    }

    read_ops[id] = 0;
    write_ops[id] = 0;
    scan_ops[id] = 0;
    read_latency[id] = 0;
    write_latency[id] = 0;
    scan_latency[id] = 0;

    int counter = 0;
    clock_t time_bound = 1000;
    int ops = 0;
    clock_t sub_time = 0;

    while (t->state->stop.Acquire_Load() == NULL && counter < initialCount) {
        counter++;
        key = start_key + rnd.Uniform(keyRange);
        snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
        snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
        status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
        if (!status.ok()) {
            std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
            break;
        }
        key_seq[id][key] = counter;
        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << "\n";
        }
    }
    if (counter != initialCount) t->state->stop.Release_Store(t);

    clock_t start_t, end_t;
    clock_t total_start_t, total_end_t;
    total_start_t = clock();
    while (t->state->stop.Acquire_Load() == NULL && counter < keyCount + initialCount) {
        counter++;
        uint32_t op_type = rnd.Uniform(scan_type);
        key = rnd.Uniform(keyRange);

        if (op_type < write_type) {
            key += start_key;
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(valbuf, sizeof(valbuf), value_in_format.c_str(), key, id, counter);
            start_t = clock();
            status = db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf));
            end_t = clock();
            write_latency[id] += end_t - start_t;
            write_ops[id]++;
            if (!status.ok()) {
                std::cout << "error write : thread=" << id << " counter=" << counter << " put error\n";
                break;
            }
            key_seq[id][key] = counter;
        } // write operator
        else {
            size_t idx = t->state->test->zipf_map_.lower_bound(rnd.Uniform(scanCount))->second;
            key = start_key + zipf_vec[idx];
            snprintf(keybuf, sizeof(keybuf), key_in_format.c_str(), key);
            snprintf(tmp_keybuf, sizeof(tmp_keybuf), key_in_format.c_str(), key + scan_len);

            int tmp_count = 0;
            start_t = clock();
            status = db->Scan(ReadOptions(), Slice(keybuf), Slice(tmp_keybuf), tmp_key, tmp_value, &tmp_count);
            if (!status.ok()) {
                std::cout << "error scan : thread=" << id << " counter=" << counter << " status=" << status.ToString()
                          << "\n";
                break;
            }
            end_t = clock();
            if (end_t - start_t < time_bound) {
                range_block[id][idx] = std::min(range_block[id][idx], tmp_count);
                range_count[id][idx]++;
                range_min_time[id][idx] = std::min(range_min_time[id][idx], end_t - start_t);
                range_max_time[id][idx] = std::max(range_max_time[id][idx], end_t - start_t);
                range_time[id][idx] += end_t - start_t;
                scan_latency[id] += end_t - start_t;
                scan_ops[id]++;
            } else {
                ops++;
                sub_time += end_t - start_t;
            }

            bool scan_error = false;
            for (size_t i = 0; i < !scan_error && tmp_value.size(); i++) {
                int k, w, c;
                ASSERT_EQ(3, sscanf(tmp_value[i].c_str(), "%d.%d.%d", &k, &w, &c)) << tmp_value[i];
                if (key_seq[id].count(k) > 0) {
                    if (w != id || c != key_seq[id][k]) {
                        std::cout << "error scan : thread=" << id << " counter=" << counter << " not equal! " << w
                                  << " " << id << " " << c << " " << key_seq[id][k] << "\n";
                        scan_error = true;
                    }
                } else {
                    std::cout << "error scan : thread=" << id << " counter=" << counter << " not found\n";
                    scan_error = true;
                }
            }
            if (scan_error) {
                break;
            }

            tmp_value.clear();
            tmp_key.clear();
        } // scan operator

        if (counter % (counter_interval * 100) == 0) {
            std::cout << "thread " << id << " finish counter=" << counter << " bound=" << time_bound << " ops=" << ops
                      << "\n";
        }
        if (counter % counter_interval == 0) {
            total_end_t = clock();
            double latency = total_end_t - total_start_t - sub_time;
            latency /= (counter_interval - ops);
            thread_latency[id].push_back(latency);
            ops = 0;
            sub_time = 0;
            total_start_t = clock();
        }
    }

    if (counter != keyCount + initialCount) t->state->stop.Release_Store(t);
    t->state->counter[id] = counter;
    t->state->thread_done[id].Release_Store(t);
}

} // namespace

TEST(DBTest, MultiThreaded) {
    do {
        // Initialize state
        MTState mt;
        mt.test = this;
        mt.stop.Release_Store(0);
        for (int id = 0; id < kNumThreads; id++) {
            mt.thread_done[id].Release_Store(0);
        }

        // Start threads
        MTThread thread[kNumThreads];
        for (int id = 0; id < kNumThreads; id++) {
            thread[id].state = &mt;
            thread[id].id = id;
            env_->StartThread(MTThreadBody, &thread[id]);
        }

        // Let them run for a while
        DelayMilliseconds(kTestSeconds * 1000);

        // Stop the threads and wait for them to finish
        mt.stop.Release_Store(&mt);
        for (int id = 0; id < kNumThreads; id++) {
            while (mt.thread_done[id].Acquire_Load() == NULL) {
                DelayMilliseconds(100);
            }
        }
    } while (ChangeOptions());
}

TEST(DBTest, MultiThreadedUnfixedInput) {
    // Initialize state
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    // Start threads
    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MTPutUnfixedThreadBody, &thread[id]);
    }

    // Let them run for a while
    DelayMilliseconds(kTestSeconds * 1000);

    // Stop the threads and wait for them to finish
    mt.stop.Release_Store(&mt);
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(100);
        }
    }
}

TEST(DBTest, MTReorder_1) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    mt.test->LoadZipf("zipf-1000k.txt");
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(ReorderThreadBody_1, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("reorder_1.txt");
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n\n";
        {
            fout << "total_latency-------------------------\n";
            double total_latency;
            int i, j = 0;
            while (true) {
                total_latency = 0;
                for (i = 0; i < kNumThreads; i++) {
                    if (j >= thread_latency[i].size()) {
                        break;
                    } else {
                        total_latency += thread_latency[i][j];
                    }
                }
                if (i < kNumThreads) {
                    break;
                }
                fout << total_latency << "\n";
                j++;
            }
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < 5; j++) {
                fout << "hot_range" << j << " latency-------------------------\n";
                for (int k = 0; k < hot_range_latency[i][j].size(); k++) {
                    fout << hot_range_latency[i][j][k] << "\n";
                }
            }
        }
        fout << "\n------------------------------------------------------\n";

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < range_block[i].size(); j++) {
                fout << j << " block=" << range_block[i][j] << " count=" << range_count[i][j];
                fout << " min_time=" << range_min_time[i][j] << " max_time=" << range_max_time[i][j];
                fout << " avg_time=";
                if (range_count[i][j])
                    fout << ((double)range_time[i][j] / range_count[i][j]);
                else
                    fout << "0";
                fout << "\n";
            }
            fout << "------------------------------------------------------\n";
        }
        fout.close();
    }
}

TEST(DBTest, MTReorder_2) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    mt.test->LoadZipf("zipf-3000k.txt");
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(ReorderThreadBody_2, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        double total_latency = 0;
        int total_ops = 0;
        std::ofstream fout("reorder_2.txt");

        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_latency[i].size(); j++) {
                total_latency += thread_latency[i][j];
                total_ops++;
            }
        }

        fout << " avg_latency=" << total_latency / total_ops << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " read_latency=" << (read_ops[0] ? read_latency[0] / read_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " write_latency=" << (write_ops[0] ? write_latency[0] / write_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " scan_latency=" << (scan_ops[0] ? scan_latency[0] / scan_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";
        fout << " rewrite_size=" << dbfull()->reorder_rewrite_;
        fout << "\n------------------------------------------------------\n";

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < range_block[i].size(); j++) {
                fout << j << " block=" << range_block[i][j] << " count=" << range_count[i][j];
                fout << " min_time=" << range_min_time[i][j] << " max_time=" << range_max_time[i][j];
                fout << " avg_time=";
                if (range_count[i][j])
                    fout << ((double)range_time[i][j] / range_count[i][j]);
                else
                    fout << "0";
                fout << "\n";
            }
            fout << "------------------------------------------------------\n";
        }
        fout.close();
    }
}

TEST(DBTest, MTReorder_3) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    mt.test->LoadZipf("zipf-5000k.txt");
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(ReorderThreadBody_3, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        double total_latency = 0;
        int total_ops = 0;
        std::ofstream fout("reorder_3.txt");

        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_latency[i].size(); j++) {
                total_latency += thread_latency[i][j];
                total_ops++;
            }
        }

        fout << " avg_latency=" << total_latency / total_ops << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " read_latency=" << (read_ops[0] ? read_latency[0] / read_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " write_latency=" << (write_ops[0] ? write_latency[0] / write_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " scan_latency=" << (scan_ops[0] ? scan_latency[0] / scan_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";
        fout << " rewrite_size=" << dbfull()->reorder_rewrite_;
        fout << "\n------------------------------------------------------\n";

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < range_block[i].size(); j++) {
                fout << j << " block=" << range_block[i][j] << " count=" << range_count[i][j];
                fout << " min_time=" << range_min_time[i][j] << " max_time=" << range_max_time[i][j];
                fout << " avg_time=";
                if (range_count[i][j])
                    fout << ((double)range_time[i][j] / range_count[i][j]);
                else
                    fout << "0";
                fout << "\n";
            }
            fout << "------------------------------------------------------\n";
        }
        fout.close();
    }
}

TEST(DBTest, GC_1) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(GCThreadBody_1, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        double total_time = 0;
        std::ofstream fout("gc_1.txt");
        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_time[i].size(); j++) {
                total_time += thread_time[i][j];
            }
        }
        double input_size = (test_key_size + test_value_size) * 10;
        total_time /= (1000 * 1000);
        fout << " throughput=" << input_size / total_time;
        fout << "\n------------------------------------------------------\n";
        fout << " total_time=" << total_time;
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";
        fout.close();
    }
}

TEST(DBTest, MTMW_1) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MWThreadBody_1, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("mw_1.csv", std::ios::out);
        uint64_t total_time;
        int i, j = 0;
        while (true) {
            total_time = 0;
            for (i = 0; i < kNumThreads; i++) {
                if (j >= thread_time[i].size()) {
                    break;
                } else {
                    total_time += thread_time[i][j];
                }
            }
            if (i < kNumThreads) {
                break;
            }
            fout << total_time << "\n";
            j++;
        }
        fout.close();
    }

    {
        double total_time = 0;
        std::ofstream fout("mw_1.txt");
        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_time[i].size(); j++) {
                total_time += thread_time[i][j];
            }
        }
        double input_size = (test_kv_size)*10;
        total_time /= (1000 * 1000);
        fout << " throughput=" << input_size / total_time;
        fout << "\n------------------------------------------------------\n";
        fout << " total_time=" << total_time;
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";
        fout.close();
    }
}

TEST(DBTest, MTMW_2) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MWThreadBody_2, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("mw_2.csv", std::ios::out);
        uint64_t total_time;
        int i, j = 0;
        while (true) {
            total_time = 0;
            for (i = 0; i < kNumThreads; i++) {
                if (j >= thread_time[i].size()) {
                    break;
                } else {
                    total_time += thread_time[i][j];
                }
            }
            if (i < kNumThreads) {
                break;
            }
            fout << total_time << "\n";
            j++;
        }
        fout.close();
    }

    {
        double total_time = 0;
        std::ofstream fout("mw_2.txt");
        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_time[i].size(); j++) {
                total_time += thread_time[i][j];
            }
        }
        double input_size = (test_key_size + test_value_size) * 10;
        total_time /= (1000 * 1000);
        fout << " throughput=" << input_size / total_time;
        fout << "\n------------------------------------------------------\n";
        fout << " total_time=" << total_time;
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";
        fout.close();
    }
}

TEST(DBTest, MTMW_3) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MWThreadBody_3, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("mw_3.csv", std::ios::out);
        double total_latency;
        int i, j = 0;
        while (true) {
            total_latency = 0;
            for (i = 0; i < kNumThreads; i++) {
                if (j >= thread_latency[i].size()) {
                    break;
                } else {
                    total_latency += thread_latency[i][j];
                }
            }
            if (i < kNumThreads) {
                break;
            }
            fout << total_latency << "\n";
            j++;
        }
    }

    {
        double total_latency = 0;
        int total_ops = 0;
        std::ofstream fout("mw_3.txt");

        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_latency[i].size(); j++) {
                total_latency += thread_latency[i][j];
                total_ops++;
            }
        }

        fout << " avg_latency=" << total_latency / total_ops << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";
        fout.close();
    }
}

TEST(DBTest, MTMW_4) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    mt.test->LoadZipf("zipf-1000k.txt");
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MWThreadBody_4, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("mw_4.csv", std::ios::out);
        double total_latency;
        int i, j = 0;
        while (true) {
            total_latency = 0;
            for (i = 0; i < kNumThreads; i++) {
                if (j >= thread_latency[i].size()) {
                    break;
                } else {
                    total_latency += thread_latency[i][j];
                }
            }
            if (i < kNumThreads) {
                break;
            }
            fout << total_latency << "\n";
            j++;
        }
    }

    {
        double total_latency = 0;
        int total_ops = 0;
        std::ofstream fout("mw_4.txt");

        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_latency[i].size(); j++) {
                total_latency += thread_latency[i][j];
                total_ops++;
            }
        }

        fout << " avg_latency=" << total_latency / total_ops << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " read_latency=" << (read_ops[0] ? read_latency[0] / read_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " write_latency=" << (write_ops[0] ? write_latency[0] / write_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " scan_latency=" << (scan_ops[0] ? scan_latency[0] / scan_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < range_block[i].size(); j++) {
                fout << j << " block=" << range_block[i][j] << " count=" << range_count[i][j];
                fout << " min_time=" << range_min_time[i][j] << " max_time=" << range_max_time[i][j];
                fout << " avg_time=";
                if (range_count[i][j])
                    fout << ((double)range_time[i][j] / range_count[i][j]);
                else
                    fout << "0";
                fout << "\n";
            }
            fout << "------------------------------------------------------\n";
        }
        fout.close();
    }
}

TEST(DBTest, MTMW_5) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    mt.test->LoadZipf("zipf-3000k.txt");
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MWThreadBody_5, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("mw_5.csv", std::ios::out);
        double total_latency;
        int i, j = 0;
        while (true) {
            total_latency = 0;
            for (i = 0; i < kNumThreads; i++) {
                if (j >= thread_latency[i].size()) {
                    break;
                } else {
                    total_latency += thread_latency[i][j];
                }
            }
            if (i < kNumThreads) {
                break;
            }
            fout << total_latency << "\n";
            j++;
        }
    }

    {
        double total_latency = 0;
        int total_ops = 0;
        std::ofstream fout("mw_5.txt");

        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_latency[i].size(); j++) {
                total_latency += thread_latency[i][j];
                total_ops++;
            }
        }

        fout << " avg_latency=" << total_latency / total_ops << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " read_latency=" << (read_ops[0] ? read_latency[0] / read_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " write_latency=" << (write_ops[0] ? write_latency[0] / write_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " scan_latency=" << (scan_ops[0] ? scan_latency[0] / scan_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < range_block[i].size(); j++) {
                fout << j << " block=" << range_block[i][j] << " count=" << range_count[i][j];
                fout << " min_time=" << range_min_time[i][j] << " max_time=" << range_max_time[i][j];
                fout << " avg_time=";
                if (range_count[i][j])
                    fout << ((double)range_time[i][j] / range_count[i][j]);
                else
                    fout << "0";
                fout << "\n";
            }
            fout << "------------------------------------------------------\n";
        }
        fout.close();
    }
}

TEST(DBTest, MTMW_6) {
    MTState mt;
    mt.test = this;
    mt.stop.Release_Store(0);
    mt.test->LoadZipf("zipf-5000k.txt");
    for (int id = 0; id < kNumThreads; id++) {
        mt.thread_done[id].Release_Store(0);
    }

    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
        thread[id].state = &mt;
        thread[id].id = id;
        env_->StartThread(MWThreadBody_6, &thread[id]);
    }

    clock_t start_t = clock();
    for (int id = 0; id < kNumThreads; id++) {
        while (mt.thread_done[id].Acquire_Load() == NULL) {
            DelayMilliseconds(1000);
        }
    }
    dbfull()->PrintFiles();

    for (int i = 0; i < kNumThreads; i++) {
        fprintf(stderr, "success:  ... stopping thread %d after %d ops\n", i, mt.counter[i]);
    }
    clock_t end_t = clock();

    {
        std::ofstream fout("mw_6.csv", std::ios::out);
        double total_latency;
        int i, j = 0;
        while (true) {
            total_latency = 0;
            for (i = 0; i < kNumThreads; i++) {
                if (j >= thread_latency[i].size()) {
                    break;
                } else {
                    total_latency += thread_latency[i][j];
                }
            }
            if (i < kNumThreads) {
                break;
            }
            fout << total_latency << "\n";
            j++;
        }
    }

    {
        double total_latency = 0;
        int total_ops = 0;
        std::ofstream fout("mw_6.txt");

        for (int i = 0; i < kNumThreads; i++) {
            fout << "thread " << i << "stopping thread after " << mt.counter[i] << "ops\n";
        }

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < thread_latency[i].size(); j++) {
                total_latency += thread_latency[i][j];
                total_ops++;
            }
        }

        fout << " avg_latency=" << total_latency / total_ops << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " read_latency=" << (read_ops[0] ? read_latency[0] / read_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " write_latency=" << (write_ops[0] ? write_latency[0] / write_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " scan_latency=" << (scan_ops[0] ? scan_latency[0] / scan_ops[0] : 0) << " us";
        fout << "\n------------------------------------------------------\n";
        fout << " all_time=" << (double)(end_t - start_t) / (1000 * 1000);
        fout << "\n------------------------------------------------------\n";

        for (int i = 0; i < kNumThreads; i++) {
            for (int j = 0; j < range_block[i].size(); j++) {
                fout << j << " block=" << range_block[i][j] << " count=" << range_count[i][j];
                fout << " min_time=" << range_min_time[i][j] << " max_time=" << range_max_time[i][j];
                fout << " avg_time=";
                if (range_count[i][j])
                    fout << ((double)range_time[i][j] / range_count[i][j]);
                else
                    fout << "0";
                fout << "\n";
            }
            fout << "------------------------------------------------------\n";
        }
        fout.close();
    }
}

std::string MakeKey(unsigned int num) {
    char buf[30];
    snprintf(buf, sizeof(buf), "%016u", num);
    return std::string(buf);
}

void BM_LogAndApply(int iters, int num_base_files) {
    std::string dbname = test::TmpDir() + "/leveldb_test_benchmark";
    DestroyDB(dbname, Options());

    DB* db = NULL;
    Options opts;
    opts.create_if_missing = true;
    Status s = DB::Open(opts, dbname, &db);
    ASSERT_OK(s);
    ASSERT_TRUE(db != NULL);

    delete db;
    db = NULL;

    Env* env = Env::Default();

    port::Mutex mu;
    MutexLock l(&mu);

    InternalKeyComparator cmp(BytewiseComparator());
    Options options;
    VersionSet vset(dbname, &options, NULL, &cmp);
    bool save_manifest;
    ASSERT_OK(vset.Recover(&save_manifest));
    VersionEdit vbase;
    uint64_t fnum = 1;
    for (int i = 0; i < num_base_files; i++) {
        InternalKey start(MakeKey(2 * fnum), 1, kTypeValue);
        InternalKey limit(MakeKey(2 * fnum + 1), 1, kTypeDeletion);
        vbase.AddFile(2, fnum++, 1 /* file size */, start, limit);
    }
    ASSERT_OK(vset.LogAndApply(&vbase, &mu));

    uint64_t start_micros = env->NowMicros();

    for (int i = 0; i < iters; i++) {
        VersionEdit vedit;
        vedit.DeleteFile(2, fnum);
        InternalKey start(MakeKey(2 * fnum), 1, kTypeValue);
        InternalKey limit(MakeKey(2 * fnum + 1), 1, kTypeDeletion);
        vedit.AddFile(2, fnum++, 1 /* file size */, start, limit);
        vset.LogAndApply(&vedit, &mu);
    }
    uint64_t stop_micros = env->NowMicros();
    unsigned int us = stop_micros - start_micros;
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", num_base_files);
    fprintf(stderr, "BM_LogAndApply/%-6s   %8d iters : %9u us (%7.0f us / iter)\n", buf, iters, us,
            ((float)us) / iters);
}

} // namespace leveldb

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--benchmark") {
        leveldb::BM_LogAndApply(1000, 1);
        leveldb::BM_LogAndApply(1000, 100);
        leveldb::BM_LogAndApply(1000, 10000);
        leveldb::BM_LogAndApply(100, 100000);
        return 0;
    }

    return leveldb::test::RunAllTests();
}
