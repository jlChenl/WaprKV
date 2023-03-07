#include "db/garbage_collector.h"

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "fcntl.h"
#include "leveldb/slice.h"

namespace leveldb {

GarbageCollector::GarbageCollector(DBImpl* db)
    : vlog_number_(0), garbage_pos_(0), vlog_reader_(NULL), db_(db), mem_(NULL) {
    finished_ = false;
    need_pause_.Release_Store(NULL);
    pthread_mutex_init(&mutex_, NULL);
    pthread_cond_init(&cond_recover_, NULL);
    pthread_cond_init(&cond_pause_, NULL);
}

GarbageCollector::~GarbageCollector() {
    delete vlog_reader_;
    mem_->Unref();
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_recover_);
    pthread_cond_destroy(&cond_pause_);
    if (vlog_rewrite_.empty() == false) {
        db_->vlog_manager_.AddDropCount(vlog_rewrite_);
    }
}

void GarbageCollector::SetVlog(uint64_t vlog_number, uint64_t garbage_beg_pos) {
    SequentialFile* vlr_file;
    db_->options_.env->NewSequentialFile(VLogFileName(db_->dbname_, vlog_number), &vlr_file);
    vlog_reader_ = new log::VReader(vlr_file, true, 0);
    // vlog_reader_ = db_->vlog_manager_.GetVlog(vlog_number);
    vlog_number_ = vlog_number;
    garbage_pos_ = garbage_beg_pos;
    initial_pos_ = garbage_pos_;
    mem_ = new MemTable(db_->internal_comparator_);
    mem_->Ref();
}

void GarbageCollector::FullGarbageCollect(VersionEdit* edit, bool* save_edit, bool* delete_vlog) {
    // std::cout << "111\n";
    Status status;
    *save_edit = false;
    *delete_vlog = true;
    Slice record;
    std::string str;
    if (garbage_pos_ > 0) {
        if (!vlog_reader_->SkipToPos(garbage_pos_)) {
            return;
        }
    }
    // std::cout << "222\n";
    auto& vlog_partial_offset = db_->vlog_partial_offset_[vlog_number_];
    // std::cout << "333\n";
    WriteBatch batch(vlog_number_ > sort_vlog_number_initial);
    Slice key, value;
    std::string val;
    uint64_t seq;
    int record_count;
    bool isEndOfFile = false;
    size_t kv_rewrite_size = 128;

    // std::cout << "FullGC start vlog=" << vlog_number_
    //           << "  -----------------------------------------------------------------\n";
    std::vector<std::string> k_buf(kv_rewrite_size), v_buf(kv_rewrite_size), ptr_buf;
    std::vector<SequenceNumber> seq_buf(kv_rewrite_size);
    std::vector<ValueType> type_buf(kv_rewrite_size);
    size_t count = 0, total_count = 0, rewrite_count = 0, vlog_count = 0, skip_count = 0, delete1_count = 0,
           delete2_count = 0, error_count = 0;
    uint64_t check_numb = 0, check_offset = 0;
    while (!db_->IsShutDown()) {
        int head_size = 0;
        if (!vlog_reader_->ReadRecord(&record, &str, head_size)) {
            isEndOfFile = true;
            break;
        }
        WriteBatchInternal::SetContents(&batch, record);
        seq = WriteBatchInternal::Sequence(&batch);
        record_count = WriteBatchInternal::Count(&batch);
        ReadOptions read_options;
        uint64_t size = record.size();
        uint64_t pos = vlog_number_ > sort_vlog_number_initial ? 4 : 12;
        garbage_pos_ += head_size;
        std::string str_rec = record.ToString();
        int cnt = 0;
        while (pos < size) {
            vlog_count++;
            cnt++;
            // if (vlog_number_ > sort_vlog_number_initial)
            //     std::cout << "1 full gc pos_=" << garbage_pos_ << " vlog_count=" << vlog_count << "\n";
            bool isDel = false;
            uint64_t old_pos = pos;
            if (need_pause_.NoBarrier_Load() != NULL) {
                if (count > 0) {
                    status = db_->ReWriteValue(v_buf, seq_buf, ptr_buf, count, vlog_rewrite_, check_numb, check_offset);
                    assert(status.ok());
                    for (size_t i = 0; i < count; i++) {
                        mem_->Add(seq_buf[i], type_buf[i], k_buf[i], ptr_buf[i]);
                    }
                }
                pthread_mutex_lock(&mutex_);
                pthread_cond_signal(&cond_pause_);
                pthread_cond_wait(&cond_recover_, &mutex_);
                pthread_mutex_unlock(&mutex_);
                // std::cout << "start clear mem 1 vlog=" << vlog_number_ << " count=" << count << " total=" <<
                // total_count
                //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " error=" << error_count
                //           << " vlog=" << vlog_count << "-----------------\n";
                vlog_rewrite_.clear();
                ptr_buf.clear();
                // std::cout << "finish clear mem vlog=" << vlog_number_ << " count=" << count << " total=" <<
                // total_count
                //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " delete=" << delete1_count
                //           << " " << delete2_count << " error=" << error_count << " vlog=" << vlog_count
                //           << "-----------------\n";
                count = 0;
                total_count = 0;
            }

            if (vlog_number_ > sort_vlog_number_initial) {
                if (vlog_partial_offset.count(garbage_pos_ + old_pos + 8)) {
                    // db_->gc_skip_off[vlog_number_].insert(garbage_pos_ + old_pos + 8);
                    pos = old_pos + vlog_partial_offset[garbage_pos_ + old_pos + 8] + 8;
                    skip_count++;
                    // std::cout << "full gc skip vlog count=" << vlog_count << "\n";
                    continue;
                }
            } else {
                if (vlog_partial_offset.count(garbage_pos_ + old_pos)) {
                    // db_->gc_skip_off[vlog_number_].insert(garbage_pos_ + old_pos);
                    pos = old_pos + vlog_partial_offset[garbage_pos_ + old_pos];
                    skip_count++;
                    // std::cout << "full gc skip vlog count=" << vlog_count << "\n";
                    continue;
                }
            }

            // if (vlog_number_ > sort_vlog_number_initial) std::cout << "start parse record\n";
            status = WriteBatchInternal::ParseRecord(&batch, pos, key, value, isDel, &seq);
            if (!status.ok()) {
                std::cout << "parse record error " << pos << " " << key.size() << " " << value.size() << "\n";
            }
            // } else {
            // std::cout << "parse record success\n";
            // }
            // if (vlog_number_ > sort_vlog_number_initial)
            //     std::cout << "parse record success " << old_pos << " " << pos << " " << size << "\n";

            if (!isDel) {
                // if (vlog_number_ > sort_vlog_number_initial) std::cout << "full gc GetPtr start\n";
                status = db_->GetPtr(read_options, key, &val);
                // if (vlog_number_ > sort_vlog_number_initial) std::cout << "full gc GetPtr finish\n";
                if (!status.ok()) {
                    error_count++;
                    std::cout << "full gc getptr error " << status.ToString() << " " << key.size()
                              << "  -----------------\n";
                }
                uint64_t item_size;
                std::pair<uint32_t, uint64_t> file_pos;
                Slice val_ptr(val);
                status = ParsePtr(val_ptr, item_size, file_pos);
                if (!status.ok()) {
                    std::cout << "full gc parseptr error " << status.ToString() << "  -----------------\n";
                }
                if (file_pos.first == vlog_number_ && file_pos.second + item_size == garbage_pos_ + pos) {
                    k_buf[count] = key.ToString();
                    if (vlog_number_ > sort_vlog_number_initial) old_pos += 8;
                    v_buf[count] = str_rec.substr(old_pos, pos - old_pos);
                    seq_buf[count] = SequenceNumber(seq + cnt);
                    type_buf[count] = kTypeValue;
                    count++;
                    total_count++;
                    rewrite_count++;
                } else {
                    delete2_count++;
                }
            } else {
                delete1_count++;
            }

            if (count == kv_rewrite_size) {
                status = db_->ReWriteValue(v_buf, seq_buf, ptr_buf, count, vlog_rewrite_, check_numb, check_offset);
                assert(status.ok());
                for (size_t i = 0; i < count; i++) {
                    mem_->Add(seq_buf[i], type_buf[i], k_buf[i], ptr_buf[i]);
                }
                count = 0;
                ptr_buf.clear();
            }
            // if (vlog_number_ > sort_vlog_number_initial)
            //     std::cout << "3 full gc pos_=" << garbage_pos_ << " vlog_count=" << vlog_count << "\n";
        }

        assert(pos == size);
        garbage_pos_ += size;
        if (mem_->ApproximateMemoryUsage() > db_->options_.write_buffer_size) {
            if (count > 0) {
                status = db_->ReWriteValue(v_buf, seq_buf, ptr_buf, count, vlog_rewrite_, check_numb, check_offset);
                assert(status.ok());
                for (size_t i = 0; i < count; i++) {
                    mem_->Add(seq_buf[i], type_buf[i], k_buf[i], ptr_buf[i]);
                }
            }
            uint64_t flushed_file;
            pthread_mutex_lock(&mutex_);
            status = db_->FullGCCompaction(check_numb, check_offset, &flushed_file);
            assert(status.ok());
            mem_->Unref();
            mem_ = new MemTable(db_->internal_comparator_);
            mem_->Ref();
            pthread_mutex_unlock(&mutex_);
            // std::cout << "flush to sstable 1 vlog=" << vlog_number_ << " count=" << count << " total=" << total_count
            //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " error=" << error_count
            //           << " vlog=" << vlog_count << " sst=" << flushed_file << "-----------------\n";

            vlog_rewrite_.clear();
            ptr_buf.clear();
            count = 0;
            total_count = 0;
        }
    }

    // std::cout << "prepare 1 to finish full gc vlog=" << vlog_number_ << " count=" << count << " total=" <<
    // total_count
    //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " delete=" << delete1_count << " "
    //           << delete2_count << " error=" << error_count << " vlog=" << vlog_count << " " << db_->IsShutDown()
    //           << "-----------------\n";

    if (total_count > 0) {
        if (count > 0) {
            status = db_->ReWriteValue(v_buf, seq_buf, ptr_buf, count, vlog_rewrite_, check_numb, check_offset);
            assert(status.ok());
            for (size_t i = 0; i < count; i++) {
                mem_->Add(seq_buf[i], type_buf[i], k_buf[i], ptr_buf[i]);
            }
        }

        int sleep_count = 0;
        while (!db_->IsShutDown() && need_pause_.NoBarrier_Load() == NULL && sleep_count < 100) {
            db_->env_->SleepForMicroseconds(500);
            sleep_count++;
        }

        if (need_pause_.NoBarrier_Load() != NULL) {
            pthread_mutex_lock(&mutex_);
            pthread_cond_signal(&cond_pause_);
            pthread_cond_wait(&cond_recover_, &mutex_);
            finished_ = true;
            pthread_mutex_unlock(&mutex_);
            // std::cout << "start clear mem 2 vlog=" << vlog_number_ << " count=" << count << " total=" << total_count
            //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " error=" << error_count
            //           << " vlog=" << vlog_count << "-----------------\n";
        } else {
            uint64_t flushed_file;
            pthread_mutex_lock(&mutex_);
            status = db_->FullGCCompaction(check_numb, check_offset, &flushed_file);
            assert(status.ok());
            mem_->Unref();
            mem_ = new MemTable(db_->internal_comparator_);
            mem_->Ref();
            finished_ = true;
            pthread_cond_signal(&cond_pause_);
            pthread_mutex_unlock(&mutex_);
            // std::cout << "flush to sstable 2 vlog=" << vlog_number_ << " count=" << count << " total=" << total_count
            //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " error=" << error_count
            //           << " vlog=" << vlog_count << " sst=" << flushed_file << "-----------------\n";
        }
        vlog_rewrite_.clear();
        ptr_buf.clear();
    } else {
        pthread_mutex_lock(&mutex_);
        finished_ = true;
        pthread_cond_signal(&cond_pause_);
        pthread_mutex_unlock(&mutex_);
    }

    // std::cout << "prepare 2 to finish full gc vlog=" << vlog_number_ << " count=" << count << " total=" <<
    // total_count
    //           << " rewrite=" << rewrite_count << " skip=" << skip_count << " delete=" << delete1_count << " "
    //           << delete2_count << " error=" << error_count << " vlog=" << vlog_count << " " << db_->IsShutDown()
    //           << "-----------------\n";

    if (isEndOfFile) {
        *delete_vlog = true;
    } else {
        edit->SetTailInfo(vlog_number_, garbage_pos_);
        *save_edit = true;
        *delete_vlog = false;
    }
}

void GarbageCollector::BeginGarbageCollect(VersionEdit* edit, bool* save_edit) {
    *save_edit = false;
    uint64_t garbage_pos = garbage_pos_;
    Slice record;
    std::string str;
    WriteOptions write_options;
    if (garbage_pos_ > 0) {
        if (!vlog_reader_->SkipToPos(garbage_pos_)) // 从指定位置开始回收
        {
            return;
        }
    }

    Slice key, value;
    WriteBatch batch, clean_valid_batch;
    std::string val;
    bool isEndOfFile = false;
    SequenceNumber seq;
    // std::cout << "GarbageCollect start " << vlog_number_ << "\n";
    while (!db_->IsShutDown()) // db关了
    {
        int head_size = 0;
        if (!vlog_reader_->ReadRecord(&record, &str, head_size)) {
            isEndOfFile = true;
            break;
        }

        garbage_pos_ += head_size;
        WriteBatchInternal::SetContents(&batch, record); // 会把record的内容拷贝到batch中去
        seq = WriteBatchInternal::Sequence(&batch);
        ReadOptions read_options;
        uint64_t size = record.size();                                   // size是整个batch的长度，包括batch头
        uint64_t pos = vlog_number_ > sort_vlog_number_initial ? 4 : 12; // 是相对batch起始位置的偏移
        uint64_t old_garbage_pos = garbage_pos_;
        while (pos < size) // 遍历batch看哪些kv有效
        {
            bool isDel = false;
            Status s =
                WriteBatchInternal::ParseRecord(&batch, pos, key, value, isDel); // 解析完一条kv后pos是下一条kv的pos
            assert(s.ok());
            garbage_pos_ = old_garbage_pos + pos;

            // log文件里的delete记录可以直接丢掉，因为sst文件会记录
            if (!isDel && db_->GetPtr(read_options, key, &val).ok()) {
                Slice val_ptr(val);
                uint32_t file_numb;
                uint64_t item_pos, item_size;
                GetVarint64(&val_ptr, &item_size);
                GetVarint32(&val_ptr, &file_numb);
                GetVarint64(&val_ptr, &item_pos);
                if (item_pos + item_size == garbage_pos_ && file_numb == vlog_number_) {
                    clean_valid_batch.Put(key, value);
                }
            }
        }
        assert(pos == size);
        if (WriteBatchInternal::ByteSize(&clean_valid_batch) >
            db_->options_
                .clean_write_buffer_size) { // clean_write_buffer_size必须要大于12才行，12是batch的头部长，创建batch或者clear
                                            // batch后的初始大小就是12
            Status s = db_->Write(write_options, &clean_valid_batch);
            assert(s.ok());
            clean_valid_batch.Clear();
        }
    }
    // std::cout << "GarbageCollect finish\n";

#ifndef NDEBUG
    Log(db_->options_.info_log, "tail is %lu, last key is %s, ;last value is %s\n", garbage_pos_, key.data(),
        value.data());
    if (db_->IsShutDown())
        Log(db_->options_.info_log, " clean stop by shutdown\n");
    else if (isEndOfFile)
        Log(db_->options_.info_log, " clean stop by read end\n");
    else
        Log(db_->options_.info_log, " clean stop by unknown reason\n");
#endif
    if (WriteBatchInternal::Count(&clean_valid_batch) > 0) {
        Status s = db_->Write(write_options, &clean_valid_batch);
        assert(s.ok());
        clean_valid_batch.Clear();
    }

    if (garbage_pos_ - garbage_pos > 0) {
        if (isEndOfFile) {
            *save_edit = false;
        } else {
            vlog_reader_->DeallocateDiskSpace(garbage_pos, garbage_pos_ - garbage_pos);
            edit->SetTailInfo(vlog_number_, garbage_pos_);
            *save_edit = true;
        }
    }
}
} // namespace leveldb
