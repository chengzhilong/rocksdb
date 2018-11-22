#include "util/coding.h"
#include "table/merging_iterator.h"

#include "fixed_range_tab.h"
#include "persistent_chunk.h"
#include "persistent_chunk_iterator.h"

namespace rocksdb {

using pmem::obj::persistent_ptr;

FixedRangeTab::FixedRangeTab(pool_base &pop, p_range::p_node node, FixedRangeBasedOptions *options)
        :   pop_(pop),
            interal_options_(options) {
    if(!pmap_node_->seq_num_ == 0){
        // new node
        pmap_node_ = node;
        raw_ = pmap_node_->buf->get();
        // set cur_
        EncodeFixed64(raw_, 0);
        // set seq_
        EncodeFixed64(raw_ + sizeof(uint64_t), 0);
        raw_ += 2 * sizeof(uint64_t);
        in_compaction_ = false;
        pmap_node_->inited_ = true;
    }else{
        // rebuild
        RebuildBlkList();
    }

}

/* *
 * | prefix data | prefix size |
 * | cur_ | seq_ |
 * | chunk blmFilter | chunk len | chunk data ..| 不定长
 * | chunk blmFilter | chunk len | chunk data ...| 不定长
 * | chunk blmFilter | chunk len | chunk data .| 不定长
 * */

InternalIterator* FixedRangeTab::NewInternalIterator(
        const InternalKeyComparator* icmp, Arena *arena)
{
  InternalIterator* internal_iter;
  MergeIteratorBuilder merge_iter_builder(icmp,
                                          arena);
  // TODO
  // 预设 range 持久化
  //  char *chunkBlkOffset = data_ + sizeof(stat.used_bits_) + sizeof(stat.start_)
  //      + sizeof(stat.end_);
  PersistentChunk pchk;
  for (ChunkBlk &blk : blklist) {
    pchk.reset(interal_options_->chunk_bloom_bits_, blk.chunkLen_,
               pmap_node_->buf + blk.getDatOffset());
    merge_iter_builder.AddIterator(pchk.NewIterator(arena));
  }

    internal_iter = merge_iter_builder.Finish();
    return internal_iter;
}

Status FixedRangeTab::Get(const InternalKeyComparator &internal_comparator,
                          const Slice &key, std::string *value)
{
  // 1.从下往上遍历所有的chunk
  PersistentChunkIterator *iter = new PersistentChunkIterator;
  shared_ptr<PersistentChunkIterator> sp_persistent_chunk_iter(iter);

  uint64_t bloom_bits = interal_options_->chunk_bloom_bits_;
  for (size_t i = blklist.size() - 1; i >= 0; i--) {
    ChunkBlk &blk = blklist.at(i);
    persistent_ptr<char[]> bloom_dat = pmap_node_->buf + blk.offset_;
    // 2.获取当前chunk的bloom data，查找这个bloom data判断是否包含对应的key
    if (interal_options_->filter_policy_->KeyMayMatch(key, Slice(bloom_dat.get(), bloom_bits))) {
      // 3.如果有则读取元数据进行chunk内的查找
      new (iter) PersistentChunkIterator(pmap_node_->buf + blk.getDatOffset(), blk.chunkLen_,
                                        nullptr);
      Status s = searchInChunk(iter, internal_comparator, key, value);
      if (s.ok()) return s;
    } else {
      continue;
    }
  } // 4.循环直到查找完所有的chunk
}

/* range data format:
 *
 * |--  cur_  --|
 * |--  seq_  --|
 * |-- chunk1 --|
 * |-- chunk2 --|
 * |--   ...  --|
 *
 * */

void FixedRangeTab::Append(const InternalKeyComparator& icmp,
                           const char *bloom_data, const Slice &chunk_data,
                           const Slice& start, const Slice& end) {
    if (pmap_node_->dataLen + chunk_data.size_ >= pmap_node_->bufSize
        || pmap_node_->chunk_num_ > max_chunk_num_to_flush()) {
        // TODO：mark tab as pendding compaction
    }

    if(in_compaction_){
        // TODO: append when need compaction
    }

    //size_t cur_len = node_in_pmem_map->dataLen;
    size_t chunk_blk_len = interal_options_->chunk_bloom_bits_ + sizeof(uint64_t) + chunk_data.size();
    uint64_t raw_cur = DecodeFixed64(raw_ - 2 * sizeof(uint64_t));
    uint64_t last_seq = DecodeFixed64(raw_ - sizeof(uint64_t));

    char *dst = raw_ + raw_cur;
    // append bloom data
    memcpy(dst, bloom_data, interal_options_->chunk_bloom_bits_);
    // append chunk data size
    EncodeFixed64(dst + interal_options_->chunk_bloom_bits_, chunk_data.size());

    dst += interal_options_->chunk_bloom_bits_ + sizeof(uint64_t);
    // append data
    memcpy(dst, chunk_data.data(), chunk_data.size());

    // update cur and seq
    // transaction
    {
        EncodeFixed64(raw_ - 2 * sizeof(uint64_t), raw_cur + chunk_blk_len);
        EncodeFixed64(raw_ - sizeof(uint64_t), last_seq + 1);
    }
    // update meta info

    CheckAndUpdateKeyRange(icmp, start, end);

    // update version
    // transaction
    {
        pmap_node_->seq_num_ = pmap_node_->seq_num_ + 1;
        pmap_node_->chunk_num_ = pmap_node_->chunk_num_ + 1;
        pmap_node_->dataLen = pmap_node_->dataLen + chunk_blk_len
    }

    // record this offset to volatile vector
    blklist.emplace_back(interal_options_->chunk_bloom_bits_, raw_cur, chunk_data.size());
}

void FixedRangeTab::CheckAndUpdateKeyRange(const InternalKeyComparator &icmp, const Slice &new_start,
                                           const Slice &new_end) {
    Slice cur_start, cur_end;
    bool update_start = false, update_end = false;
    GetRealRange(cur_start, cur_end);
    if(icmp.Compare(cur_start, new_start) > 0){
        cur_start = new_start;
        update_start = true;
    }

    if(icmp.Compare(cur_end, new_end) < 0){
        cur_end = new_end;
        update_end = true;
    }

    if(update_start || update_end){
        transaction::run(pop_, [&]{
            persistent_ptr<char[]> new_range = make_persistent<char[]>(cur_start.size() + cur_end.size()
                    + 2 * sizeof(uint64_t));
            // get raw ptr
            char* p_new_range = new_range.get();
            // put start
            EncodeFixed64(p_new_range, cur_start.size());
            memcpy(p_new_range + sizeof(uint64_t), cur_start.data(), cur_start.size());
            // put end
            p_new_range += sizeof(uint64_t) + cur_start.size();
            EncodeFixed64(p_new_range, cur_end.size());
            memcpy(p_new_range + sizeof(uint64_t), cur_end.data(), cur_end.size());

        });
    }
}

void FixedRangeTab::Release() {
    //TODO: release
    // 删除这个range
}

void FixedRangeTab::CleanUp() {
    // 清除这个range的数据
    EncodeFixed64(raw_ - 2 * sizeof(uint64_t), 0);//set cur to 0
    blklist.clear();
    p_range::Node* vnode = pmap_node_.get();
    in_compaction_ = false;
    transaction::run(pop_, [&]{
        vnode->chunk_num_ = 0;
        vnode->dataLen = 0;
        Slice start, end;
        GetRealRange(start, end);
        delete_persistent<char[]>(vnode->key_range_, start.size() + end.size() + 2 * sizeof(uint64_t));
        vnode->key_range_ = nullptr;
    });

}

/*Status FixedRangeTab::Get(const InternalKeyComparator &internal_comparator, const rocksdb::Slice &key,
                          std::string *value) {
    // 1.从下往上遍历所有的chunk
    uint64_t bloom_bits = interal_options_->chunk_bloom_bits_;
    for(size_t i = blklist.size() - 1; i >= 0; i--){
        size_t chunk_off = blklist[i].getDatOffset();
        char* bloom_data = raw_ + (chunk_off - sizeof(uint64_t) - interal_options_->chunk_bloom_bits_);
        if (interal_options_->filter_policy_->KeyMayMatch(key, Slice(bloom_data, bloom_bits))) {
            // 3.如果有则读取元数据进行chunk内的查找
            uint64_t chunk_len = DecodeFixed64(bloom_data + bloom_bits);
            uint64_t item_num = DecodeFixed64(raw_ + chunk_len - sizeof(uint64_t));
            char *chunk_index = raw_ + (chunk_off +  (item_num + 1) * sizeof(uint64_t));
            std::vector<uint64_t> item_offs;
            while (item_num-- > 0) {
                item_offs.push_back(DecodeFixed64(chunk_index));
                chunk_index += sizeof(uint64_t);
            }
            Status s = DoInChunkSearch(internal_comparator, key, value, item_offs, raw_+chunk_off);
            if (s.ok()) return s;
        } else {
            continue;
        }
    }

    // 4.循环直到查找完所有的chunk
}*/


Status FixedRangeTab::searchInChunk(PersistentChunkIterator *iter, const InternalKeyComparator &icmp,
                                    const Slice &key, std::string *value) {
    size_t left = 0, right = iter->count() - 1;
    while (left <= right) {
        size_t middle = left + ((right - left) >> 1);
        iter->SeekTo(middle);
        Slice& ml_key = iter->key();
        int result = icmp.Compare(ml_key, key);
        if (result == 0) {
            //found
            Slice& raw_value = iter->value();
            value->assign(raw_value.data(), raw_value.size());
            return Status::OK();
        } else if (result < 0) {
            // middle < key
            left = middle + 1;
        } else if (result > 0) {
            // middle >= key
            right = middle - 1;
        }
    }
    return Status::NotFound("not found");
}

Slice FixedRangeTab::GetKVData(char *raw, uint64_t item_off) {
    char *target = raw + item_off;
    uint64_t target_size = DecodeFixed64(target);
    return Slice(target + sizeof(uint64_t), static_cast<size_t>(target_size));
}

void FixedRangeTab::GetRealRange(rocksdb::Slice &real_start, rocksdb::Slice &real_end) {
    if(pmap_node_->key_range_ != nullptr){
        char *raw = pmap_node_->key_range_.get();
        real_start = GetKVData(raw, 0);
        real_end = GetKVData(raw, real_start.size() + sizeof(uint64_t));
    }else{
        // if there is no key_range return null Slice
        real_start = Slice();
        real_end = Slice();
    }

}

void FixedRangeTab::RebuildBlkList()
{
    // TODO :check consistency
    //ConsistencyCheck();
    size_t dataLen;
    dataLen = pmap_node_->dataLen;
    // TODO
    // range 从一开始就存 chunk ?
    size_t offset = 0;
    while(offset < dataLen) {
        size_t chunkLenOffset = offset + interal_options_->chunk_bloom_bits_;
        size_t chunkLen;
        memcpy(&chunkLen, pmap_node_->buf + chunkLenOffset, sizeof(chunkLen));
        blklist.emplace_back(interal_options_->chunk_bloom_bits_, offset, chunkLen);
        // next chunk block
        offset += interal_options_->chunk_bloom_bits_ + sizeof(chunkLen) + chunkLen;
    }
}

Usage FixedRangeTab::RangeUsage() {
    Usage usage;
    Slice start, end;
    GetRealRange(start, end);
    usage.range_size = pmap_node_->total_size_;
    usage.chunk_num = pmap_node_->chunk_num_;
    usage.start.DecodeFrom(start);
    usage.end.DecodeFrom(end);
    return usage;
}

void FixedRangeTab::ConsistencyCheck() {
    uint64_t data_seq_num;
    data_seq_num = DecodeFixed64(raw_ - sizeof(uint64_t));
    p_range::Node* vnode = pmap_node_.get();
    if(data_seq_num != vnode->seq_num_){
        // TODO:又需要一个comparator
        /*Slice last_start, last_end;
        GetLastChunkKeyRange(last_start, last_end);*/
    }
}

} // namespace rocksdb
