//
// Created by 张艺文 on 2018/10/31.
//

#pragma once
#ifndef ROCKSDB_RANGE_MEMTABLE_SET_H
#define ROCKSDB_RANGE_MEMTABLE_SET_H

#include <stdio.h>
#include <string>
#include "include/rocksdb/iterator.h"

namespace rocksdb{

        class PersistentRangeMem {
            public:
            Iterator *NewIterator();

            PersistentRangeMem();

            ~PersistentRangeMem();

            Status
            Get(
            const Slice
            &key, std::string * value);

            bool IsComppactWorking();

            char *GetBloomFilter();

            void Append(Slice& chunk_data, const char* chunk_bloom_data, const char* global_bloom_data);

            void UpdateStat(const Slice
            &new_start,
            const Slice
            &new_end);

            void GetRealRange(Slice
            &real_start, Slice & real_end);

            void MaybeScheduleCompact();

            void Release();

            void CleanUp();

            struct PersistentRangeMemStat {
                Slice start, end;
                Slice real_start, real_end;
                size_t chunk_num;
            };
        };

        class PersistentRangeMemSet {
            public:
            Iterator *NewIterator();

            PersistentRangeMemSet();

            ~PersistentRangeMemSet();

            PersistentRangeMem *GetRangeMemtable(uint64_t range_mem_id);

            Status
            Get(
            const Slice
            &key, std::string * value);
        };


}


#endif //ROCKSDB_RANGE_MEMTABLE_SET_H
