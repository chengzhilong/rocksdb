//
// Created by zzyyy on 2018/11/2.
//

#pragma once

#include "utilities/nvm_write_cache/nvm_write_cache.h"
#include "utilities/nvm_write_cache/nvm_cache_options.h"

namespace rocksdb{

    class FixedRangeChunkBasedNVMWriteCache: public NVMWriteCache{
        explicit FixedRangeChunkBasedNVMWriteCache(FixedRangeBasedOptions* cache_options_);

        ~FixedRangeChunkBasedNVMWriteCache();


    };
}