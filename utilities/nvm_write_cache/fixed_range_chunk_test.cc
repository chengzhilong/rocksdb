#include <string>
#include <unistd.h>
#include <util/testharness.h>

#include "third-party/gtest-1.7.0/fused-src/gtest/gtest.h"
#include "util/testutil.h"
#include "util/random.h"

#include "utilities/nvm_write_cache/skiplist/test_common.h"
#include "fixed_range_tab.h"
#include "chunk.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"
#include "common.h"
#include "debug.h"

#define TAB_DEBUG

namespace rocksdb {

/*
 * KeyGenerator—— generate key(SEQUENTIAL, RANDOM, UNIQIUE_RANDOM)
 * RandomGenerator——generate value(RANDOM)
 * FixedRangeChunkTest——Test fixed range chunk
 *      - FixedRangeChunkTest::FixedRangeChunkTest(ioptions, file, pmem_size, false);
 *      - 构造Key和Value
 */

enum WriteMode {
    RANDOM = 0,
    SEQUENTIAL,
    SEQUENTIAL_10K,
    UNIQUE_RANDOM
};

class KeyGenerator {
public:
    KeyGenerator(Random64 *rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
            : rand_(rand), mode_(mode), num_(num), next_(0) {
        if (mode_ == UNIQUE_RANDOM) {
            // NOTE: if memory consumption of this approach becomes a concern,
            // we can either break it into pieces and only random shuffle a section
            // each time. Alternatively, use a bit map implementation
            // (https://reviews.facebook.net/differential/diff/54627/)
            values_.resize(num_);
            for (uint64_t i = 0; i < num_; ++i) {
                values_[i] = i;
            }
            std::shuffle(
                    values_.begin(), values_.end(),
                    std::default_random_engine(static_cast<unsigned int>(16)));
        }
    }

    uint64_t Next() {
        switch (mode_) {
            case SEQUENTIAL:
                return next_++;
            case SEQUENTIAL_10K:
                return (next_++)*100 + 10;
            case RANDOM:
                return rand_->Next() % num_;
            case UNIQUE_RANDOM:
                assert(next_ < num_);
                return values_[next_++];
        }
        assert(false);
        return std::numeric_limits<uint64_t>::max();
    }

private:
    Random64 *rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
};

class RandomGenerator {
private:
    std::string data_;
    unsigned int pos_;

public:
    RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < (unsigned) std::max(1048576, 4194304)) {
            // Add a short fragment that is as compressible as specified
            // by FLAGS_compression_ratio.
            test::CompressibleString(&rnd, 0.5, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    Slice Generate(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }

    Slice GenerateWithTTL(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }
};

class FixedRangeChunkTest : public testing::Test {
public:
    FixedRangeChunkTest() : file_name_("/pmem/fixed_range_chunk_test"),
                    prefix("fixRange_pref"),
                    icmp_(BytewiseComparator()) {
        foptions_ = new FixedRangeBasedOptions(
                            16, prefix.size(),
                            new SimplePrefixExtractor(prefix.size()),
                            NewBloomFilterPolicy(16, false),
                            1 << 27);
        base_size_ = (1 << 20);
        pmem_size_ = (base_size_ << 12);
        fixed_range_chunk_ = new FixedRangeChunkBasedNVMWriteCache(foptions_, file_name_, pmem_size_, true);
        value_size_ = 4 * 1024;
        last_prefix.clear();
        last_chunk = nullptr;
    }

    ~FixedRangeChunkTest() {
        pending_output_chunk.clear();
        delete fixed_range_chunk_;
    }

    std::string file_name_;
    std::string prefix;
    uint64_t pmem_size_;
    uint64_t base_size_;
    bool reset;

    FixedRangeBasedOptions* foptions_;
    const InternalKeyComparator icmp_;
    size_t value_size_;

    FixedRangeChunkBasedNVMWriteCache *fixed_range_chunk_;

    // 
    std::string last_prefix;        // 记录上次插入Key的prefix和所在的chunk
    BuildingChunk* last_chunk;
    std::unordered_map<std::string, BuildingChunk*> pending_output_chunk;
};

TEST_F(FixedRangeChunkTest, BuildChunk) {
    Random64 rand(16);
    KeyGenerator key_gen(&rand, SEQUENTIAL, 100);
    RandomGenerator value_gen;

    vector<string> insert_key;
	vector<string> insert_key_2;
    
    for (int i = 0; i < 20; i++) {
        for (int j = 0; j < 10; j++) {
            char key[17];
            sprintf(key, "%016lu", key_gen.Next());
            key[16] = 0;
            InternalKey ikey;
            ikey.Set(Slice(key, 17), static_cast<uint64_t>(i * 10 + j), kTypeValue);
            Slice i_user_key = ikey.user_key();
            std::string now_prefix = (*foptions_->prefix_extractor_)(i_user_key.data(),
                    i_user_key.size());
            if (now_prefix == last_prefix && last_chunk != nullptr) {
                last_chunk->Insert(ikey.Encode(), value_gen.Generate(value_size_));
            } else {
                BuildingChunk* now_chunk = nullptr;
                auto chunk_found = pending_output_chunk.find(now_prefix);
                if (chunk_found == pending_output_chunk.end()) {
                    DBG_PRINT("prepare to create new chunk: last_prefix[%s], now_prefix[%s]", 
                        last_prefix.c_str(), now_prefix.c_str());
                    auto new_chunk = new BuildingChunk(foptions_->filter_policy_, now_prefix);
                    DBG_PRINT("end create chunk");
                    pending_output_chunk[now_prefix] = new_chunk;
                    now_chunk = new_chunk;
                } else {
                    now_chunk = chunk_found->second;
                }

                now_chunk->Insert(ikey.Encode(), value_gen.Generate(value_size_));
                last_chunk = now_chunk;
                last_prefix = now_prefix;

                //DBG_PRINT("end insert");
            }
            insert_key.emplace_back(key, 17);
        }
    }

    DBG_PRINT("prepare to call RangeExistsOrCreat(): pending_output_chunk size[%lu]", pending_output_chunk.size());

    for (auto pending_chunk : pending_output_chunk) {
        fixed_range_chunk_->RangeExistsOrCreat(pending_chunk.first);
    }

    auto finish_build_chunk = [&](std::string prefix_chunk) {
        string bloom_data;
        ChunkMeta meta;
        meta.prefix = prefix_chunk;
        std::string* output_data = pending_output_chunk[prefix_chunk]->Finish(bloom_data,
            meta.cur_start, meta.cur_end);
        //Slice chunk_data(*output_data);
        DBG_PRINT("output_data: size[%lu], bloom_data: size[%lu]", output_data->size(), bloom_data.size());
        DBG_PRINT("meta.cur_start: [%s], cur_end: [%s]", meta.cur_start.data(), meta.cur_end.data());
		const Slice output_(output_data->c_str(), output_data->size());
        fixed_range_chunk_->AppendToRange(icmp_, bloom_data, output_, meta);
        delete output_data;
    };

    DBG_PRINT("Prepare to call finish_build_chunk()");
	FixedRangeTab* tab_;
    for (auto pending_chunk = pending_output_chunk.begin(); pending_chunk != pending_output_chunk.end();
            pending_chunk++) {
        finish_build_chunk(pending_chunk->first);
		tab_ = fixed_range_chunk_->GetRangeTab(pending_chunk->first);
		tab_->SetCompactionWorking(true);
    }

	DBG_PRINT("Insert kv pairs again");
	pending_output_chunk.clear();
	last_prefix = nullptr;
	last_chunk = nullptr;
	
	for (int i = 20; i < 60; i++) {
		char key[17];
        sprintf(key, "%016lu", key_gen.Next());
		key[16] = 0;
        InternalKey ikey;
        ikey.Set(Slice(key, 17), static_cast<uint64_t>(1000 + i), kTypeValue);
        Slice i_user_key = ikey.user_key();
        std::string now_prefix = (*foptions_->prefix_extractor_)(i_user_key.data(),
               i_user_key.size());
        if (now_prefix == last_prefix && last_chunk != nullptr) {
            last_chunk->Insert(ikey.Encode(), value_gen.Generate(value_size_));
        } else {
            BuildingChunk* now_chunk = nullptr;
            auto chunk_found = pending_output_chunk.find(now_prefix);
            if (chunk_found == pending_output_chunk.end()) {
                DBG_PRINT("prepare to create new chunk: last_prefix[%s], now_prefix[%s]", 
                    last_prefix.c_str(), now_prefix.c_str());
                auto new_chunk = new BuildingChunk(foptions_->filter_policy_, now_prefix);
                DBG_PRINT("end create chunk");
                pending_output_chunk[now_prefix] = new_chunk;
                now_chunk = new_chunk;
            } else {
                now_chunk = chunk_found->second;
            }

            now_chunk->Insert(ikey.Encode(), value_gen.Generate(value_size_));
            last_chunk = now_chunk;
            last_prefix = now_prefix;

                //DBG_PRINT("end insert");
        }
        insert_key_2.emplace_back(key, 17);
	}

	for (auto pending_chunk : pending_output_chunk) {
        fixed_range_chunk_->RangeExistsOrCreat(pending_chunk.first);
    }

    // test Get
    DBG_PRINT("prepare to test Get method");
    for (auto key : insert_key) {
        LookupKey lkey(Slice(key), 100);
        string *get_value = new string();
        Status s = fixed_range_chunk_->Get(icmp_, lkey, get_value);
        ASSERT_OK(s);
        delete get_value;
    }

	DBG_PRINT("prepare to test Get method second time!");
	int times_ = 0;
	for (auto key : insert_key_2) {
		if (times_ == 10) {
			tab_->CleanUp();
			tab_->SetCompactionWorking(false);
		}
		times_++;
		LookupKey lkey(Slice(key), 100);
        string *get_value = new string();
        Status s = fixed_range_chunk_->Get(icmp_, lkey, get_value);
        ASSERT_OK(s);
        delete get_value;
	}

    DBG_PRINT("end of TEST");
}


}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}