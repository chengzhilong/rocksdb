#ifndef CHUNKBLK_H
#define CHUNKBLK_H

#include <stdint.h>
#include <stddef.h>

namespace rocksdb {

/* *
 * --------------
 * | bloom data | // bloom_bits
 * --------------
 * | chunk  len | // sizeof(size_t)
 * --------------
 * | chunk data | // chunkLen
 * --------------
 * */
class ChunkBlk {
public:
    explicit ChunkBlk(size_t bloom_bytes, size_t offset, size_t chunkLen)
            : bloom_bytes_(bloom_bytes), offset_(offset), chunkLen_(chunkLen) {

    }

    // return the offset of the chunk's data
    size_t getDatOffset() {
        return offset_ + bloom_bytes_ + sizeof(uint64_t) * 2;
    }

    size_t bloom_bytes_;
    size_t offset_; // offset of bloom filter in range buffer
    size_t chunkLen_;

    // kv data start at offset + CHUNK_BLOOM_FILTER_SIZE + sizeof(chunLen)
};

} // namespace rocksdb

#endif // CHUNKBLK_H