#pragma once

#include <stdint.h>
#include <unistd.h>

namespace rocksdb{

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

static inline int
file_exists(char const *file) {
    return access(file, F_OK);
}

}
