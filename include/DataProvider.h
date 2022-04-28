#ifndef MEMORDMA_RDMA_DATA_PROVIDER_H
#define MEMORDMA_RDMA_DATA_PROVIDER_H

#include <inttypes.h>

class DataProvider {
   public:
    DataProvider();
    ~DataProvider();

    void generateDummyData(uint64_t size);

    uint64_t currSize;
    uint64_t* data;

   private:
    uint64_t xorshift64() {
        x64 ^= x64 << 13;
        x64 ^= x64 >> 7;
        x64 ^= x64 << 17;
        return x64;
    };

    uint64_t x64 = 88172645463325252ull;
};

#endif  // MEMORDMA_RDMA_DATA_PROVIDER_H