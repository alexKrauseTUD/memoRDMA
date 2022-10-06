#ifndef MEMORDMA_RDMA_BUFFER
#define MEMORDMA_RDMA_BUFFER

#include <infiniband/verbs.h>
#include <inttypes.h>
#include <stdio.h>

#include <string>
#include <vector>

#include "PackageManager.hpp"
#include "common.h"


class Buffer {
   public:
    explicit Buffer(std::size_t _bufferSize);
    ~Buffer();

    std::size_t getBufferSize() const;
    char *getBufferPtr() const;
    struct ibv_mr *getMrPtr() const;

    void clearBuffer() const;

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd);
    
    int postRequest(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void *writePtr, uint64_t wrID) const;

   private:
    int resourcesDestroy();

   protected:
    char *buf;
    struct ibv_mr *mr;
    std::size_t bufferSize;
};

class SendBuffer : public Buffer {
   public:
    uint8_t sendOpcode = rdma_no_op;  // the opcode that is sent at the end of a data sending process to indicate the end of the process

    explicit SendBuffer(std::size_t _bufferSize);

    void loadPackage(char *writePtr, package_t *p, char *meta) const;
    void sendPackage(uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void *writePtr, uint64_t wrID) const;

    void print() const;

   private:
};

class ReceiveBuffer : public Buffer {
   public:
    explicit ReceiveBuffer(std::size_t _bufferSize);

    std::size_t getMaxPayloadSize() const;

   private:
};

#endif  // MEMORDMA_RDMA_BUFFER