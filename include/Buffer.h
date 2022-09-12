#ifndef MEMORDMA_RDMA_BUFFER
#define MEMORDMA_RDMA_BUFFER

#include <infiniband/verbs.h>
#include <inttypes.h>
#include <stdio.h>

#include <string>
#include <vector>

#include "PackageManager.hpp"
#include "common.h"
#include "util.h"

// // structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint64_t meta_receive_buf;
    uint32_t meta_receive_rkey;
    uint64_t meta_send_buf;
    uint32_t meta_send_rkey;
    uint32_t receive_num;
    uint32_t send_num;
    uint64_t receive_buf[8]{0, 0, 0, 0, 0, 0, 0, 0};   // buffer address
    uint32_t receive_rkey[8]{0, 0, 0, 0, 0, 0, 0, 0};  // remote key
    uint64_t send_buf[8]{0, 0, 0, 0, 0, 0, 0, 0};      // buffer address
    uint32_t send_rkey[8]{0, 0, 0, 0, 0, 0, 0, 0};     // remote key
    buffer_config_t buffer_config;
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // GID
} __attribute__((packed));

struct reconfigure_data {
    buffer_config_t buffer_config;
    uint64_t send_buf[8]{0, 0, 0, 0, 0, 0, 0, 0};      // buffer address
    uint32_t send_rkey[8]{0, 0, 0, 0, 0, 0, 0, 0};     // remote key
    uint64_t receive_buf[8]{0, 0, 0, 0, 0, 0, 0, 0};   // buffer address
    uint32_t receive_rkey[8]{0, 0, 0, 0, 0, 0, 0, 0};  // remote key
};

class Buffer {
   public:
    explicit Buffer(std::size_t _bufferSize);
    ~Buffer();

    std::size_t getBufferSize() const;
    char *getBufferPtr() const;
    struct ibv_mr *getMrPtr() const;
    void clearBuffer() const;

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd);

    int post_request(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void *writePtr, uint64_t wrID) const;

   private:
    int resources_destroy();

   protected:
    char *buf;
    struct ibv_mr *mr;
    std::size_t bufferSize;
};

class SendBuffer : public Buffer {
   public:
    uint8_t sendOpcode = rdma_data_finished;  // the opcode that is sent at the end of a data sending process to indicate the end of the process

    explicit SendBuffer(std::size_t _bufferSize);

    void loadData(const char *data, char *writePtr, uint64_t totalSize, uint64_t currentSize, uint64_t package_number, uint64_t dataType, uint64_t packageID) const;

    void loadPackage(char *writePtr, package_t *p) const;
    void loadAppMetaData(char *writePtr, package_t *p, char *meta) const;
    void sendPackage(uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void *writePtr, uint64_t wrID) const;

    void sendReconfigure(reconfigure_data &recData, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp) const;

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