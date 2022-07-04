#ifndef MEMORDMA_RDMA_BUFFER
#define MEMORDMA_RDMA_BUFFER

#include <infiniband/verbs.h>
#include <inttypes.h>
#include <stdio.h>

#include <string>
#include <vector>

#include "common.h"
#include "package_manager.hpp"
#include "util.h"

// // structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint64_t meta_buf;
    uint32_t meta_rkey;
    uint64_t send_buf;
    uint32_t send_rkey;
    uint32_t receive_num;
    uint64_t receive_buf[8]{0, 0, 0, 0, 0, 0, 0, 0};   // buffer address
    uint32_t receive_rkey[8]{0, 0, 0, 0, 0, 0, 0, 0};  // remote key
    buffer_config_t buffer_config;
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // GID
} __attribute__((packed));

struct reconfigure_data {
    buffer_config_t buffer_config;
    uintptr_t send_buf;
    uint32_t send_rkey;
    uint64_t receive_buf[8]{0, 0, 0, 0, 0, 0, 0, 0};   // buffer address
    uint32_t receive_rkey[8]{0, 0, 0, 0, 0, 0, 0, 0};  // remote key
};

class Buffer {
   public:
    explicit Buffer(std::size_t _bufferSize);
    ~Buffer();

    struct ibv_mr *mr;
    char *buf;
    std::size_t bufferSize;
    std::size_t getBufferSize();

    char *bufferPtr();

    void clearBuffer();

    int post_request(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void *writePtr, uint64_t wrID);

   private:
    int resources_destroy();
};

class SendBuffer : public Buffer {
   public:
    uint8_t metaInfo;

    explicit SendBuffer(std::size_t _bufferSize);

    void loadData(const char *data, char *writePtr, uint64_t totalSize, uint64_t currentSize, uint64_t package_number, uint64_t dataType, uint64_t packageID);

    void loadPackage(char *writePtr, package_t *p);
    void loadAppMetaData(char *writePtr, package_t *p, char *meta);
    void sendPackage(package_t *p, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void *writePtr, uint64_t wrID);

    void sendReconfigure(reconfigure_data &recData, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp);

    void print() const;

   private:
};

class ReceiveBuffer : public Buffer {
   public:
    explicit ReceiveBuffer(std::size_t _bufferSize);

    std::size_t getMaxPayloadSize();

   private:
};

#endif  // MEMORDMA_RDMA_BUFFER