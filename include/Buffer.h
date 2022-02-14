#ifndef MEMORDMA_RDMA_BUFFER
#define MEMORDMA_RDMA_BUFFER

#include <infiniband/verbs.h>
#include <inttypes.h>
#include <stdio.h>

#include <string>

#include "common.h"
#include "package_manager.hpp"
#include "util.h"

// // structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint64_t addr;    // buffer address
    uint32_t rkey;    // remote key
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // GID
} __attribute__((packed));

class Buffer {
   public:
    explicit Buffer(std::size_t _bufferSize);
    ~Buffer();

    struct ibv_mr *mr;
    char *buf;
    std::size_t bufferSize;
    std::size_t getBufferSize();

    void clearCode();
    void clearBuffer();

    int poll_completion();

   private:
    int resources_destroy();
};

class SendBuffer : public Buffer {
   public:
    explicit SendBuffer(std::size_t _bufferSize = 1024 * 1024 * 2);

    void setSendData(std::string s);
    void setSendData(uint64_t *data, uint64_t totalSize, uint64_t currentSize);
    void setCommitCode(rdma_handler_communication opcode);

    void setPackageHeader(package_t *p);
    void sendPackage(package_t *p, rdma_handler_communication opcode);

    std::size_t getMaxWriteSize();

    int post_send(int len, ibv_wr_opcode opcode, size_t offset = 0);

    void print() const;

   private:
};

class ReceiveBuffer : public Buffer {
   public:
    explicit ReceiveBuffer(std::size_t _bufferSize = 1024 * 1024 * 2 + 128);

    void consumeData();

    int modify_qp_to_init(struct config_t &config, struct ibv_qp *qp);
    int modify_qp_to_rtr(struct config_t &config, struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
    int modify_qp_to_rts(struct ibv_qp *qp);

    int post_receive();

   private:
};

#endif  // MEMORDMA_RDMA_BUFFER