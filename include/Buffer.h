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
    uint64_t receive_buf[7]{0, 0, 0, 0, 0, 0, 0};   // buffer address
    uint32_t receive_rkey[7]{0, 0, 0, 0, 0, 0, 0};  // remote key
    buffer_config_t buffer_config;
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

    char *bufferPtr();

    void clearCode();
    void clearBuffer();

    int poll_completion();

   private:
    int resources_destroy();
};

class SendBuffer : public Buffer {
   public:
    explicit SendBuffer(std::size_t _bufferSize);

    void loadData(const char *data, char *writePtr, uint64_t totalSize, uint64_t currentSize, uint64_t package_number, uint64_t dataType, uint64_t packageID);
    void sendData(std::string s, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp);
    void sendData(uint64_t *data, uint64_t totalSize, uint64_t currentSize, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp);
    int post_send(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void* writePtr, uint64_t wrID);
    //  void setCommitCode(rdma_handler_communication opcode);

    void loadPackage(char *writePtr, package_t *p);
    void sendPackage(package_t *p, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp *qp, void* writePtr, uint64_t wrID);

    //  std::size_t getMaxWriteSize();

    void print() const;

   private:
};

class ReceiveBuffer : public Buffer {
   public:
    explicit ReceiveBuffer(std::size_t _bufferSize);

    void consumeData();

    std::size_t getMaxPayloadSize();

    int modify_qp_to_init(struct config_t &config, struct ibv_qp *qp);
    int modify_qp_to_rtr(struct config_t &config, struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
    int modify_qp_to_rts(struct ibv_qp *qp);

    int post_receive();

   private:
};

#endif  // MEMORDMA_RDMA_BUFFER