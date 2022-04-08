#include <stdio.h>

#include "Buffer.h"

SendBuffer::SendBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] WARNING - bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }
    std::cout << "[Buffer] Creating new send-buffer with buffer size: " << bufferSize << std::endl;
}

// This function will create and post a send work request.
int SendBuffer::post_send(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp, void* writePtr, uint64_t wrID) {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;

    // prepare the scatter / gather entry
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)writePtr;
    sge.length = len;
    sge.lkey = mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));

    sr.next = NULL;
    sr.wr_id = wrID;
    sr.sg_list = &sge;

    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = IBV_SEND_SIGNALED;
    // sr.send_flags = IBV_SEND_INLINE;

    if (opcode != IBV_WR_SEND) {
        sr.wr.rdma.remote_addr = receivePtr;
        sr.wr.rdma.rkey = receiveRkey;
    }

    auto send_result = ibv_post_send(qp, &sr, &bad_wr);

    if (send_result != 0) {
        std::cout << "ERROR " << send_result << std::endl;
    }
    CHECK(send_result);

    return 0;
}

void SendBuffer::loadData(const char* data, char* writePtr, uint64_t totalSize, uint64_t currentSize, uint64_t package_number, uint64_t dataType, uint64_t packageID) {
    memcpy(writePtr, &packageID, sizeof(packageID));
    memcpy(writePtr + 8, &currentSize, sizeof(currentSize));
    memcpy(writePtr + 16, &package_number, sizeof(package_number));
    memcpy(writePtr + 24, &dataType, sizeof(dataType));
    memcpy(writePtr + 32, &totalSize, sizeof(totalSize));
    memcpy(writePtr + META_INFORMATION_SIZE, data, currentSize);
}

// void SendBuffer::sendData(std::string s, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp) {
//     strcpy(buf, s.c_str());
//     post_send(s.size() + 16, IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp);
//     // poll_completion();
// }

// void SendBuffer::sendData(uint64_t* data, uint64_t totalSize, uint64_t currentSize, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp) {
//     clearBuffer();
//     memcpy(buf, &totalSize, sizeof(totalSize));
//     memcpy(buf + 8, &currentSize, sizeof(currentSize));
//     memcpy(buf + 16, data, currentSize);
//     post_send(currentSize + 16, IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp);
//     // poll_completion();
// }

void SendBuffer::loadPackage(char* writePtr, package_t* p) {
    memcpy(writePtr, &p->get_header(), sizeof(package_t::header_t));
    memcpy(writePtr + sizeof(package_t::header_t), p->get_payload(), p->get_header().current_payload_size);
}

void SendBuffer::sendPackage(package_t* p, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp, void* writePtr, uint64_t wrID) {
    post_send(p->packageSize(), IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, writePtr, wrID);
}

void SendBuffer::sendReconfigure(reconfigure_data& recData, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp) {
    auto p = buf;
    memcpy(p, &recData.buffer_config, sizeof(buffer_config_t));
    p += sizeof(buffer_config_t);
    memcpy(p, &recData.send_buf, sizeof(recData.send_buf));
    p += sizeof(recData.send_buf);
    memcpy(p, &recData.send_rkey, sizeof(recData.send_rkey));
    p += sizeof(recData.send_rkey);
    memcpy(p, recData.receive_buf.data(), sizeof(uintptr_t) * recData.receive_buf.size());
    p += sizeof(uintptr_t) * recData.receive_buf.size();
    memcpy(p, recData.receive_rkey.data(), sizeof(uintptr_t) * recData.receive_rkey.size());
    p += sizeof(uintptr_t) * recData.receive_rkey.size();

    post_send(p-buf, IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, buf, 1);
}