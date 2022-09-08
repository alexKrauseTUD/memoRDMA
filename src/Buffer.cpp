#include "Buffer.h"

#include <stdio.h>

Buffer::Buffer(std::size_t _bufferSize) : bufferSize{_bufferSize} {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] WARNING - bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }

    // a buffer to hold the data
    buf = (char*)calloc(1, bufferSize);
    assert(buf != NULL);

    memset(buf, 0, bufferSize);
}

Buffer::~Buffer() {
    resources_destroy();
}

// Cleanup and deallocate all resources used
int Buffer::resources_destroy() {
    free(buf);
    // FIXME: ;)
    return 0;
}

std::size_t Buffer::getBufferSize() {
    return bufferSize;
}

char* Buffer::bufferPtr() {
    return buf;
}

void Buffer::clearBuffer() {
    memset(buf, 0, bufferSize);
}

// This function will create and post a send work request.
int Buffer::post_request(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp, void* writePtr, uint64_t wrID) {
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