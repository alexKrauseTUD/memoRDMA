
#include "Buffer.h"

#include <stdio.h>

#include <iostream>

#include "Logger.h"
#include "Utility.h"
#include <assert.h>

Buffer::Buffer(std::size_t _bufferSize) : bufferSize{_bufferSize} {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] Parameter bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }

    // a buffer to hold the data
    buf = (char*)calloc(1, bufferSize);
    assert(buf != NULL);
}

Buffer::~Buffer() {
    resourcesDestroy();
}

/**
 * @brief Cleanup and deallocate all resources of this buffer.
 *
 * @return int
 */
int Buffer::resourcesDestroy() {
    ibv_dereg_mr(mr);
    free(buf);
    // FIXME: ;)
    return 0;
}

/**
 * @brief For getting read access to the size of the buffer (in bytes).
 *
 * @return const std::size_t The size of the buffer in bytes.
 */
std::size_t Buffer::getBufferSize() const {
    return bufferSize;
}

/**
 * @brief For getting read access to the private buffer-pointer.
 *
 * @return char* The char pointer (byte addressing) of the buffer.
 */
char* Buffer::getBufferPtr() const {
    return buf;
}

/**
 * @brief For getting read access to the private buffer-mr.
 *
 * @return struct ibv_mr* The pointer to the mr of the buffer.
 */
struct ibv_mr* Buffer::getMrPtr() const {
    return mr;
}

/**
 * @brief Resetting the buffer to complete 0. This is technically not needed, but we do it on creation at least.
 *
 */
void Buffer::clearBuffer() const {
    memset(buf, 0, bufferSize);
}

/**
 * @brief Registering the Memory Region for the buffer.
 *
 * @param pd The protection domain where the mr should be registered.
 * @return struct ibv_mr* A pointer to the registered memory region object.
 */
struct ibv_mr* Buffer::registerMemoryRegion(struct ibv_pd* pd) {
    mr = ibv_reg_mr(pd, buf, bufferSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    assert(mr != NULL);
    return mr;
}

/**
 * @brief               This function will create and post a send work request.
 *
 * @param len           The total length of the data in bytes.
 * @param opcode        The operation code for RDMA communication. In our case this is mostly IBV_WR_RDMA_WRITE or IBV_WR_RDMA_READ.
 * @param receivePtr    The pointer for the remote buffer. Indicates where the data will be written. Needed for IBV_WR_RDMA_WRITE and IBV_WR_RDMA_READ.
 * @param receiveRkey   The r-key of the remote buffer. Needed for IBV_WR_RDMA_WRITE and IBV_WR_RDMA_READ.
 * @param qp            The query-pair used for sending. receivePtr and receiveRkey must belong to this qp.
 * @param writePtr      The local pointer for the used buffer. Data is written from here (IBV_WR_RDMA_WRITE) or written to this buffer (IBV_WR_RDMA_READ).
 * @param wrID          The ID for the write request. This is practically uninportant for us.
 * @return int          Value indicating success (0) or failure (1). Note that this currently defaults to success. Failure is catched before.
 */
int Buffer::postRequest(int len, ibv_wr_opcode opcode, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp, void* writePtr, uint64_t wrID) const {
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

    using namespace memordma;
    if (send_result != 0) {
        LOG_ERROR("Send_result: " << send_result << std::endl);
    }
    Utility::checkOrDie(send_result);

    return 0;
}