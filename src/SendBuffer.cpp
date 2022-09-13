#include <stdio.h>

#include "Buffer.h"

SendBuffer::SendBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    std::cout << "[Buffer] Allocating: " << getBufferSize() << " bytes for RDMA Send-Buffer" << std::endl;
}

/**
 * @brief           Function for copying a package into the send buffer.
 *
 * @param writePtr  Pointer where to start writing into this send buffer.
 * @param p         The package that should be loaded.
 */
void SendBuffer::loadPackage(char* writePtr, package_t* p, char* meta) const {
    memcpy(writePtr, &p->get_header(), sizeof(package_t::header_t));
    memcpy(writePtr + sizeof(package_t::header_t), meta, p->get_header().payload_start);
    memcpy(writePtr + p->metaDataSize() + p->get_header().payload_start, p->get_payload(), p->get_header().current_payload_size);
}

/**
 * @brief               For sending the already loaded package.
 *
 * @param receivePtr    The address of the remote buffer where the package will be written to.
 * @param receiveRkey   The r-key of the remote buffer where the package will be written to.
 * @param qp            The query-pair used for sending. receivePtr and receiveRkey must belong to this qp.
 * @param writePtr      The local pointer of the send buffer from whereon the data is written.
 * @param wrID          The ID for the write request. This is practically uninportant for us.
 */
void SendBuffer::sendPackage(uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp, void* writePtr, uint64_t wrID) const {
    package_t::header_t* header = reinterpret_cast<package_t::header_t*>(writePtr);
    auto packageSize = sizeof(package_t::header_t) + header->current_payload_size + header->payload_start;

    postRequest(packageSize, IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, writePtr, wrID);
}