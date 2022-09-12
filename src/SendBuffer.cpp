#include <stdio.h>

#include "Buffer.h"

SendBuffer::SendBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    std::cout << "[Buffer] Allocating: " << getBufferSize() << " bytes for RDMA Send-Buffer" << std::endl;
}

/**
 * @brief DEPRECATED For loading non-packaged data.
 *
 * @param data
 * @param writePtr
 * @param totalSize
 * @param currentSize
 * @param package_number
 * @param dataType
 * @param packageID
 */
void SendBuffer::loadData(const char* data, char* writePtr, uint64_t totalSize, uint64_t currentSize, uint64_t package_number, uint64_t dataType, uint64_t packageID) const {
    memcpy(writePtr, &packageID, sizeof(packageID));
    memcpy(writePtr + 8, &currentSize, sizeof(currentSize));
    memcpy(writePtr + 16, &package_number, sizeof(package_number));
    memcpy(writePtr + 24, &dataType, sizeof(dataType));
    memcpy(writePtr + 32, &totalSize, sizeof(totalSize));
    memcpy(writePtr + package_t::metaDataSize(), data, currentSize);
}

/**
 * @brief           Function for copying a package into the send buffer.
 *
 * @param writePtr  Pointer where to start writing into this send buffer.
 * @param p         The package that should be loaded.
 */
void SendBuffer::loadPackage(char* writePtr, package_t* p) const {
    memcpy(writePtr, &p->get_header(), sizeof(package_t::header_t));
    memcpy(writePtr + p->metaDataSize() + p->get_header().payload_start, p->get_payload(), p->get_header().current_payload_size);
}

/**
 * @brief           For loading the application specifig meta data.
 *
 * @param writePtr  Where to start writing in the send buffer.
 * @param p         The package it the meta data belongs to.
 * @param meta      The application specific meta data.
 */
void SendBuffer::loadAppMetaData(char* writePtr, package_t* p, char* meta) const {
    memcpy(writePtr + sizeof(package_t::header_t), meta, p->get_header().payload_start);
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

    post_request(packageSize, IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, writePtr, wrID);
}

/**
 * @brief               Special function for sending a reconfigure request. Can possibly be eliminated when sended as package.
 *
 * @param recData       The information for reconfiguring.
 * @param receivePtr    The address of the remote buffer where the package will be written to.
 * @param receiveRkey   The r-key of the remote buffer where the package will be written to.
 * @param qp            The query-pair used for sending. receivePtr and receiveRkey must belong to this qp.
 */
void SendBuffer::sendReconfigure(reconfigure_data& recData, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp) const {
    mempcpy(buf, &recData, sizeof(reconfigure_data));

    post_request(sizeof(reconfigure_data), IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, buf, 1);
}