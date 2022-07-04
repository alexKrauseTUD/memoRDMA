#include <stdio.h>

#include "Buffer.h"

SendBuffer::SendBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] WARNING - bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }
    std::cout << "[Buffer] Creating new send-buffer with buffer size: " << bufferSize << std::endl;
}

void SendBuffer::loadData(const char* data, char* writePtr, uint64_t totalSize, uint64_t currentSize, uint64_t package_number, uint64_t dataType, uint64_t packageID) {
    memcpy(writePtr, &packageID, sizeof(packageID));
    memcpy(writePtr + 8, &currentSize, sizeof(currentSize));
    memcpy(writePtr + 16, &package_number, sizeof(package_number));
    memcpy(writePtr + 24, &dataType, sizeof(dataType));
    memcpy(writePtr + 32, &totalSize, sizeof(totalSize));
    memcpy(writePtr + package_t::metaDataSize(), data, currentSize);
}

void SendBuffer::loadPackage(char* writePtr, package_t* p) {
    memcpy(writePtr, &p->get_header(), sizeof(package_t::header_t));
    memcpy(writePtr + p->metaDataSize() + p->get_header().payload_start, p->get_payload(), p->get_header().current_payload_size);
}

void SendBuffer::loadAppMetaData(char *writePtr, package_t* p, char *meta) {
    memcpy(writePtr + sizeof(package_t::header_t), meta, p->get_header().payload_start);
}

void SendBuffer::sendPackage(package_t* p, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp, void* writePtr, uint64_t wrID) {
    post_request(p->packageSize(), IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, writePtr, wrID);
}

void SendBuffer::sendReconfigure(reconfigure_data& recData, uint64_t receivePtr, uint32_t receiveRkey, ibv_qp* qp) {
    mempcpy(buf, &recData, sizeof(reconfigure_data));

    post_request(sizeof(reconfigure_data), IBV_WR_RDMA_WRITE, receivePtr, receiveRkey, qp, buf, 1);
}