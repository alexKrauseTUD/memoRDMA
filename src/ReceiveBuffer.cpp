#include <stdio.h>

#include "Buffer.h"

ReceiveBuffer::ReceiveBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    std::cout << "[Buffer] Allocating: " << bufferSize << " bytes for RDMA Receive-Buffer" << std::endl;
}

std::size_t ReceiveBuffer::getMaxPayloadSize() {
    return bufferSize - package_t::metaDataSize();
}