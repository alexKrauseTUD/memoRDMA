#include <stdio.h>

#include "Buffer.h"

ReceiveBuffer::ReceiveBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    std::cout << "[Buffer] Allocating: " << getBufferSize() << " bytes for RDMA Receive-Buffer" << std::endl;
}

std::size_t ReceiveBuffer::getMaxPayloadSize() const {
    return getBufferSize() - package_t::metaDataSize();
}