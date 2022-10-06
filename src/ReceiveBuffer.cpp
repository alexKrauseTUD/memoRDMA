#include <stdio.h>

#include "Buffer.h"
#include "Logger.h"

using namespace memordma;

ReceiveBuffer::ReceiveBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    LOG_INFO("[ReceiveBuffer] Allocating: " << getBufferSize() << " bytes for RDMA Receive-Buffer" << std::endl);
}

std::size_t ReceiveBuffer::getMaxPayloadSize() const {
    return getBufferSize() - package_t::metaDataSize();
}