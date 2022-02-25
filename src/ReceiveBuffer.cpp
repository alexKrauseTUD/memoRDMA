#include <stdio.h>

#include "Buffer.h"

ReceiveBuffer::ReceiveBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
}

int ReceiveBuffer::post_receive() {
    return 0;
}

std::size_t ReceiveBuffer::getMaxPayloadSize() {
    return bufferSize - META_INFORMATION_SIZE;
}