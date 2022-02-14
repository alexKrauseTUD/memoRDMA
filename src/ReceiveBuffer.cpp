#include <stdio.h>

#include "Buffer.h"

ReceiveBuffer::ReceiveBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] WARNING - bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }
    std::cout << "[Buffer] Creating new receive-buffer with buffer size: " << bufferSize << std::endl;
}

int ReceiveBuffer::post_receive() {
    return 0;
}