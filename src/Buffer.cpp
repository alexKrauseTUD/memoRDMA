#include "Buffer.h"

#include <stdio.h>

Buffer::Buffer(std::size_t _bufferSize) : bufferSize{_bufferSize} {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] WARNING - bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }
    std::cout << "[Buffer] Creating new buffer with buffer size: " << bufferSize << std::endl;

    // a buffer to hold the data
    std::cout << "[Buffer] Allocating: " << bufferSize << " bytes for RDMA buffer" << std::endl;
    buf = (char*)calloc(1, bufferSize);
    assert(buf != NULL);

    memset(buf, 0, bufferSize);
}

Buffer::~Buffer() {
    resources_destroy();
}

// Cleanup and deallocate all resources used
int Buffer::resources_destroy() {
    // FIXME: ;)
    return 0;
}

std::size_t Buffer::getBufferSize() {
    return bufferSize;
}
