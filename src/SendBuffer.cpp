#include <stdio.h>

#include "Buffer.h"

SendBuffer::SendBuffer(std::size_t _bufferSize) : Buffer(_bufferSize) {
    if (bufferSize % 2 != 0) {
        std::cout << "[Buffer] WARNING - bufferSize not divisible by 2. Rounding up!" << std::endl;
        ++bufferSize;
    }
    std::cout << "[Buffer] Creating new send-buffer with buffer size: " << bufferSize << std::endl;
}

// This function will create and post a send work request.
int SendBuffer::post_send(int len, ibv_wr_opcode opcode, size_t offset) {
    return 0;
}

void SendBuffer::setSendData(std::string s) {
}

void SendBuffer::setSendData(uint64_t* data, uint64_t totalSize, uint64_t currentSize) {
}

void SendBuffer::setCommitCode(rdma_handler_communication opcode) {
}

void SendBuffer::setPackageHeader(package_t* p) {
}

void SendBuffer::sendPackage(package_t* p, rdma_handler_communication opcode) {
}