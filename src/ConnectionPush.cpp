#include "Connection.hpp"
#include "ConnectionManager.h"

ConnectionPush::ConnectionPush(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId) : Connection(_config, _bufferConfig, _localConId) {
    // for resetting the buffer -> this is needed for the callbacks as they do not have access to the necessary structures
    reset_buffer = [this](const size_t i) -> void {
        // ownReceiveBuffer[i]->clearBuffer();
        setReceiveOpcode(i, rdma_ready, true);
    };

    // for the receiving threads -> check whether a RB is ready to be consumed
    check_receive = [this](std::atomic<bool> *abort, size_t tid, size_t thrdcnt) -> void {
        LOG_INFO("[check_receive] Starting monitoring thread " << tid + 1 << "/" << +thrdcnt << " for receiving on connection!" << std::endl);
        size_t metaSizeHalf = metaInfoReceive.size() / 2;

        // currently this works with (busy) waiting -> TODO: conditional variable with wait
        while (!*abort) {
            // std::this_thread::sleep_for(1000ms);
            for (size_t i = tid; i < metaSizeHalf; i += thrdcnt) {
                if (ConnectionManager::getInstance().hasCallback(metaInfoReceive[i])) {
                    // LOG_DEBUG1("[Connection] Invoking custom callback for code " << (size_t)metaInfoReceive[i] << std::endl);

                    // Handle the call
                    auto cb = ConnectionManager::getInstance().getCallback(metaInfoReceive[i]);
                    cb(localConId, ownReceiveBuffer[i].get(), std::bind(reset_buffer, i));

                    continue;
                }
                switch (metaInfoReceive[i]) {
                    case rdma_no_op:
                    case rdma_ready:
                    case rdma_working: {
                        continue;
                    }; break;
                    case rdma_data_finished: {
                        consumeData(i);
                    }; break;
                    case rdma_reconfigure: {
                        auto recFunc = [this](size_t index) {
                            receiveReconfigureBuffer(index);
                        };
                        setReceiveOpcode(i, rdma_reconfiguring, false);
                        std::thread(recFunc, i).detach();
                    } break;
                    case rdma_reconfigure_ack: {
                        ackReconfigureBuffer(i);
                    } break;
                    case rdma_shutdown: {
                        auto shutdown = []() { std::raise(SIGUSR1); };
                        std::thread(shutdown).detach();
                    }; break;
                    default: {
                        continue;
                    }; break;
                }
            }
        }

        LOG_INFO("[check_receive] Ending thread " << tid + 1 << "/" << +thrdcnt << " through global abort." << std::endl);
    };

    // for the sending threads -> check whether a SB is ready to be send
    check_send = [this](std::atomic<bool> *abort, size_t tid, size_t thrdcnt) -> void {
        LOG_INFO("[check_send] Starting monitoring thread " << tid + 1 << "/" << +thrdcnt << " for sending on connection!" << std::endl);
        size_t metaSizeHalf = metaInfoSend.size() / 2;

        while (!*abort) {
            // std::this_thread::sleep_for(100ms);
            for (size_t i = tid; i < metaSizeHalf; i += thrdcnt) {
                switch (metaInfoSend[i]) {
                    case rdma_no_op:
                    case rdma_ready:
                    case rdma_working: {
                        continue;
                    }; break;
                    case rdma_ready_to_send: {
                        __sendData(i);
                    }; break;
                    default: {
                        continue;
                    }; break;
                }
            }
        }

        LOG_INFO("[check_send] Ending thread " << tid + 1 << "/" << +thrdcnt << " through global abort." << std::endl);
    };

    init();
}

/**
 * @brief                   Function for distributing the data to send on the available SBs. The real sending process is triggered by the opcode and done in an other function.
 *
 * @param data              Pointer to the start of the payload data that should be sent.
 * @param dataSize          The size of the whole payload data that should be sent.
 * @param appMetaData       Pointer to the application specific meta data that is written into each package.
 * @param appMetaDataSize   The size of the application specific meta data in bytes.
 * @param opcode            The opcode that should be written to remote for every package.
 * @return int              Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int ConnectionPush::sendData(char *data, size_t dataSize, char *appMetaData, size_t appMetaDataSize, uint8_t opcode) {
    int nextFreeSend;

    uint64_t remainingSize = dataSize;                                                                                                   // Whats left to write
    uint64_t maxPayloadSize = bufferConfig.size_own_send - package_t::metaDataSize() - appMetaDataSize;                                  // As much as we can fit into the SB excluding metadata
    uint64_t maxDataToWrite = remainingSize <= maxPayloadSize ? remainingSize : (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);  // Only write full 64bit elements -- should be adjusted to underlying datatype, e.g. float or uint8_t
    uint64_t packageID = generatePackageID();                                                                                            // Some randomized identifier

    size_t packageCounter = 0;
    package_t package(packageID, maxDataToWrite, packageCounter, 0, dataSize, appMetaDataSize, data);

    while (remainingSize > 0) {
        nextFreeSend = findNextFreeSendAndBlock();
        auto &sb = ownSendBuffer[nextFreeSend];

        if (remainingSize < maxDataToWrite) {
            maxDataToWrite = remainingSize;
            package.setCurrentPackageSize(remainingSize);
        }

        package.setCurrentPackageNumber(packageCounter++);

        sb->loadPackage(sb->getBufferPtr(), &package, appMetaData);

        sb->sendOpcode = opcode;

        package.advancePayloadPtr(maxDataToWrite);

        setSendOpcode(nextFreeSend, rdma_ready_to_send, false);

        remainingSize -= maxDataToWrite;
    }

    return remainingSize == 0 ? 0 : 1;
}

/**
 * @brief The actual sending process. It assumes that the data is already in the indicated SB and is called when the corresponding opcode is set.
 *
 * @param index The local index of the SB.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int ConnectionPush::__sendData(const size_t index) {
    setSendOpcode(index, rdma_working, false);
    auto &sb = ownSendBuffer[index];
    int nextFreeRec = findNextFreeReceiveAndBlock();

    sb->sendPackage(res.remote_receive_buffer[nextFreeRec], res.remote_receive_rkeys[nextFreeRec], res.dataQp, sb->getBufferPtr(), 10 * index + nextFreeRec);
    uint64_t wrId = pollCompletion<CompletionType::useDataCq>();

    auto &doneSb = ownSendBuffer[wrId / 10];

    setReceiveOpcode((wrId % 10) + (metaInfoReceive.size() / 2), doneSb->sendOpcode, doneSb->sendOpcode != rdma_ready);  // do not send opcode if rdma_ready -> throughput test
    setSendOpcode(wrId / 10, rdma_ready, false);

    return 0;
}

/**
 * @brief Copying the data from the RB into local memory and set it free again.
 *
 * @param index The RB index where the data is located (or should be read into).
 */
void ConnectionPush::consumeData(const size_t index) {
    setReceiveOpcode(index, rdma_working, false);

    char *ptr = ownReceiveBuffer[index]->getBufferPtr();

    package_t::header_t *header = reinterpret_cast<package_t::header_t *>(ptr);

    LOG_DEBUG2(header->id << "\t" << header->total_data_size << "\t" << header->current_payload_size << "\t" << header->package_number << std::endl);

    uint64_t *localPtr = reinterpret_cast<uint64_t *>(malloc(header->current_payload_size));
    memset(localPtr, 0, header->current_payload_size);
    memcpy(localPtr, ptr + package_t::metaDataSize(), header->current_payload_size);

    free(localPtr);

    setReceiveOpcode(index, rdma_ready, true);
}

/**
 * @brief Reconfiguration of the local buffer setup to the (possibly) new setup. Note: trying to be as lazy as possible, there are only changes if the configuration changed.
 *
 * @param bufConfig The new buffer configuration that should be applied.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int ConnectionPush::sendReconfigureBuffer(buffer_config_t &bufConfig) {
    BufferConnectionData bufConData = reconfigureBuffer(bufConfig);

    sendData(reinterpret_cast<char *>(&bufConData), sizeof(BufferConnectionData), nullptr, 0, rdma_reconfigure);

    std::unique_lock<std::mutex> reconfigureLock(reconfigureMutex);
    reconfigureCV.wait(reconfigureLock);

    return 0;
}

/**
 * @brief Receiving the task to reconfigure. Apply the given information to the own buffer structure.
 *
 * @param index The index of the RB where the new buffer information is stored.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int ConnectionPush::receiveReconfigureBuffer(const uint8_t index) {
    char *ptr = ownReceiveBuffer[index]->getBufferPtr() + package_t::metaDataSize();

    BufferConnectionData *bufConData = reinterpret_cast<BufferConnectionData *>(malloc(sizeof(BufferConnectionData)));
    memcpy(bufConData, ptr, sizeof(BufferConnectionData));
    bufConData->bufferConfig = invertBufferConfig(bufConData->bufferConfig);

    setReceiveOpcode(index, rdma_ready, true);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (bufConData->receiveBuffers[i] != 0) {
            res.remote_receive_buffer.push_back(bufConData->receiveBuffers[i]);
            res.remote_receive_rkeys.push_back(bufConData->receiveRkeys[i]);
        }
    }

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (bufConData->sendBuffers[i] != 0) {
            res.remote_send_buffer.push_back(bufConData->sendBuffers[i]);
            res.remote_send_rkeys.push_back(bufConData->sendRkeys[i]);
        }
    }

    BufferConnectionData reconfData = reconfigureBuffer(bufConData->bufferConfig);

    free(bufConData);

    sendData(reinterpret_cast<char *>(&reconfData), sizeof(BufferConnectionData), nullptr, 0, rdma_reconfigure_ack);

    return 0;
}

/**
 * @brief Reconfiguration of the local buffer setup to the (possibly) new setup. Note: trying to be as lazy as possible, there are only changes if the configuration changed.
 *
 * @param bufConfig The new buffer configuration that should be applied.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
void ConnectionPush::ackReconfigureBuffer(size_t index) {
    char *ptr = ownReceiveBuffer[index]->getBufferPtr() + package_t::metaDataSize();

    BufferConnectionData *recData = reinterpret_cast<BufferConnectionData *>(malloc(sizeof(BufferConnectionData)));
    memcpy(recData, ptr, sizeof(BufferConnectionData));
    recData->bufferConfig = invertBufferConfig(recData->bufferConfig);

    setReceiveOpcode(index, rdma_ready, true);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (recData->receiveBuffers[i] != 0) {
            res.remote_receive_buffer.push_back(recData->receiveBuffers[i]);
            res.remote_receive_rkeys.push_back(recData->receiveRkeys[i]);
        }
    }

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (recData->sendBuffers[i] != 0) {
            res.remote_send_buffer.push_back(recData->sendBuffers[i]);
            res.remote_send_rkeys.push_back(recData->sendRkeys[i]);
        }
    }

    std::unique_lock<std::mutex> reconfigureLock(reconfigureMutex);
    reconfigureCV.notify_all();

    setReceiveOpcode(index, rdma_ready, true);
}

ConnectionPush::~ConnectionPush() {
    closeConnection();
}
