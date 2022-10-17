#include "FunctionalTests.hpp"

#include "Utility.h"

using namespace memordma;

FunctionalTests::FunctionalTests() {
    CallbackFunction receiveDataTransferTest = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) {
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->getBufferPtr());
        // Start of Payload
        uint64_t* data = reinterpret_cast<uint64_t*>(rcv_buffer->getBufferPtr() + sizeof(package_t::header_t) + head->payload_start);
        uint64_t packageId = head->id;

        std::lock_guard<std::mutex> lg(mapMutex);

        if (!receiveMap.contains(packageId)) {
            receiveMap.emplace(packageId, ReceiveData());
        }

        receiveMap[packageId].receivedBytes += head->current_payload_size;

        auto currentElements = head->current_payload_size / sizeof(uint64_t);

        for (size_t i = 0; i < currentElements; ++i) {
            receiveMap[packageId].result += data[i];
        }

        LOG_DEBUG2("Package id " << head->id << "\tPackage Number " << head->package_number << "\tReceived Bytes " << receiveMap[packageId].receivedBytes << "\tCurrent Result " << receiveMap[packageId].result << std::endl);

        reset_buffer();

        if (receiveMap[packageId].receivedBytes == dataSize) {
            ConnectionManager::getInstance().sendData(conId, reinterpret_cast<char*>(&receiveMap[packageId].result), sizeof(receiveMap[packageId].result), nullptr, 0, rdma_functional_test_ack, Strategies::push);
            receiveMap.erase(packageId);
        }
    };

    // We have to comply with the callback function signature but don't need the size_t conId here. Thus a GCC specific unused information.
    CallbackFunction dataTransferTestAck = [this](__attribute__((unused)) const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) {
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->getBufferPtr());
        // Start of Payload
        uint64_t* data = reinterpret_cast<uint64_t*>(rcv_buffer->getBufferPtr() + sizeof(package_t::header_t) + head->payload_start);

        ReceiveData rd = ReceiveData();
        uint64_t packageId = head->id;
        rd.result = data[0];

        reset_buffer();

        receiveMap.emplace(packageId, rd);

        if (receiveMap.size() == parallelExecutions) {
            std::lock_guard<std::mutex> resultWaitLock(resultWaitMutex);
            resultsArrived = true;
            resultWaitCV.notify_all();
        }
    };

    ConnectionManager::getInstance().registerCallback(static_cast<uint8_t>(rdma_functional_test), receiveDataTransferTest);
    ConnectionManager::getInstance().registerCallback(static_cast<uint8_t>(rdma_functional_test_ack), dataTransferTestAck);
}

uint8_t FunctionalTests::executeAllTests(bool lite) {
    uint8_t numberProblems = 0;
    auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string name = "";
    if (lite) {
        name = "FunctionalTestsLite";
    } else {
        name = "FunctionalTests";
    }
    std::stringstream logNameStream;
    logNameStream << name << std::put_time(std::localtime(&in_time_t), "_%Y-%m-%d-%H-%M-%S") << ".log";
    std::string logName = logNameStream.str();
    LOG_INFO("[" << name << "] Set name: " << logName << std::endl);

    std::ofstream out;
    out.open(logName, std::ios_base::app);

    if (lite) {
        // numberProblems += bufferReconfigurationTestLite(out);
        // numberProblems += dataTransferTestLite(out, Strategies::push);
        numberProblems += dataTransferTestLite(out, Strategies::pull);
    } else {
        // numberProblems += bufferReconfigurationTest(out);
        // numberProblems += dataTransferTest(out, Strategies::push);
        numberProblems += dataTransferTest(out, Strategies::pull);
    }

    out.close();

    LOG_INFO("\t[" << name << "]\tMet " << +numberProblems << " Problems while executing all tests.\n"
                   << std::endl);

    return numberProblems;
}

// FULL TESTS -> very time consuming

uint8_t FunctionalTests::dataTransferTest(std::ofstream& out, Strategies strat) {
    using namespace std::chrono_literals;

    uint8_t errorCount = 0;
    uint64_t* data = memordma::Utility::generateRandomDummyData<uint64_t>(elementCount);

    LOG_INFO("\t[DataTransferTest]\tGenerated " << elementCount << " Elements with a total size of ca. " << Utility::GetBytesReadable(dataSize) << std::endl);
    out << "[INFO]\t\t[DataTransferTest]\tGenerated " << elementCount << " Elements with a total size of ca. " << Utility::GetBytesReadable(dataSize) << std::endl;

    uint64_t checkSum = 0;
    for (size_t i = 0; i < elementCount; ++i) {
        checkSum += data[i];
    }

    LOG_INFO("\t[DataTransferTest]\tThe checksum of the generated data is\t" << +checkSum << std::endl);
    out << "[INFO]\t\t[DataTransferTest]\tThe checksum of the generated data is\t" << +checkSum << std::endl;

    for (uint8_t num_rb = 1; num_rb <= 8; ++num_rb) {
        for (uint8_t num_r_threads = 1; num_r_threads <= num_rb; ++num_r_threads) {
            for (uint8_t num_sb = 1; num_sb <= num_rb; ++num_sb) {
                for (uint8_t num_s_threads = 1; num_s_threads <= num_sb; ++num_s_threads) {
                    for (uint64_t bytes = 1ull << 10; bytes <= 1ull << 30; bytes <<= 1) {
                        buffer_config_t bufferConfig = {.num_own_send_threads = num_s_threads,
                                                        .num_own_receive_threads = num_r_threads,
                                                        .num_remote_send_threads = num_s_threads,
                                                        .num_remote_receive_threads = num_r_threads,
                                                        .num_own_receive = num_rb,
                                                        .size_own_receive = bytes,
                                                        .num_remote_receive = num_rb,
                                                        .size_remote_receive = bytes,
                                                        .num_own_send = num_sb,
                                                        .size_own_send = bytes,
                                                        .num_remote_send = num_sb,
                                                        .size_remote_send = bytes,
                                                        .meta_info_size = 16};

                        ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

                        LOG_INFO("\t[DataTransferTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl);
                        out << "[INFO]\t\t[DataTransferTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;

                        for (size_t i = 0; i < 5; ++i) {
                            receiveMap.clear();

                            auto sendLambda = [&]() {
                                ConnectionManager::getInstance().sendData(1, reinterpret_cast<char*>(data), dataSize, nullptr, 0, strat == Strategies::push ? rdma_functional_test : rdma_functional_test_pull, strat);
                            };

                            for (size_t k = 0; k < parallelExecutions; ++k) {
                                std::thread(sendLambda).detach();
                            }

                            std::unique_lock<std::mutex> resultWaitLock(resultWaitMutex);

                            resultWaitCV.wait_for(resultWaitLock, 10s);
                            if (!resultsArrived) {
                                errorCount++;
                                LOG_ERROR("\t[DataTransferTest]\tA result in iteration " << +i << " did not arrive within 10s!" << std::endl);
                                LOG_ERROR("\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl);
                                out << "[ERROR]\t\t[DataTransferTest]\tA result in iteration " << +i << " did not arrive within 10s!" << std::endl;
                                out << "\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                            } else {
                                resultsArrived = false;

                                for (auto it = receiveMap.begin(); it != receiveMap.end(); ++it) {
                                    auto currentResult = it->second.result;
                                    if (currentResult == checkSum) {
                                        LOG_SUCCESS("\t[DataTransferTest]\tThe Result in iteration " << +i << " matches the expected one." << std::endl);
                                        out << "[SUCCESS]\t[DataTransferTest]\tThe Result in iteration " << +i << " matches the expected one." << std::endl;
                                    } else {
                                        errorCount++;
                                        LOG_ERROR("\t[DataTransferTest]\tThe Result in iteration " << +i << " does not match the expected one. Expected: " << checkSum << "; Got: " << currentResult << std::endl);
                                        LOG_ERROR("\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl);
                                        out << "[ERROR]\t\t[DataTransferTest]\tThe Result in iteration " << +i << " does not match the expected one. Expected: " << checkSum << "; Got: " << currentResult << std::endl;
                                        out << "\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    LOG_INFO("\t[DataTransferTest]\tEnded with " << +errorCount << " Errors.\n"
                                                 << std::endl);
    out << "[INFO]\t\t[DataTransferTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    out << std::endl;

    free(data);

    return errorCount;
}

uint8_t FunctionalTests::bufferReconfigurationTest(std::ofstream& out) {
    uint8_t errorCount = 0;

    LOG_INFO("\t[BufferReconfigurationTest]\tStarting Buffer Reconfiguration Test." << std::endl);
    out << "[INFO]\t[BufferReconfigurationTest]\tStarting Buffer Reconfiguration Test." << std::endl;

    for (uint8_t num_rb = 1; num_rb <= 8; ++num_rb) {
        for (uint8_t num_r_threads = 1; num_r_threads <= num_rb; ++num_r_threads) {
            for (uint8_t num_sb = 1; num_sb <= num_rb; ++num_sb) {
                for (uint8_t num_s_threads = 1; num_s_threads <= num_sb; ++num_s_threads) {
                    for (uint64_t bytes = 1ull << 10; bytes <= 1ull << 30; bytes <<= 1) {
                        buffer_config_t bufferConfig = {.num_own_send_threads = num_s_threads,
                                                        .num_own_receive_threads = num_r_threads,
                                                        .num_remote_send_threads = num_s_threads,
                                                        .num_remote_receive_threads = num_r_threads,
                                                        .num_own_receive = num_rb,
                                                        .size_own_receive = bytes,
                                                        .num_remote_receive = num_rb,
                                                        .size_remote_receive = bytes,
                                                        .num_own_send = num_sb,
                                                        .size_own_send = bytes,
                                                        .num_remote_send = num_sb,
                                                        .size_remote_send = bytes,
                                                        .meta_info_size = 16};

                        LOG_INFO("\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl);
                        out << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;

                        ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);
                    }
                }
            }
        }
    }

    for (uint8_t num_rb = 8; num_rb >= 1; --num_rb) {
        for (uint8_t num_r_threads = num_rb; num_r_threads >= 1; --num_r_threads) {
            for (uint8_t num_sb = num_rb; num_sb >= 1; --num_sb) {
                for (uint8_t num_s_threads = num_sb; num_s_threads >= 1; --num_s_threads) {
                    for (uint64_t bytes = 1ull << 30; bytes >= 1ull << 10; bytes >>= 1) {
                        buffer_config_t bufferConfig = {.num_own_send_threads = num_s_threads,
                                                        .num_own_receive_threads = num_r_threads,
                                                        .num_remote_send_threads = num_s_threads,
                                                        .num_remote_receive_threads = num_r_threads,
                                                        .num_own_receive = num_rb,
                                                        .size_own_receive = bytes,
                                                        .num_remote_receive = num_rb,
                                                        .size_remote_receive = bytes,
                                                        .num_own_send = num_sb,
                                                        .size_own_send = bytes,
                                                        .num_remote_send = num_sb,
                                                        .size_remote_send = bytes,
                                                        .meta_info_size = 16};

                        LOG_INFO("\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl);
                        out << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;

                        ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);
                    }
                }
            }
        }
    }

    LOG_INFO("\t[BufferReconfigurationTest]\tEnded with " << +errorCount << " Errors.\n"
                                                          << std::endl);
    out << "[INFO]\t[BufferReconfigurationTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    out << std::endl;

    return errorCount;
}

// LITE TESTS -> significantly less time consuming but also less accurate

uint8_t FunctionalTests::dataTransferTestLite(std::ofstream& out, Strategies strat) {
    using namespace std::chrono_literals;

    uint8_t errorCount = 0;
    uint64_t* data = memordma::Utility::generateRandomDummyData<uint64_t>(elementCount);

    LOG_INFO("\t[DataTransferTest]\tGenerated " << elementCount << " Elements with a total size of ca. " << Utility::GetBytesReadable(dataSize) << std::endl);
    out << "[INFO]\t\t[DataTransferTest]\tGenerated " << elementCount << " Elements with a total size of ca. " << Utility::GetBytesReadable(dataSize) << std::endl;

    uint64_t checkSum = 0;
    for (size_t i = 0; i < elementCount; ++i) {
        checkSum += data[i];
    }

    LOG_INFO("\t[DataTransferTest]\tThe checksum of the generated data is\t" << +checkSum << std::endl);
    out << "[INFO]\t\t[DataTransferTest]\tThe checksum of the generated data is\t" << +checkSum << std::endl;

    for (uint8_t num_rb = 4; num_rb <= 8; ++num_rb) {
        for (uint64_t bytes = 1ull << 16; bytes <= 1ull << 21; bytes <<= 1) {
            buffer_config_t bufferConfig = {.num_own_send_threads = num_rb,
                                            .num_own_receive_threads = num_rb,
                                            .num_remote_send_threads = num_rb,
                                            .num_remote_receive_threads = num_rb,
                                            .num_own_receive = num_rb,
                                            .size_own_receive = bytes,
                                            .num_remote_receive = num_rb,
                                            .size_remote_receive = bytes,
                                            .num_own_send = num_rb,
                                            .size_own_send = bytes,
                                            .num_remote_send = num_rb,
                                            .size_remote_send = bytes,
                                            .meta_info_size = 16};

            ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

            LOG_INFO("\t[DataTransferTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl);
            out << "[INFO]\t\t[DataTransferTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl;

            for (size_t i = 0; i < 5; ++i) {
                receiveMap.clear();

                auto sendLambda = [&]() {
                    ConnectionManager::getInstance().sendData(1, reinterpret_cast<char*>(data), dataSize, nullptr, 0, strat == Strategies::push ? rdma_functional_test : rdma_pull_consume, strat);
                };

                for (size_t k = 0; k < parallelExecutions; ++k) {
                    std::thread(sendLambda).detach();
                }

                std::unique_lock<std::mutex> resultWaitLock(resultWaitMutex);

                resultWaitCV.wait_for(resultWaitLock, 10s);
                if (!resultsArrived) {
                    errorCount++;
                    LOG_ERROR("\t[DataTransferTest]\tA result in iteration " << +i << " did not arrive within 10s!" << std::endl);
                    LOG_ERROR("\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl);
                    out << "[ERROR]\t\t[DataTransferTest]\tA result in iteration " << +i << " did not arrive within 10s!" << std::endl;
                    out << "\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl;
                } else {
                    resultsArrived = false;

                    for (auto it = receiveMap.begin(); it != receiveMap.end(); ++it) {
                        auto currentResult = it->second.result;
                        if (currentResult == checkSum) {
                            LOG_SUCCESS("\t[DataTransferTest]\tThe Result in iteration " << +i << " matches the expected one." << std::endl);
                            out << "[SUCCESS]\t[DataTransferTest]\tThe Result in iteration " << +i << " matches the expected one." << std::endl;
                        } else {
                            errorCount++;
                            LOG_ERROR("\t[DataTransferTest]\tThe Result in iteration " << +i << " does not match the expected one. Expected: " << checkSum << "; Got: " << currentResult << std::endl);
                            LOG_ERROR("\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl);
                            out << "[ERROR]\t\t[DataTransferTest]\tThe Result in iteration " << +i << " does not match the expected one. Expected: " << checkSum << "; Got: " << currentResult << std::endl;
                            out << "\t\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl;
                        }
                    }
                }
            }
        }
    }

    LOG_INFO("\t[DataTransferTest]\tEnded with " << +errorCount << " Errors.\n"
                                                 << std::endl);
    out << "[INFO]\t\t[DataTransferTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    out << std::endl;

    free(data);

    return errorCount;
}

uint8_t FunctionalTests::bufferReconfigurationTestLite(std::ofstream& out) {
    uint8_t errorCount = 0;

    LOG_INFO("\t[BufferReconfigurationTest]\tStarting Buffer Reconfiguration Test." << std::endl);
    out << "[INFO]\t[BufferReconfigurationTest]\tStarting Buffer Reconfiguration Test." << std::endl;

    for (uint8_t num_rb = 1; num_rb <= 8; ++num_rb) {
        for (uint64_t bytes = 1ull << 16; bytes <= 1ull << 21; bytes <<= 1) {
            buffer_config_t bufferConfig = {.num_own_send_threads = num_rb,
                                            .num_own_receive_threads = num_rb,
                                            .num_remote_send_threads = num_rb,
                                            .num_remote_receive_threads = num_rb,
                                            .num_own_receive = num_rb,
                                            .size_own_receive = bytes,
                                            .num_remote_receive = num_rb,
                                            .size_remote_receive = bytes,
                                            .num_own_send = num_rb,
                                            .size_own_send = bytes,
                                            .num_remote_send = num_rb,
                                            .size_remote_send = bytes,
                                            .meta_info_size = 16};

            LOG_INFO("\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl);
            out << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl;

            ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);
        }
    }

    for (uint8_t num_rb = 8; num_rb >= 1; --num_rb) {
        for (uint64_t bytes = 1ull << 21; bytes >= 1ull << 16; bytes >>= 1) {
            buffer_config_t bufferConfig = {.num_own_send_threads = num_rb,
                                            .num_own_receive_threads = num_rb,
                                            .num_remote_send_threads = num_rb,
                                            .num_remote_receive_threads = num_rb,
                                            .num_own_receive = num_rb,
                                            .size_own_receive = bytes,
                                            .num_remote_receive = num_rb,
                                            .size_remote_receive = bytes,
                                            .num_own_send = num_rb,
                                            .size_own_send = bytes,
                                            .num_remote_send = num_rb,
                                            .size_remote_send = bytes,
                                            .meta_info_size = 16};

            LOG_INFO("\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl);
            out << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << Utility::GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_rb << "; #SB " << +num_rb << "; #ST " << +num_rb << std::endl;

            ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);
        }
    }

    LOG_INFO("\t[BufferReconfigurationTest]\tEnded with " << +errorCount << " Errors.\n"
                                                          << std::endl);
    out << "[INFO]\t[BufferReconfigurationTest]\tEnded with " << +errorCount << " Errors.\n"
        << std::endl;

    return errorCount;
}

FunctionalTests::~FunctionalTests() {
}
