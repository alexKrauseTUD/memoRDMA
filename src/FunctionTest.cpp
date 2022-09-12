#include "FunctionalTests.hpp"

FunctionalTests::FunctionalTests() {
    CallbackFunction receiveDataTransferTest = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) {
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->getBufferPtr());
        // Start of Payload
        uint64_t* data = reinterpret_cast<uint64_t*>(rcv_buffer->getBufferPtr() + sizeof(package_t::header_t) + head->payload_start);

        std::lock_guard<std::mutex> lg(mapMutex);

        uint64_t packageId = head->id;

        if (!receiveMap.contains(packageId)) {
            receiveMap.emplace(packageId, ReceiveData());
        }

        receiveMap[packageId].receivedBytes += head->current_payload_size;

        auto currentElements = head->current_payload_size / sizeof(uint64_t);

        reset_buffer();

        for (size_t i = 0; i < currentElements; ++i) {
            receiveMap[packageId].result += data[i];
        }

        if (receiveMap[packageId].receivedBytes == dataSize) {
            ConnectionManager::getInstance().sendData(1, reinterpret_cast<char*>(&receiveMap[packageId].result), sizeof(receiveMap[packageId].result), nullptr, 0, rdma_functional_test_ack, Strategies::push);

            receiveMap.erase(packageId);
        }
    };

    CallbackFunction dataTransferTestAck = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) {
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

uint8_t FunctionalTests::executeAllTests() {
    uint8_t numberProblems = 0;
    auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream logNameStream;
    logNameStream << "FunctionalTests" << std::put_time(std::localtime(&in_time_t), "_%Y-%m-%d-%H-%M-%S") << ".log";
    std::string logName = logNameStream.str();
    std::cout << "[FunctionalTests] Set name: " << logName << std::endl;

    std::ofstream out;
    out.open(logName, std::ios_base::app);

    numberProblems += bufferReconfigurationTest(out);
    numberProblems += dataTransferTest(out);

    out.close();

    std::cout << std::endl;
    std::cout << "[FunctionalTests] Met " << +numberProblems << " Problems while executing all tests." << std::endl;
    std::cout << std::endl;

    return numberProblems;
}

uint8_t FunctionalTests::dataTransferTest(std::ofstream& out) {
    uint8_t errorCount = 0;
    uint64_t* data = generateRandomDummyData<uint64_t>(elementCount);

    std::cout << "[INFO]\t[DataTransferTest]\tGenerated " << elementCount << " Elements with a total size of ca. " << GetBytesReadable(dataSize) << std::endl;
    out << "[INFO]\t[DataTransferTest]\tGenerated " << elementCount << " Elements with a total size of ca. " << GetBytesReadable(dataSize) << std::endl;

    uint64_t checkSum = 0;
    for (size_t i = 0; i < elementCount; ++i) {
        checkSum += data[i];
    }

    std::cout << "[INFO]\t[DataTransferTest]\tThe checksum of the generated data is\t" << +checkSum << std::endl;
    out << "[INFO]\t[DataTransferTest]\tThe checksum of the generated data is\t" << +checkSum << std::endl;

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

                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(2s);

                        std::cout << "[INFO]\t[DataTransferTest]\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                        out << "[INFO]\t[DataTransferTest]\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;

                        for (size_t i = 0; i < 5; ++i) {
                            receiveMap.clear();

                            std::unique_lock<std::mutex> resultWaitLock(resultWaitMutex);

                            for (size_t k = 0; k < parallelExecutions; ++k) {
                                ConnectionManager::getInstance().sendData(1, reinterpret_cast<char*>(data), dataSize, nullptr, 0, rdma_functional_test, Strategies::push);
                            }

                            resultWaitCV.wait(resultWaitLock, [&] { return resultsArrived; });
                            resultsArrived = false;

                            for (auto it = receiveMap.begin(); it != receiveMap.end(); ++it) {
                                auto currentResult = it->second.result;
                                if (currentResult == checkSum) {
                                    std::cout << "[SUCCESS]\t[DataTransferTest]\tThe Result in iteration " << +i << " matches the expected one." << std::endl;
                                    out << "[SUCCESS]\t[DataTransferTest]\tThe Result in iteration " << +i << " matches the expected one." << std::endl;
                                } else {
                                    errorCount++;
                                    std::cout << "[ERROR]\t[DataTransferTest]\tThe Result in iteration " << +i << " does not match the expected one. Expected: " << checkSum << "; Got: " << currentResult << std::endl;
                                    std::cout << "\t\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                                    out << "[ERROR]\t[DataTransferTest]\tThe Result in iteration " << +i << " does not match the expected one. Expected: " << checkSum << "; Got: " << currentResult << std::endl;
                                    out << "\t\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    std::cout << "[INFO]\t[DataTransferTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    out << "[INFO]\t[DataTransferTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    std::cout << std::endl;
    out << std::endl;

    return errorCount;
}

uint8_t FunctionalTests::bufferReconfigurationTest(std::ofstream& out) {
    uint8_t errorCount = 0;

    std::cout << "[INFO]\t[BufferReconfigurationTest]\tStarting Buffer Reconfiguration Test." << std::endl;
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

                        std::cout << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                        out << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;

                        ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(2s);
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

                        std::cout << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;
                        out << "[INFO]\t[BufferReconfigurationTest]\tConnection-ID 1; Buffer Size " << GetBytesReadable(bytes) << "; #RB " << +num_rb << "; #RT " << +num_r_threads << "; #SB " << +num_sb << "; #ST " << +num_s_threads << std::endl;

                        ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(2s);
                    }
                }
            }
        }
    }


    std::cout << "[INFO]\t[BufferReconfigurationTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    out << "[INFO]\t[BufferReconfigurationTest]\tEnded with " << +errorCount << " Errors." << std::endl;
    std::cout << std::endl;
    out << std::endl;
}

FunctionalTests::~FunctionalTests() {
}