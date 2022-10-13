#include "PerformanceTests.h"

#include <iostream>

#include "ConnectionManager.h"
#include "Logger.h"
#include "Utility.h"

using namespace memordma;

PerformanceTests::PerformanceTests() {
    CallbackFunction continuosTest = [this](const size_t conId, const ReceiveBuffer *rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) {
        // Package header
        package_t::header_t *head = reinterpret_cast<package_t::header_t *>(rcv_buffer->getBufferPtr());
        // Start of Payload
        uint64_t data_received = head->current_payload_size + sizeof(package_t::header_t);
        reset_buffer();

        auto con = ConnectionManager::getInstance().getConnectionById(conId);
        con->sendData((char *)&data_received, sizeof(uint64_t), nullptr, 0, rdma_continuous_test_ack, Strategies::push);
    };

    CallbackFunction continuosTestAck = [this](__attribute__ ((unused)) const size_t conId, const ReceiveBuffer *rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) {
        package_t::header_t *head = reinterpret_cast<package_t::header_t *>(rcv_buffer->getBufferPtr());
        uint64_t *data = reinterpret_cast<uint64_t *>(rcv_buffer->getBufferPtr() + sizeof(package_t::header_t) + head->payload_start);
        continuousBenchmarkReceivedBytes += *data + sizeof(package_t::header_t);
        reset_buffer();
    };
    
    ConnectionManager::getInstance().registerCallback(static_cast<uint8_t>(rdma_continuous_test), continuosTest);
    ConnectionManager::getInstance().registerCallback(static_cast<uint8_t>(rdma_continuous_test_ack), continuosTestAck);
}

int PerformanceTests::continuousConsumeBenchmark(size_t conId, size_t seconds) {
    LOG_CONSOLE("Entering benchmark" << std::endl;)
    continuousBenchmarkReceivedBytes = 0;

    const size_t dataSize = 1024 * 1024 * 1024;
    LOG_INFO("Generating data" << std::endl;)
    uint64_t *data = memordma::Utility::generateRandomDummyData<uint64_t>(dataSize / sizeof(uint64_t));  // 1 GB
    LOG_INFO("Done generating." << std::endl;)

    size_t millisecondsExecutedTotal = 0;
    size_t testRuntimeMilliseconds = 0;

    auto con = ConnectionManager::getInstance().getConnectionById(conId);
    auto lastCheckpoint = std::chrono::high_resolution_clock::now();
    auto currentCheckpoint = lastCheckpoint;
    while (millisecondsExecutedTotal < seconds * 1000) {
        while (testRuntimeMilliseconds < 1000.0) {
            con->sendData(reinterpret_cast<char *>(data), dataSize, nullptr, 0, rdma_continuous_test, Strategies::push);
            currentCheckpoint = std::chrono::high_resolution_clock::now();
            testRuntimeMilliseconds += std::chrono::duration_cast<std::chrono::milliseconds>(currentCheckpoint - lastCheckpoint).count();
            lastCheckpoint = currentCheckpoint;
        }
        const double seconds = static_cast<double>(testRuntimeMilliseconds) / 1000;
        const double bytesPerSecond = static_cast<double>(continuousBenchmarkReceivedBytes) / seconds;
        std::cout << "\rThroughput: " << memordma::Utility::GetBytesReadable( bytesPerSecond ) << "/s      " << std::flush;
        millisecondsExecutedTotal += testRuntimeMilliseconds;
        testRuntimeMilliseconds = 0;
        continuousBenchmarkReceivedBytes = 0;
    }
    std::cout << std::endl;

    return 0;
}

PerformanceTests::~PerformanceTests() {}