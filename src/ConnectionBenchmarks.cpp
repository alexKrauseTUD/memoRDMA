#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <thread>
#include <tuple>
#include <vector>

#include "Connection.h"
#include "DataProvider.h"
#include "Utility.h"

using namespace memordma;
int Connection::throughputBenchmark(std::string logName, Strategies strat) {
    /* provide data to remote */
    std::size_t maxDataElements = 1ull << MAX_DATA_SIZE;
    DataProvider d;
    d.generateDummyData(maxDataElements >> 1);
    std::ofstream out;
    out.open(logName, std::ios_base::app);

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            uint64_t dataSize = elementCount * sizeof(uint64_t);
            std::cout << "[ThroughputTest] Generating " << dataSize << " Byte of data and send them over." << std::endl;
            char *copy = reinterpret_cast<char *>(d.data);

            auto s_ts = std::chrono::high_resolution_clock::now();

            sendData(copy, dataSize, nullptr, 0, rdma_ready, strat);

            auto e_ts = std::chrono::high_resolution_clock::now();

            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

            typedef std::chrono::duration<double> d_sec;
            d_sec secs = e_ts - s_ts;

            std::cout << "[ThroughputTest] Communicated " << dataSize << " Bytes (" << Utility::BtoMB(dataSize) << " MB) in " << secs.count() << " s -- " << Utility::BtoMB(dataSize) / (secs.count()) << " MB/s " << std::endl;

            auto readable_size = Utility::GetBytesReadable(dataSize);

            std::cout.precision(15);
            std::cout.setf(std::ios::fixed, std::ios::floatfield);
            std::cout.setf(std::ios::showpoint);
            out << +bufferConfig.num_own_send_threads << "\t" << +bufferConfig.num_own_send << "\t" << bufferConfig.size_own_send << "\t" << +bufferConfig.num_remote_receive_threads << "\t" << +bufferConfig.num_remote_receive << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << dataSize << "\t" << transfertime_ns << "\t" << Utility::BtoMB(dataSize) / (secs.count()) << std::endl
                << std::flush;
            std::cout.flags(f);
        }
    }
    std::cout << "[ThroughputTest] Finished." << std::endl;
    out.close();

    return 0;
}

int Connection::consumingBenchmark(std::string logName, Strategies strat) {
    bool allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfoReceive[c + (metaInfoReceive.size() / 2)] == rdma_ready);
        }
        for (size_t c = 0; c < bufferConfig.num_own_send; ++c) {
            allDone &= (metaInfoSend[c] == rdma_ready);
        }
    }

    /* provide data to remote */
    std::size_t maxDataElements = 1ull << MAX_DATA_SIZE;
    DataProvider d;
    d.generateDummyData(maxDataElements >> 1);
    std::ofstream out;
    out.open(logName, std::ios_base::app);

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            uint64_t dataSize = elementCount * sizeof(uint64_t);
            char *copy = reinterpret_cast<char *>(d.data);

            std::cout << "[ConsumeTest] Generating " << dataSize << " Byte of data and send them over." << std::endl;

            auto s_ts = std::chrono::high_resolution_clock::now();

            sendData(copy, dataSize, nullptr, 0, rdma_ready, strat);

            auto e_ts = std::chrono::high_resolution_clock::now();

            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

            typedef std::chrono::duration<double> d_sec;
            d_sec secs = e_ts - s_ts;

            std::cout << "[ConsumeTest] Communicated " << dataSize << " Bytes (" << Utility::BtoMB(dataSize) << " MB) in " << secs.count() << " s -- " << Utility::BtoMB(dataSize) / (secs.count()) << " MB/s " << std::endl;

            auto readable_size = Utility::GetBytesReadable(dataSize);

            std::cout.precision(15);
            std::cout.setf(std::ios::fixed, std::ios::floatfield);
            std::cout.setf(std::ios::showpoint);
            out << bufferConfig.num_own_send_threads << "\t" << bufferConfig.num_own_send << "\t" << bufferConfig.size_own_send << "\t" << bufferConfig.num_remote_receive_threads << "\t" << bufferConfig.num_remote_receive << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << dataSize << "\t" << transfertime_ns << "\t" << Utility::BtoMB(dataSize) / (secs.count()) << std::endl
                << std::flush;
            std::cout.flags(f);
        }
    }
    std::cout << "[ConsumeTest] Finished." << std::endl;
    out.close();

    allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfoReceive[c + (metaInfoReceive.size() / 2)] == rdma_ready);
        }
        for (size_t c = 0; c < bufferConfig.num_own_send; ++c) {
            allDone &= (metaInfoSend[c] == rdma_ready);
        }
    }

    return 0;
}