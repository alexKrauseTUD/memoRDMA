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

template <BenchmarkType benchType, Strategies strat>
int Connection::benchmark(std::string shortName, std::string name) {
    /* provide data to remote */
    std::size_t maxDataElements = 1ull << MAX_DATA_SIZE;
    DataProvider d;
    d.generateDummyData(maxDataElements >> 1);

    for (uint8_t num_rb = 1; num_rb <= 8; ++num_rb) {
        for (uint8_t num_sb = 1; num_sb <= num_rb; ++num_sb) {
            for (uint8_t thrds = 1; thrds <= num_sb; ++thrds) {
                auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                std::stringstream logNameStream;
                logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << shortName << "_" << +num_sb << "_" << +num_rb << "_" << +thrds << ".log";
                std::string logName = logNameStream.str();
                LOG_INFO("[" << name << "]\tSet file name: " << logName << std::endl);
                std::ofstream out;
                out.open(logName, std::ios_base::app);

                for (uint64_t size_rb = 1ull << 15; size_rb < 1ull << 28; size_rb <<= 1) {
                    buffer_config_t bufferConfig = {.num_own_send_threads = thrds,
                                                    .num_own_receive_threads = thrds,
                                                    .num_remote_send_threads = thrds,
                                                    .num_remote_receive_threads = thrds,
                                                    .num_own_receive = num_rb,
                                                    .size_own_receive = size_rb + package_t::metaDataSize(),
                                                    .num_remote_receive = num_rb,
                                                    .size_remote_receive = size_rb + package_t::metaDataSize(),
                                                    .num_own_send = num_sb,
                                                    .size_own_send = size_rb + package_t::metaDataSize(),
                                                    .num_remote_send = num_sb,
                                                    .size_remote_send = size_rb + package_t::metaDataSize(),
                                                    .meta_info_size = 16};

                    Utility::checkOrDie(sendReconfigureBuffer(bufferConfig));

                    LOG_DEBUG1("[" << name << "]\tUsed connection with id '1' and " << +num_rb << " remote receive buffer (size for one remote receive: " << Utility::GetBytesReadable(size_rb) << ")\n"
                                   << std::endl);

                    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
                        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
                            uint64_t dataSize = elementCount * sizeof(uint64_t);
                            char *copy = reinterpret_cast<char *>(d.data);
                            auto readable_size = Utility::GetBytesReadable(dataSize);
                            LOG_DEBUG1("[" << name << "]\tGenerating " << readable_size << " of data to send them over." << std::endl);

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

                            auto s_ts = std::chrono::high_resolution_clock::now();

                            if (benchType == BenchmarkType::throughput) {
                                if (strat == Strategies::push) {
                                    sendData(copy, dataSize, nullptr, 0, rdma_ready, strat);
                                } else if (strat == Strategies::pull) {
                                    sendData(copy, dataSize, nullptr, 0, rdma_pull_read, strat);
                                }
                            } else if (benchType == BenchmarkType::consume) {
                                if (strat == Strategies::push) {
                                    sendData(copy, dataSize, nullptr, 0, rdma_data_finished, strat);
                                } else if (strat == Strategies::pull) {
                                    sendData(copy, dataSize, nullptr, 0, rdma_pull_consume, strat);
                                }
                            }

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

                            auto e_ts = std::chrono::high_resolution_clock::now();

                            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

                            typedef std::chrono::duration<double> d_sec;
                            d_sec secs = e_ts - s_ts;

                            LOG_SUCCESS(std::setprecision(10) << "[" << name << "]\tCommunicated " << dataSize << " Bytes (" << readable_size << ") in " << secs.count() << " s -- " << Utility::BtoMB(dataSize) / (secs.count()) << " MB/s " << std::endl);

                            out << bufferConfig.num_own_send_threads << "\t" << bufferConfig.num_own_send << "\t" << bufferConfig.size_own_send << "\t" << bufferConfig.num_remote_receive_threads << "\t" << bufferConfig.num_remote_receive << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << dataSize << "\t" << transfertime_ns << "\t" << Utility::BtoMB(dataSize) / (secs.count()) << std::endl
                                << std::flush;
                        }
                    }

                    out.close();

                    LOG_SUCCESS("[" << name << "]\tBenchmark ended." << std::endl);
                }
            }
        }
    }

    return 0;
}