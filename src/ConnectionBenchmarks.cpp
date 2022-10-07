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

int Connection::benchmark(const std::string shortName, const std::string name, const BenchmarkType benchType, const Strategies strat) {
    /* provide data to remote */
    DataProvider d;
    d.generateDummyData(MAX_DATA_ELEMENTS);

    uint8_t sendOpcode = rdma_ready;
    if (benchType == BenchmarkType::throughput) {
        if (strat == Strategies::push) {
            sendOpcode = rdma_ready;
        } else if (strat == Strategies::pull) {
            sendOpcode = rdma_pull_read;
        }
    } else if (benchType == BenchmarkType::consume) {
        if (strat == Strategies::push) {
            sendOpcode = rdma_data_finished;
        } else if (strat == Strategies::pull) {
            sendOpcode = rdma_pull_consume;
        }
    }

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

                for (uint64_t bufferSize = 1ull << 16; bufferSize <= 1ull << 22; bufferSize <<= 1) {
                    buffer_config_t bufferConfig = {.num_own_send_threads = thrds,
                                                    .num_own_receive_threads = thrds,
                                                    .num_remote_send_threads = thrds,
                                                    .num_remote_receive_threads = thrds,
                                                    .num_own_receive = num_rb,
                                                    .size_own_receive = bufferSize + package_t::metaDataSize(),
                                                    .num_remote_receive = num_rb,
                                                    .size_remote_receive = bufferSize + package_t::metaDataSize(),
                                                    .num_own_send = num_sb,
                                                    .size_own_send = bufferSize + package_t::metaDataSize(),
                                                    .num_remote_send = num_sb,
                                                    .size_remote_send = bufferSize + package_t::metaDataSize(),
                                                    .meta_info_size = 16};

                    Utility::checkOrDie(sendReconfigureBuffer(bufferConfig));

                    LOG_DEBUG1("[" << name << "]\tUsed connection with id '1', " << +num_rb << " remote receive buffer (size for one remote receive: " << Utility::GetBytesReadable(bufferSize) << ") and " << +num_sb << " \n"
                                   << std::endl);

                    for (std::size_t elementCount = 1; elementCount <= MAX_DATA_ELEMENTS; elementCount <<= 1) {
                        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
                            uint64_t dataSize = elementCount * sizeof(uint64_t);
                            char *copy = reinterpret_cast<char *>(d.data);
                            auto readable_size = Utility::GetBytesReadable(dataSize);
                            // LOG_DEBUG1("[" << name << "]\tGenerating " << readable_size << " of data to send them over." << std::endl);

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

                            sendData(copy, dataSize, nullptr, 0, sendOpcode, strat);

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

                            out << bufferSize << "\t" << dataSize << "\t" << transfertime_ns << "\t" << Utility::BtoMB(dataSize) / (secs.count()) << std::endl;
                        }
                    }

                    LOG_SUCCESS("[" << name << "]\tBenchmark with Buffer Size " << bufferSize << " ended." << std::endl);
                }

                out.close();
            }
        }
    }

    LOG_SUCCESS("[" << name << "]\tBenchmark done." << std::endl);

    return 0;
}