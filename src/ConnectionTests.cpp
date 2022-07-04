#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <thread>
#include <tuple>
#include <vector>

#include "Connection.h"
#include "DataProvider.h"

int Connection::throughputTest(std::string logName, Strategies strat) {
    while (reconfiguring) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
    }

    ownSendBuffer->clearBuffer();

    /* provide data to remote */
    std::size_t maxDataElements = 1ull << MAX_DATA_SIZE;
    DataProvider d;
    d.generateDummyData(maxDataElements >> 1);
    std::ofstream out;
    out.open(logName, std::ios_base::app);

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ThroughputTest] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ThroughputTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;
            uint64_t packageID = generatePackageID();

            package_t package(packageID, maxDataToWrite, 1, 0, type_package, remainingSize, 0, copy);

            timePoint s_ts, e_ts;

            if (strat == Strategies::push) {
                std::tie(s_ts, e_ts) = throughputTestPush(package, remainingSize, maxPayloadSize, maxDataToWrite);
            } else if (strat == Strategies::pull) {
                std::tie(s_ts, e_ts) = throughputTestPull(package, remainingSize, maxPayloadSize, maxDataToWrite);
            }

            auto datasize = elementCount * sizeof(uint64_t);
            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

            typedef std::chrono::duration<double> d_sec;
            d_sec secs = e_ts - s_ts;

            std::cout << "[ThroughputTest] Communicated " << datasize << " Bytes (" << BtoMB(datasize) << " MB) in " << secs.count() << " s -- " << BtoMB(datasize) / (secs.count()) << " MB/s " << std::endl;

            auto readable_size = GetBytesReadable(datasize);

            std::cout.precision(15);
            std::cout.setf(std::ios::fixed, std::ios::floatfield);
            std::cout.setf(std::ios::showpoint);
            out << ownSendBuffer->bufferSize << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB(datasize) / (secs.count()) << std::endl
                << std::flush;
            std::cout.flags(f);
        }
    }
    std::cout << "[ThroughputTest] Finished." << std::endl;
    out.close();
    busy = false;

    return 0;
}

std::tuple<timePoint, timePoint> Connection::throughputTestPush(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite) {
    size_t maxPackNum;

    auto s_ts = std::chrono::high_resolution_clock::now();

    while (remainingSize > maxPayloadSize) {
        maxPackNum = bufferConfig.num_remote_receive;
        for (size_t pack = 1; pack <= bufferConfig.num_remote_receive; ++pack) {
            if (remainingSize < maxPayloadSize * pack) {
                maxPackNum = pack - 1;
                break;
            }
        }

        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            ownSendBuffer->loadPackage(ownSendBuffer->buf + (rbi * bufferConfig.size_remote_receive), &package);
        }
        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            if (remainingSize <= maxPayloadSize) break;
            ownSendBuffer->sendPackage(&package, res.remote_buffer[rbi], res.remote_rkeys[rbi], res.qp, ownSendBuffer->buf + (rbi * bufferConfig.size_remote_receive), 0);
            poll_completion();

            remainingSize -= maxDataToWrite;
            package.advancePayloadPtr(maxDataToWrite);
        }
    }

    package.setCurrentPackageSize(remainingSize);
    ownSendBuffer->loadPackage(ownSendBuffer->buf, &package);
    ownSendBuffer->sendPackage(&package, res.remote_buffer[0], res.remote_rkeys[0], res.qp, ownSendBuffer->buf, 0);
    poll_completion();
    auto e_ts = std::chrono::high_resolution_clock::now();

    return std::make_pair(s_ts, e_ts);
}

std::tuple<timePoint, timePoint> Connection::throughputTestPull(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite) {
    size_t maxPackNum;
    bool allDone;
    auto metaSizeOwn = metaInfo.size() / 2;

    auto s_ts = std::chrono::high_resolution_clock::now();

    while (remainingSize > maxPayloadSize) {
        maxPackNum = bufferConfig.num_remote_receive;
        for (size_t pack = 1; pack <= bufferConfig.num_remote_receive; ++pack) {
            if (remainingSize < maxPayloadSize * pack) {
                maxPackNum = pack - 1;
                break;
            }
        }

        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            ownSendBuffer->loadPackage(ownSendBuffer->buf + (rbi * bufferConfig.size_remote_receive), &package);
        }
        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            if (remainingSize <= maxPayloadSize) break;
            setOpcode(rbi + metaSizeOwn, rdma_pull_read, true);

            remainingSize -= maxDataToWrite;
            package.advancePayloadPtr(maxDataToWrite);
        }

        allDone = false;
        while (!allDone) {
            allDone = true;
            for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
                allDone &= (metaInfo[c + metaSizeOwn] == rdma_ready);
            }
        }
    }

    package.setCurrentPackageSize(remainingSize);
    ownSendBuffer->loadPackage(ownSendBuffer->buf, &package);
    setOpcode(metaSizeOwn, rdma_pull_read, true);

    allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfo[c + metaSizeOwn] == rdma_ready);
        }
    }

    auto e_ts = std::chrono::high_resolution_clock::now();

    return std::make_pair(s_ts, e_ts);
}

int Connection::consumingTest(std::string logName, Strategies strat) {
    while (reconfiguring) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
    }

    ownSendBuffer->clearBuffer();

    bool allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfo[c + (metaInfo.size() / 2)] == rdma_ready);
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
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            uint64_t *copy = d.data;
            uint64_t packageID = generatePackageID();

            std::cout << "[ConsumeTest] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ConsumeTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;

            package_t package(packageID, maxDataToWrite, 1, 0, type_package, remainingSize, 0, copy);

            timePoint s_ts, e_ts;

            if (strat == Strategies::push) {
                std::tie(s_ts, e_ts) = consumingTestPush(package, remainingSize, maxPayloadSize, maxDataToWrite);
            } else if (strat == Strategies::pull) {
                std::tie(s_ts, e_ts) = consumingTestPull(package, remainingSize, maxPayloadSize, maxDataToWrite);
            }

            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();
            auto datasize = elementCount * sizeof(uint64_t);

            typedef std::chrono::duration<double> d_sec;
            d_sec secs = e_ts - s_ts;

            std::cout << "[ConsumeTest] Communicated " << datasize << " Bytes (" << BtoMB(datasize) << " MB) in " << secs.count() << " s -- " << BtoMB(datasize) / (secs.count()) << " MB/s " << std::endl;

            auto readable_size = GetBytesReadable(datasize);

            std::cout.precision(15);
            std::cout.setf(std::ios::fixed, std::ios::floatfield);
            std::cout.setf(std::ios::showpoint);
            out << ownSendBuffer->bufferSize << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB(datasize) / (secs.count()) << std::endl
                << std::flush;
            std::cout.flags(f);

            ownSendBuffer->clearBuffer();
        }
    }
    std::cout << "[ConsumeTest] Finished." << std::endl;
    out.close();
    busy = false;

    allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfo[c + (metaInfo.size() / 2)] == rdma_ready);
        }
    }

    return 0;
}

std::tuple<timePoint, timePoint> Connection::consumingTestPush(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite) {
    int nextFree;
    uint64_t packNum = 0;
    uint64_t maxPackNum;

    auto s_ts = std::chrono::high_resolution_clock::now();

    while (remainingSize > maxPayloadSize) {
        ownSendBuffer->clearBuffer();
        maxPackNum = bufferConfig.num_remote_receive;
        for (size_t pack = 1; pack <= bufferConfig.num_remote_receive; ++pack) {
            if (remainingSize < maxPayloadSize * pack) {
                maxPackNum = pack - 1;
                break;
            }
        }
        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            package.setCurrentPackageNumber(packNum);
            ownSendBuffer->loadPackage(ownSendBuffer->buf + (rbi * bufferConfig.size_remote_receive), &package);
            ++packNum;
        }
        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            if (remainingSize <= maxPayloadSize) break;
            do {
                nextFree = getNextFreeReceive();
            } while (nextFree == -1);

            setOpcode((metaInfo.size() / 2) + nextFree, rdma_sending, false);
            ownSendBuffer->sendPackage(&package, res.remote_buffer[nextFree], res.remote_rkeys[nextFree], res.qp, ownSendBuffer->buf + (rbi * bufferConfig.size_remote_receive), 0);
            poll_completion();

            setOpcode((metaInfo.size() / 2) + nextFree, rdma_consume_test, true);

            remainingSize -= maxDataToWrite;
            package.advancePayloadPtr(maxDataToWrite);
        }
    }

    do {
        nextFree = getNextFreeReceive();
    } while (nextFree == -1);

    setOpcode((metaInfo.size() / 2) + nextFree, rdma_sending, false);
    package.setCurrentPackageNumber(packNum);
    package.setCurrentPackageSize(remainingSize);
    ownSendBuffer->loadPackage(ownSendBuffer->buf, &package);
    ownSendBuffer->sendPackage(&package, res.remote_buffer[nextFree], res.remote_rkeys[nextFree], res.qp, ownSendBuffer->buf, 0);
    poll_completion();
    setOpcode((metaInfo.size() / 2) + nextFree, rdma_consume_test, true);

    auto e_ts = std::chrono::high_resolution_clock::now();

    return std::make_pair(s_ts, e_ts);
}

std::tuple<timePoint, timePoint> Connection::consumingTestPull(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite) {
    int nextFree;
    size_t packNum = 0;
    size_t maxPackNum;
    bool allDone;
    auto metaSizeOwn = metaInfo.size() / 2;

    auto s_ts = std::chrono::high_resolution_clock::now();

    while (remainingSize > maxPayloadSize) {
        ownSendBuffer->clearBuffer();
        maxPackNum = bufferConfig.num_remote_receive;
        for (size_t pack = 1; pack <= bufferConfig.num_remote_receive; ++pack) {
            if (remainingSize < maxPayloadSize * pack) {
                maxPackNum = pack - 1;
                break;
            }
        }
        for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
            setOpcode(metaSizeOwn + rbi, rdma_sending, false);
            package.setCurrentPackageNumber(packNum);
            ownSendBuffer->loadPackage(ownSendBuffer->buf + (rbi * bufferConfig.size_remote_receive), &package);
            ++packNum;

            setOpcode(metaSizeOwn + rbi, rdma_pull_consume, true);

            remainingSize -= maxDataToWrite;
            package.advancePayloadPtr(maxDataToWrite);

            if (remainingSize <= maxPayloadSize) break;
        }

        allDone = false;
        while (!allDone) {
            allDone = true;
            for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
                allDone &= (metaInfo[c + metaSizeOwn] == rdma_ready);
            }
        }
    }

    do {
        nextFree = getNextFreeReceive();
    } while (nextFree == -1);

    setOpcode(metaSizeOwn + nextFree, rdma_sending, false);
    package.setCurrentPackageNumber(packNum);
    package.setCurrentPackageSize(remainingSize);
    ownSendBuffer->loadPackage(ownSendBuffer->buf, &package);
    setOpcode(metaSizeOwn + nextFree, rdma_pull_consume, true);

    allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfo[c + metaSizeOwn] == rdma_ready);
        }
    }

    auto e_ts = std::chrono::high_resolution_clock::now();

    return std::make_pair(s_ts, e_ts);
}

int Connection::throughputTestMultiThread(std::string logName, Strategies strat) {
    while (reconfiguring) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
    }

    ownSendBuffer->clearBuffer();

    if (strat == Strategies::pull) {
        while (metaInfo[(metaInfo.size() / 2)] != rdma_ready) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(10ms);
        }

        setOpcode(metaInfo.size() / 2, rdma_multi_thread, true);

        while (metaInfo[(metaInfo.size() / 2)] != rdma_ready) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(10ms);
        }
    }

    std::vector<std::thread *> pool;
    size_t thread_cnt = (int)(bufferConfig.num_remote_receive / 2);
    thread_cnt = thread_cnt > 0 ? thread_cnt : 1;
    bool *ready_vec = (bool *)malloc(thread_cnt * sizeof(bool));
    std::vector<std::vector<package_t *>> work_queue(thread_cnt);

    /* provide data to remote */
    std::size_t maxDataElements = 1ull << MAX_DATA_SIZE;
    DataProvider d;
    d.generateDummyData(maxDataElements >> 1);
    std::ofstream out;
    out.open(logName, std::ios_base::app);

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            memset(ready_vec, 0, thread_cnt * sizeof(bool));
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ThroughputTest-MultiThreaded] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ThroughputTest-MultiThreaded] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;
            size_t maxPackNum;
            work_queue.resize(thread_cnt);
            uint64_t packageID = generatePackageID();

            package_t package(packageID, maxDataToWrite, 1, 0, type_package, remainingSize, 0, copy);

            while (remainingSize > 0) {
                maxPackNum = thread_cnt;
                for (size_t pack = 1; pack <= thread_cnt; ++pack) {
                    if (remainingSize < maxPayloadSize * pack) {
                        maxPackNum = pack;
                        break;
                    }
                }

                for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
                    if (remainingSize <= maxDataToWrite) {
                        package.setCurrentPackageSize(remainingSize);
                        remainingSize = 0;
                    }
                    package_t *newPackage = package.deep_copy();

                    work_queue[rbi].push_back(newPackage);

                    if (remainingSize > maxDataToWrite) {
                        remainingSize -= maxDataToWrite;
                        package.advancePayloadPtr(maxDataToWrite);
                    }
                }
            }

            std::promise<void> p;
            std::shared_future<void> ready_future(p.get_future());

            for (size_t tid = 0; tid < thread_cnt; ++tid) {
                if (strat == Strategies::push) {
                    auto work = [this](size_t tid, std::vector<package_t *> *queue, bool *local_ready, std::shared_future<void> *sync_barrier, size_t thrdcnt) -> void {
                        local_ready[tid] = true;
                        sync_barrier->wait();

                        for (package_t *pack : *queue) {
                            for (size_t id = tid; id < bufferConfig.num_remote_receive; id += thrdcnt) {
                                setOpcode(id + (metaInfo.size() / 2), rdma_sending, false);
                                ownSendBuffer->loadPackage(ownSendBuffer->buf + (tid * bufferConfig.size_remote_receive), pack);
                                ownSendBuffer->sendPackage(pack, res.remote_buffer[tid], res.remote_rkeys[tid], res.qp, ownSendBuffer->buf + (tid * bufferConfig.size_remote_receive), tid);
                                poll_completion();

                                setOpcode(id + (metaInfo.size() / 2), rdma_ready, false);

                                free(pack);
                                break;
                            }
                        }
                    };

                    pool.emplace_back(new std::thread(work, tid, &work_queue[tid], ready_vec, &ready_future, thread_cnt));
                } else if (strat == Strategies::pull) {
                    auto work = [this](size_t tid, std::vector<package_t *> *queue, bool *local_ready, std::shared_future<void> *sync_barrier, size_t thrdcnt) -> void {
                        local_ready[tid] = true;
                        bool packageSent = false;
                        sync_barrier->wait();

                        for (package_t *pack : *queue) {
                            packageSent = false;
                            while (!packageSent) {
                                for (size_t id = tid; id < bufferConfig.num_remote_receive; id += thrdcnt) {
                                    size_t remoteIndex = id + (metaInfo.size() / 2);
                                    if (metaInfo[remoteIndex] == rdma_ready) {
                                        setOpcode(remoteIndex, rdma_sending, false);
                                        ownSendBuffer->loadPackage(ownSendBuffer->buf + (tid * bufferConfig.size_remote_receive), pack);

                                        setOpcode(remoteIndex, rdma_pull_read, true);

                                        free(pack);
                                        packageSent = true;
                                        break;
                                    }
                                }
                            }
                        }
                    };

                    pool.emplace_back(new std::thread(work, tid, &work_queue[tid], ready_vec, &ready_future, thread_cnt));
                } else {
                    return 1;
                }
            }

            bool all_ready = false;
            while (!all_ready) {
                /* Wait until all threads are ready to go before we pull the trigger */
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
                all_ready = true;
                for (size_t i = 0; i < thread_cnt; ++i) {
                    all_ready &= ready_vec[i];
                }
            }
            auto s_ts = std::chrono::high_resolution_clock::now();
            p.set_value();                                                   /* Start execution by notifying on the void promise */
            std::for_each(pool.begin(), pool.end(), [](std::thread *t) { t->join(); delete t; }); /* Join and delete threads as soon as they are finished */

            auto e_ts = std::chrono::high_resolution_clock::now();
            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

            work_queue.clear();
            pool.clear();
            auto datasize = elementCount * sizeof(uint64_t);

            typedef std::chrono::duration<double> d_sec;
            d_sec secs = e_ts - s_ts;

            std::cout << "[ThroughputTest-MultiThreaded] Communicated " << datasize << " Bytes (" << BtoMB(datasize) << " MB) in " << secs.count() << " s -- " << BtoMB(datasize) / (secs.count()) << " MB/s " << std::endl;

            auto readable_size = GetBytesReadable(datasize);

            std::cout.precision(15);
            std::cout.setf(std::ios::fixed, std::ios::floatfield);
            std::cout.setf(std::ios::showpoint);
            out << ownSendBuffer->bufferSize << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB(datasize) / (secs.count()) << std::endl
                << std::flush;
            std::cout.flags(f);
        }
    }
    std::cout << "[ThroughputTest-MultiThreaded] Finished." << std::endl;
    out.close();
    busy = false;
    free(ready_vec);

    if (strat == Strategies::pull) {
        bool allDone = false;
        while (!allDone) {
            allDone = true;
            for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
                allDone &= (metaInfo[c + (metaInfo.size() / 2)] == rdma_ready);
            }
        }

        for (size_t c = 0; c < thread_cnt; ++c) {
            setOpcode(c + (metaInfo.size() / 2), rdma_test_finished, true);
        }
    }

    return 0;
}

int Connection::consumingTestMultiThread(std::string logName, Strategies strat) {
    while (reconfiguring) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
    }

    ownSendBuffer->clearBuffer();

    while (metaInfo[(metaInfo.size() / 2)] != rdma_ready) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10ms);
    }

    setOpcode(metaInfo.size() / 2, rdma_multi_thread, true);

    while (metaInfo[(metaInfo.size() / 2)] != rdma_ready) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10ms);
    }

    std::vector<std::thread *> pool;
    size_t thread_cnt = (int)(bufferConfig.num_remote_receive / 2);
    thread_cnt = thread_cnt > 0 ? thread_cnt : 1;
    bool *ready_vec = (bool *)malloc(thread_cnt * sizeof(bool));
    std::vector<std::vector<package_t *>> work_queue(thread_cnt);

    /* provide data to remote */
    std::size_t maxDataElements = 1ull << MAX_DATA_SIZE;
    DataProvider d;
    d.generateDummyData(maxDataElements >> 1);
    std::ofstream out;
    out.open(logName, std::ios_base::app);

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            memset(ready_vec, 0, thread_cnt * sizeof(bool));
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ConsumeTest-MultiThreaded] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ConsumeTest-MultiThreaded] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;
            size_t maxPackNum;
            work_queue.resize(thread_cnt);
            uint64_t packageID = generatePackageID();

            package_t package(packageID, maxDataToWrite, 1, 0, type_package, remainingSize, 0, copy);

            while (remainingSize > 0) {
                maxPackNum = thread_cnt;
                for (size_t pack = 1; pack <= thread_cnt; ++pack) {
                    if (remainingSize < maxPayloadSize * pack) {
                        maxPackNum = pack;
                        break;
                    }
                }

                for (size_t rbi = 0; rbi < maxPackNum; ++rbi) {
                    if (remainingSize <= maxDataToWrite) {
                        package.setCurrentPackageSize(remainingSize);
                        remainingSize = 0;
                    }
                    package_t *newPackage = package.deep_copy();

                    work_queue[rbi].push_back(newPackage);

                    if (remainingSize > maxDataToWrite) {
                        remainingSize -= maxDataToWrite;
                        package.advancePayloadPtr(maxDataToWrite);
                    }
                }
            }

            std::promise<void> p;
            std::shared_future<void> ready_future(p.get_future());

            for (size_t tid = 0; tid < thread_cnt; ++tid) {
                if (strat == Strategies::push) {
                    auto work = [this](size_t tid, std::vector<package_t *> *queue, bool *local_ready, std::shared_future<void> *sync_barrier, size_t thrdcnt) -> void {
                        local_ready[tid] = true;
                        bool packageSent = false;
                        sync_barrier->wait();

                        for (package_t *pack : *queue) {
                            packageSent = false;
                            while (!packageSent) {
                                for (size_t id = tid; id < bufferConfig.num_remote_receive; id += thrdcnt) {
                                    size_t remoteIndex = id + (metaInfo.size() / 2);
                                    if (metaInfo[remoteIndex] == rdma_ready) {
                                        setOpcode(remoteIndex, rdma_sending, false);
                                        ownSendBuffer->loadPackage(ownSendBuffer->buf + (tid * bufferConfig.size_remote_receive), pack);
                                        ownSendBuffer->sendPackage(pack, res.remote_buffer[id], res.remote_rkeys[id], res.qp, ownSendBuffer->buf + (tid * bufferConfig.size_remote_receive), id);
                                        poll_completion();

                                        setOpcode(remoteIndex, rdma_data_finished, true);

                                        free(pack);
                                        packageSent = true;
                                        break;
                                    }
                                }
                            }
                        }
                    };

                    pool.emplace_back(new std::thread(work, tid, &(work_queue[tid]), ready_vec, &ready_future, thread_cnt));
                } else if (strat == Strategies::pull) {
                    auto work = [this](size_t tid, std::vector<package_t *> *queue, bool *local_ready, std::shared_future<void> *sync_barrier, size_t thrdcnt) -> void {
                        local_ready[tid] = true;
                        bool packageSent = false;
                        sync_barrier->wait();

                        for (package_t *pack : *queue) {
                            packageSent = false;
                            while (!packageSent) {
                                for (size_t id = tid; id < bufferConfig.num_remote_receive; id += thrdcnt) {
                                    size_t remoteIndex = id + (metaInfo.size() / 2);
                                    if (metaInfo[remoteIndex] == rdma_ready) {
                                        setOpcode(remoteIndex, rdma_sending, false);
                                        ownSendBuffer->loadPackage(ownSendBuffer->buf + (tid * bufferConfig.size_remote_receive), pack);

                                        setOpcode(remoteIndex, rdma_pull_consume, true);

                                        free(pack);
                                        packageSent = true;
                                        break;
                                    }
                                }
                            }
                        }
                    };

                    pool.emplace_back(new std::thread(work, tid, &(work_queue[tid]), ready_vec, &ready_future, thread_cnt));
                } else {
                    return 1;
                }
            }

            bool all_ready = false;
            while (!all_ready) {
                /* Wait until all threads are ready to go before we pull the trigger */
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1ms);
                all_ready = true;
                for (size_t i = 0; i < thread_cnt; ++i) {
                    all_ready &= ready_vec[i];
                }
            }

            auto s_ts = std::chrono::high_resolution_clock::now();
            p.set_value();                                                   /* Start execution by notifying on the void promise */
            std::for_each(pool.begin(), pool.end(), [](std::thread *t) { t->join(); delete t; }); /* Join and delete threads as soon as they are finished */

            auto e_ts = std::chrono::high_resolution_clock::now();
            auto transfertime_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

            work_queue.clear();
            pool.clear();
            auto datasize = elementCount * sizeof(uint64_t);

            typedef std::chrono::duration<double> d_sec;
            d_sec secs = e_ts - s_ts;

            std::cout << "[ConsumeTest-MultiThreaded] Communicated " << datasize << " Bytes (" << BtoMB(datasize) << " MB) in " << secs.count() << " s -- " << BtoMB(datasize) / (secs.count()) << " MB/s " << std::endl;

            auto readable_size = GetBytesReadable(datasize);

            std::cout.precision(15);
            std::cout.setf(std::ios::fixed, std::ios::floatfield);
            std::cout.setf(std::ios::showpoint);
            out << ownSendBuffer->bufferSize << "\t" << bufferConfig.size_remote_receive << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB(datasize) / (secs.count()) << std::endl
                << std::flush;
            std::cout.flags(f);
        }
    }
    std::cout << "[ConsumeTest-MultiThreaded] Finished." << std::endl;
    out.close();
    busy = false;
    free(ready_vec);

    bool allDone = false;
    while (!allDone) {
        allDone = true;
        for (size_t c = 0; c < bufferConfig.num_remote_receive; ++c) {
            allDone &= (metaInfo[c + (metaInfo.size() / 2)] == rdma_ready);
        }
    }

    for (size_t c = 0; c < thread_cnt; ++c) {
        setOpcode(c + (metaInfo.size() / 2), rdma_test_finished, true);
    }

    return 0;
}