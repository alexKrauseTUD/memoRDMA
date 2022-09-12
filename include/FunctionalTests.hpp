#ifndef TUDDBS_MEMORDMA_INCLUDE_FUNCTIONAL_TESTS_HPP
#define TUDDBS_MEMORDMA_INCLUDE_FUNCTIONAL_TESTS_HPP

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>

#include "Connection.h"
#include "ConnectionManager.h"
#include "DataProvider.h"

struct ReceiveData {
    uint64_t receivedBytes = 0;
    uint64_t result = 0;
};

class FunctionalTests {
   public:
    static FunctionalTests& getInstance() {
        static FunctionalTests instance;
        return instance;
    }
    ~FunctionalTests();

    uint8_t executeAllTests();
    uint8_t dataTransferTest(std::ofstream& out);
    uint8_t bufferReconfigurationTest(std::ofstream& out);

   private:
    FunctionalTests();

    const uint64_t elementCount = 200000000;
    const uint64_t dataSize = elementCount * sizeof(uint64_t);
    const uint8_t parallelExecutions = 3;

    std::mutex mapMutex;
    std::mutex resultWaitMutex;
    std::condition_variable resultWaitCV;
    bool resultsArrived = false;

    std::map<uint64_t, ReceiveData> receiveMap;

    CallbackFunction receiveDataTransferTest;
    CallbackFunction dataTransferTestAck;
};

#endif