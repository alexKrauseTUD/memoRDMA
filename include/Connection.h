#ifndef MEMORDMA_RDMA_CONNECTION
#define MEMORDMA_RDMA_CONNECTION

#include <inttypes.h>

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <condition_variable>

#include "Buffer.h"
#include "util.h"

typedef std::function<void(const size_t)> ResetFunction;

// structure of system resources
struct resources {
    struct ibv_device_attr device_attr;  // device attributes
    struct ibv_port_attr port_attr;      // IB port attributes
    struct cm_con_data_t remote_props;   // values to connect to remote side
    struct ibv_context *ib_ctx;          // device handle
    struct ibv_pd *pd;                   // PD handle
    struct ibv_cq *cq;                   // CQ handle
    struct ibv_qp *qp;                   // QP handle
    std::vector<uint64_t> remote_receive_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_receive_rkeys;
    std::vector<uint64_t> remote_send_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_send_rkeys;
    int sock;  // TCP socket file descriptor
};

struct receive_data {
    bool done;
    uint64_t *localPtr;
    uint64_t size;
    std::chrono::_V2::system_clock::time_point endTime;
};

typedef std::chrono::_V2::system_clock::time_point timePoint;

class Connection {
   public:
    explicit Connection(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId);
    ~Connection();

    void init();

    size_t maxBytesInPayload(const size_t customMetaDataSize) const;

    int sendData(char *data, std::size_t dataSize, char *appMetaData, size_t appMetaDataSize, uint8_t opcode, Strategies strat);

    int sendOpcode(uint8_t opcode, bool sendToRemote);

    int closeConnection(bool send_remote = true);

    void receiveDataFromRemote(const size_t index, bool consu, Strategies strat);

    int addReceiveBuffer(std::size_t quantity, bool own);
    int removeReceiveBuffer(std::size_t quantity, bool own);
    int resizeReceiveBuffer(std::size_t newSize, bool own);
    int resizeSendBuffer(std::size_t newSize, bool own);

    int sendReconfigureBuffer(buffer_config_t &bufConfig);
    int receiveReconfigureBuffer(const uint8_t index);

    void printConnectionInfo() const;

    int throughputBenchmark(std::string logName, Strategies strat);
    int consumingBenchmark(std::string logName, Strategies strat);

   private:
    config_t config;
    buffer_config_t bufferConfig;
    uint32_t localConId;
    resources res;

    std::mutex receiveBufferCheckMutex;
    std::mutex sendBufferCheckMutex;
    std::mutex receiveBufferBlockMutex;
    std::mutex sendBufferBlockMutex;
    std::mutex reconfigureMutex;
    std::mutex idGeneratorMutex;

    std::condition_variable reconfigureCV;
    bool reconfigureDone = false;

    std::vector<std::unique_ptr<ReceiveBuffer>> ownReceiveBuffer;
    std::vector<std::unique_ptr<SendBuffer>> ownSendBuffer;

    std::array<uint8_t, 16> metaInfoReceive{0};
    std::array<uint8_t, 16> metaInfoSend{0};

    struct ibv_mr *metaInfoReceiveMR;
    struct ibv_mr *metaInfoSendMR;

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd, void* buf, size_t bufferSize);

    void setupSendBuffer();
    void setupReceiveBuffer();

    void initTCP();
    void exchangeBufferInfo();
    void createResources();
    struct ibv_context *createContext();
    struct ibv_qp *createQueuePair(struct ibv_pd *pd, struct ibv_cq *cq);
    int changeQueuePairStateToInit(struct ibv_qp *queue_pair);
    int changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id);
    int changeQueuePairStateToRTS(struct ibv_qp *qp);
    int pollCompletion();

    reconfigure_data reconfigureBuffer(buffer_config_t &bufConfig);
    void ackReconfigureBuffer(size_t index);

    int findNextFreeReceiveAndBlock();
    int findNextFreeSendAndBlock();
    int getNextFreeReceive();
    int getNextFreeSend();
    void setReceiveOpcode(const size_t index, uint8_t opcode, bool sendToRemote);
    void setSendOpcode(const size_t index, uint8_t opcode, bool sendToRemote);
    uint64_t generatePackageID();

    int __sendData(const size_t index, Strategies strat);

    std::atomic<bool> globalReceiveAbort;
    std::atomic<bool> globalSendAbort;

    void destroyResources();

    ResetFunction reset_buffer;
    std::function<void(std::atomic<bool> *, size_t tid, size_t thrdcnt)> check_receive;
    std::function<void(std::atomic<bool> *, size_t tid, size_t thrdcnt)> check_send;

    std::mt19937_64 randGen;

    std::vector<std::unique_ptr<std::thread>> readWorkerPool;
    std::vector<std::unique_ptr<std::thread>> sendWorkerPool;

    static const std::size_t TEST_ITERATIONS = 10;
    static const std::size_t MAX_DATA_SIZE = 31;
};

#endif  // MEMORDMA_RDMA_CONNECTION