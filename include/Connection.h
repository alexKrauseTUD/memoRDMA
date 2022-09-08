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

#include "Buffer.h"
#include "util.h"

typedef std::function<void(const size_t)> ResetFunction;

// structure of system resources
struct resources {
    struct ibv_device_attr device_attr;           // device attributes
    struct ibv_port_attr port_attr;               // IB port attributes
    struct cm_con_data_t remote_props;            // values to connect to remote side
    struct ibv_context *ib_ctx;                   // device handle
    struct ibv_pd *pd;                            // PD handle
    struct ibv_cq *cq;                            // CQ handle
    struct ibv_qp *qp;                            // QP handle
    std::vector<struct ibv_mr *> own_receive_mr;  // MR handle for buf
    std::vector<char *> own_receive_buffer;       // memory buffer pointer, used for RDMA send ops
    std::vector<uint64_t> remote_receive_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_receive_rkeys;
    std::vector<struct ibv_mr *> own_send_mr;  // MR handle for buf
    std::vector<char *> own_send_buffer;       // memory buffer pointer, used for RDMA send ops
    std::vector<uint64_t> remote_send_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_send_rkeys;
    int sock;  // TCP socket file descriptor
};

struct receive_data {
    bool done;
    uint64_t *localPtr;
    DataTypes dt;
    uint64_t size;
    std::chrono::_V2::system_clock::time_point endTime;
};

enum class ConnectionStatus {
    active,
    reconfigure
};

typedef std::chrono::_V2::system_clock::time_point timePoint;

class Connection {
   public:
    explicit Connection(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId);
    ~Connection();

    config_t config;
    buffer_config_t bufferConfig;
    uint32_t localConId;
    resources res;
    
    ConnectionStatus conStat;
    std::mutex receive_buffer_check_mutex;
    std::mutex send_buffer_check_mutex;
    std::mutex receive_buffer_lock_mutex;
    std::mutex send_buffer_lock_mutex;

    bool busy;

    std::vector<ReceiveBuffer *> ownReceiveBuffer;
    std::vector<SendBuffer *> ownSendBuffer;

    void init();

    size_t maxBytesInPayload(const size_t customMetaDataSize) const;

    // int sendData(std::string &data);
    // int sendData(package_t *p);
    int sendData(char *data, std::size_t dataSize, char *appMetaData, size_t appMetaDataSize, uint8_t opcode, Strategies strat);

    int sendOpcode(uint8_t opcode, bool sendToRemote);

    int closeConnection(bool send_remote = true);
    void destroyResources();

    void receiveDataFromRemote(const size_t index, bool consu, Strategies strat);
    void pullDataFromRemote(const size_t index, bool consume);

    int addReceiveBuffer(std::size_t quantity, bool own);
    int removeReceiveBuffer(std::size_t quantity, bool own);
    int resizeReceiveBuffer(std::size_t newSize, bool own);
    int resizeSendBuffer(std::size_t newSize, bool own);

    int reconfigureBuffer(buffer_config_t &bufConfig);
    int sendReconfigureBuffer(buffer_config_t &bufConfig);
    int receiveReconfigureBuffer();
    // int receiveReconfigureBuffer(std::size_t index);

    int pendingBufferCreation();

    void printConnectionInfo();

    int throughputTest(std::string logName, Strategies strat);
    int consumingTest(std::string logName, Strategies strat);
    int throughputTestMultiThread(std::string logName, Strategies strat);
    int consumingTestMultiThread(std::string logName, Strategies strat);

    void consume(const size_t index);
    void workMultiThread();

    // std::atomic<ConnectionStatus> conStat;

   private:
    std::map<uint64_t, receive_data> receiveMap;

    std::array<uint8_t, 16> metaInfoReceive{0};
    std::array<uint8_t, 16> metaInfoSend{0};

    struct ibv_mr *metaInfoReceiveMR;
    struct ibv_mr *metaInfoSendMR;

    void setupSendBuffer();
    void setupReceiveBuffer();

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd, void *buffer, std::size_t size);

    void initTCP();
    void exchangeBufferInfo();
    void createResources();
    struct ibv_context *createContext();
    struct ibv_qp *createQueuePair(struct ibv_pd *pd, struct ibv_cq *cq);
    int changeQueuePairStateToInit(struct ibv_qp *queue_pair);
    int changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id);
    int changeQueuePairStateToRTS(struct ibv_qp *qp);
    int poll_completion();

    int getNextFreeReceive();
    int getNextFreeSend();
    int findNextFreeReceiveAndBlock();
    int findNextFreeSendAndBlock();
    uint32_t getOwnSendToRemoteReceiveRatio();
    void setReceiveOpcode(const size_t index, uint8_t opcode, bool sendToRemote);
    void setSendOpcode(const size_t index, uint8_t opcode, bool sendToRemote);
    uint64_t generatePackageID();

    int __sendData(const size_t index, Strategies strat);

    std::tuple<timePoint, timePoint> throughputTestPush(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);
    std::tuple<timePoint, timePoint> consumingTestPush(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);
    std::tuple<timePoint, timePoint> throughputTestMultiThreadPush(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);
    std::tuple<timePoint, timePoint> consumingTestMultiThreadPush(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);

    std::tuple<timePoint, timePoint> throughputTestPull(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);
    std::tuple<timePoint, timePoint> consumingTestPull(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);
    std::tuple<timePoint, timePoint> throughputTestMultiThreadPull(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);
    std::tuple<timePoint, timePoint> consumingTestMultiThreadPull(package_t &package, uint64_t remainingSize, uint64_t maxPayloadSize, uint64_t maxDataToWrite);

    std::atomic<bool> globalReceiveAbort;
    std::atomic<bool> globalSendAbort;
    std::atomic<bool> reconfiguring;

    std::function<void(std::atomic<bool> *, size_t tid, size_t thrdcnt)> check_receive;
    std::function<void(std::atomic<bool> *, size_t tid, size_t thrdcnt)> check_send;
    ResetFunction reset_buffer;

    std::mt19937_64 randGen;

    std::vector<std::thread *> readWorkerPool;
    std::vector<std::thread *> sendWorkerPool;

    static const std::size_t TEST_ITERATIONS = 10;
    static const std::size_t MAX_DATA_SIZE = 31;
};

#endif  // MEMORDMA_RDMA_CONNECTION