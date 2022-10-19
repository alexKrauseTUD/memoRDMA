#ifndef MEMORDMA_RDMA_CONNECTION
#define MEMORDMA_RDMA_CONNECTION

#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <functional>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "Buffer.h"
#include "Logger.h"
#include "Utility.h"

using namespace memordma;

#define MAX_POLL_CQ_TIMEOUT 30000  // ms

enum class CompletionType : uint8_t {
    useDataCq,
    useMetaCq
};

enum class BenchmarkType : uint8_t {
    throughput,
    consume
};

// structure of test parameters
struct config_t {
    std::string deviceName;  // IB device name
    std::string serverName;  // server hostname
    uint32_t tcpPort;        // server TCP port
    bool clientMode;         // Don't run an event loop
    int32_t infiniBandPort;  // local IB port to work with
    int32_t gidIndex;        // GID index to use
};

struct buffer_config_t {
    uint8_t num_own_send_threads = 1;
    uint8_t num_own_receive_threads = 1;
    uint8_t num_remote_send_threads = 1;
    uint8_t num_remote_receive_threads = 1;
    uint8_t num_own_receive = 1;
    uint64_t size_own_receive = 640;
    uint8_t num_remote_receive = 1;
    uint64_t size_remote_receive = 640;
    uint8_t num_own_send = 1;
    uint64_t size_own_send = 640;
    uint8_t num_remote_send = 1;
    uint64_t size_remote_send = 640;
    uint8_t meta_info_size = 16;
};

struct BufferConnectionData {
    buffer_config_t bufferConfig;
    uint64_t sendBuffers[8]{0, 0, 0, 0, 0, 0, 0, 0};     // buffer address
    uint32_t sendRkeys[8]{0, 0, 0, 0, 0, 0, 0, 0};       // remote key
    uint64_t receiveBuffers[8]{0, 0, 0, 0, 0, 0, 0, 0};  // buffer address
    uint32_t receiveRkeys[8]{0, 0, 0, 0, 0, 0, 0, 0};    // remote key
};

// // structure to exchange data which is needed to connect the QPs
struct ConnectionData {
    uint64_t metaReceiveBuffer;
    uint64_t metaSendBuffer;
    uint32_t metaReceiveRkey;
    uint32_t metaSendRkey;
    BufferConnectionData bufferConnectionData;
    uint32_t dataQpNum;  // QP number
    uint32_t metaQpNum;  // QP number
    uint16_t lid;        // LID of the IB port
    uint8_t gid[16];     // GID
} __attribute__((packed));

typedef std::function<void(const size_t)> ResetFunction;

// structure of system resources
struct resources {
    struct ibv_device_attr device_attr;           // device attributes
    struct ibv_port_attr port_attr;               // IB port attributes
    struct ConnectionData remote_props;           // values to connect to remote side
    struct ibv_context *ib_ctx;                   // device handle
    struct ibv_pd *dataPd;                        // PD handle
    struct ibv_pd *metaPd;                        // PD handle
    struct ibv_cq *dataCq;                        // CQ handle
    struct ibv_cq *metaCq;                        // CQ handle
    struct ibv_qp *dataQp;                        // QP handle
    struct ibv_qp *metaQp;                        // QP handle
    std::vector<uint64_t> remote_receive_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_receive_rkeys;
    std::vector<uint64_t> remote_send_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_send_rkeys;
    int sock;  // TCP socket file descriptor
};

typedef std::chrono::_V2::system_clock::time_point timePoint;

class Connection {
   public:
    virtual ~Connection();

    void init();

    size_t maxBytesInPayload(const size_t customMetaDataSize) const;

    int sendData(char *data, std::size_t dataSize, char *appMetaData, size_t appMetaDataSize, uint8_t opcode);

    int sendOpcode(uint8_t opcode, bool sendToRemote);

    int closeConnection(bool send_remote = true);

    virtual void consumeData(const size_t index) = 0;

    void validateBufferConfig(buffer_config_t &bufConfig);

    virtual int sendReconfigureBuffer(buffer_config_t &bufConfig) = 0;
    virtual int receiveReconfigureBuffer(const uint8_t index) = 0;

    void printConnectionInfo() const;

    int benchmark(const std::string shortName, const std::string name, const BenchmarkType benchType);

    static int sockConnect(std::string client_name, uint32_t *port);
    static int sockSyncData(int sockfd, int xfer_size, char *local_data, char *remote_data);
    static buffer_config_t invertBufferConfig(buffer_config_t bufferConfig);
    static void sockCloseFd(int &sockfd);
    static int receiveTcp(int sockfd, int xfer_size, char *remote_data);
    static int sendTcp(int sockfd, int xfer_size, char *local_data);

   protected:
    explicit Connection(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId);

    config_t config;
    buffer_config_t bufferConfig;
    uint32_t localConId;
    resources res;

    std::shared_mutex receiveBufferCheckMutex;
    std::shared_mutex sendBufferCheckMutex;
    std::mutex receiveBufferBlockMutex;
    std::mutex sendBufferBlockMutex;
    std::mutex remoteSendBufferBlockMutex;
    std::mutex reconfigureMutex;
    std::mutex closingMutex;
    std::mutex idGeneratorMutex;

    std::condition_variable reconfigureCV;
    bool reconfigureDone = false;
    bool connectionClosed = false;

    std::vector<std::unique_ptr<ReceiveBuffer>> ownReceiveBuffer;
    std::vector<std::unique_ptr<SendBuffer>> ownSendBuffer;

    std::array<uint8_t, 16> metaInfoReceive{0};
    std::array<uint8_t, 16> metaInfoSend{0};

    struct ibv_mr *metaInfoReceiveMR;
    struct ibv_mr *metaInfoSendMR;

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd, void *buf, size_t bufferSize);

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

    template <CompletionType compType>
    uint64_t pollCompletion();

    BufferConnectionData reconfigureBuffer(buffer_config_t &bufConfig);
    virtual void ackReconfigureBuffer(size_t index) = 0;

    int findNextFreeReceiveAndBlock();
    int findNextFreeSendAndBlock();
    int getNextFreeReceive();
    int getNextFreeSend();
    void setReceiveOpcode(const size_t index, uint8_t opcode, bool sendToRemote);
    void setSendOpcode(const size_t index, uint8_t opcode, bool sendToRemote);
    uint64_t generatePackageID();

    virtual int __sendData(const size_t index) = 0;

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
    static const std::uint64_t MAX_DATA_ELEMENTS = 1ull << 31;
};

class ConnectionPush : public Connection {
   public:
    explicit ConnectionPush(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId);
    ~ConnectionPush();
    
    int sendReconfigureBuffer(buffer_config_t &bufConfig);
    int receiveReconfigureBuffer(const uint8_t index);

    void consumeData(const size_t index);

   private:
    int __sendData(const size_t index);

    void ackReconfigureBuffer(size_t index);
};

class ConnectionPull : public Connection {
   public:
    explicit ConnectionPull(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId);
    ~ConnectionPull();

    int sendReconfigureBuffer(buffer_config_t &bufConfig);
    int receiveReconfigureBuffer(const uint8_t index);

    void consumeData(const size_t index);

   private:
    int __sendData(const size_t index);

    int getNextReadyToPullSend();
    uint64_t findNextReadyToPullSendAndBlock();

    void ackReconfigureBuffer(size_t index);
};

/**
 * @brief Poll the CQ for a single event. This function will continue to poll the queue until MAX_POLL_TIMEOUT ms have passed.
 *
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
template <CompletionType compType>
uint64_t Connection::pollCompletion() {
    struct ibv_wc wc;
    unsigned long start_time_ms;
    unsigned long curr_time_ms;
    struct timeval curr_time;
    int poll_result;

    // poll the completion for a while before giving up of doing it
    gettimeofday(&curr_time, NULL);
    start_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    do {
        if (compType == CompletionType::useDataCq) {
            poll_result = ibv_poll_cq(res.dataCq, 1, &wc);
        } else if (compType == CompletionType::useMetaCq) {
            poll_result = ibv_poll_cq(res.metaCq, 1, &wc);
        }
        gettimeofday(&curr_time, NULL);
        curr_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    } while ((poll_result == 0) &&
             ((curr_time_ms - start_time_ms) < MAX_POLL_CQ_TIMEOUT));

    if (poll_result < 0) {
        // poll CQ failed
        LOG_ERROR("poll CQ failed\n");
        goto die;
    } else if (poll_result == 0) {
        LOG_ERROR("Completion wasn't found in the CQ after timeout\n");
        goto die;
    } else {
        // CQE found
        // LOG_INFO("Completion was found in CQ with status 0x%x\n", wc.status);
    }

    if (wc.status != IBV_WC_SUCCESS) {
        LOG_ERROR("Got bad completion with status: " << ibv_wc_status_str(wc.status) << " (" << std::hex << wc.status << ") vendor syndrome: 0x" << std::hex << wc.vendor_err << std::endl;)
        goto die;
    }

    // FIXME: ;)
    return wc.wr_id;
die:
    exit(EXIT_FAILURE);
}

#endif  // MEMORDMA_RDMA_CONNECTION