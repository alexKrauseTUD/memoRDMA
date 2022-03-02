#ifndef MEMORDMA_RDMA_CONNECTION
#define MEMORDMA_RDMA_CONNECTION

#include <inttypes.h>

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "Buffer.h"
#include "util.h"

// structure of system resources
struct resources {
    struct ibv_device_attr device_attr;   // device attributes
    struct ibv_port_attr port_attr;       // IB port attributes
    struct cm_con_data_t remote_props;    // values to connect to remote side
    struct ibv_context *ib_ctx;           // device handle
    struct ibv_pd *pd;                    // PD handle
    struct ibv_cq *cq;                    // CQ handle
    struct ibv_qp *qp;                    // QP handle
    std::vector<struct ibv_mr *> own_mr;  // MR handle for buf
    std::vector<char *> own_buffer;       // memory buffer pointer, used for RDMA send ops
    std::vector<uint64_t> remote_buffer;  // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_rkeys;
    int sock;  // TCP socket file descriptor
};

struct receive_data {
    bool done;
    uint64_t *localPtr;
    data_types dt;
    uint64_t size;
    std::chrono::_V2::system_clock::time_point endTime;
};

enum connection_status {
    active = 1,
    closing = 2,
    reinitialize = 3
};

class Connection {
   public:
    explicit Connection(config_t _config, buffer_config_t _bufferConfig);
    ~Connection();

    config_t config;
    buffer_config_t bufferConfig;
    resources res;

    bool busy;

    SendBuffer *ownSendBuffer = NULL;

    std::vector<ReceiveBuffer *> ownReceiveBuffer;

    std::map<uint64_t, receive_data> receiveMap;

    std::array<uint16_t, 16> metaInfo{0};
    
    struct ibv_mr *metaInfoMR;

    void setupSendBuffer();
    void setupReceiveBuffer();

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd, void *buffer, size_t size);

    void init(bool reinitialize = false);

    void initTCP();
    void exchangeBufferInfo();
    void createResources();
    struct ibv_context *createContext();
    struct ibv_qp *createQueuePair(struct ibv_pd *pd, struct ibv_cq *cq);
    int changeQueuePairStateToInit(struct ibv_qp *queue_pair);
    int changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id);
    int changeQueuePairStateToRTS(struct ibv_qp *qp);
    void connectQpTCP();
    int poll_completion();

    int sendData(std::string &data);
    int sendData(package_t *p);
    int closeConnection(bool send_remote = true, bool sendReinitializ = false);
    void destroyResources();

    void receiveDataFromRemote(size_t index);

    int addReceiveBuffer(unsigned int quantity);
    int removeReceiveBuffer(unsigned int quantity);
    int resizeReceiveBuffer(std::size_t newSize);
    int resizeSendBuffer(std::size_t newSize);

    int pendingBufferCreation();

    int getNextFreeReceive();
    uint32_t getOwnSendToRemoteReceiveRatio();
    void setOpcode(size_t index, rdma_handler_communication opcode, bool sendToRemote);
    uint64_t generatePackageID();

    int throughputTest(std::string logName);
    int consumingTest(std::string logName);
    int throughputTestMultiThread(std::string logName);
    int consumingTestMultiThread(std::string logName);

    void consume(size_t index);
    void consumeMultiThread(size_t index);

    std::atomic<connection_status> conStat;

   private:
    std::atomic<bool> globalAbort;

    std::function<void(std::atomic<bool> *)> check_receive;
    std::function<void(bool *)> check_regions;
    std::function<void(bool *)> check_receive_done;

    std::thread *readWorker;
    std::thread *creationWorker;
    std::thread *receiveDoneWorker;
};

#endif  // MEMORDMA_RDMA_CONNECTION