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
    struct ibv_device_attr device_attr;      // device attributes
    struct ibv_port_attr port_attr;          // IB port attributes
    struct cm_con_data_t remote_props;       // values to connect to remote side
    struct ibv_context *ib_ctx;              // device handle
    struct ibv_pd *pd;                       // PD handle
    struct ibv_cq *cq;                       // CQ handle
    struct ibv_qp *qp;                       // QP handle
    std::vector<struct ibv_mr *> own_mr;     // MR handle for buf
    std::vector<char *> own_buffer;          // memory buffer pointer, used for RDMA send ops
    std::vector<uint64_t> remote_buffer;       // memory buffer pointer, used for RDMA send ops
    std::vector<uint32_t> remote_rkeys;
    int sock;                                // TCP socket file descriptor
};

struct receive_data {
    bool done;
    uint64_t* localPtr;
    data_types dt;
    uint64_t size;
};

class Connection {
   public:
    explicit Connection(config_t _config, buffer_config_t _bufferConfig);
    ~Connection();

    config_t config;
    buffer_config_t bufferConfig;
    struct resources res;

    bool busy;

    SendBuffer *ownSendBuffer = NULL;
    SendBuffer *remoteSendBuffer = NULL;

    std::vector<ReceiveBuffer *> ownReceiveBuffer;
    std::vector<ReceiveBuffer *> remoteReceiveBuffer;

    std::map<uint64_t, receive_data> receiveMap;

    std::array<uint16_t, 16> metaInfo;
    struct ibv_mr *metaInfoMR;

    void setupSendBuffer();
    void setupReceiveBuffer();

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd, void *buffer, size_t size);

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

    bool sendData(std::string &data);
    bool sendData(package_t* p);
    bool closeConnection(bool send_remote = true);

    void receiveDataFromRemote(size_t index);

    bool addReceiveBuffer(unsigned int quantity);
    bool removeReceiveBuffer(unsigned int quantity);
    bool resizeReceiveBuffer(std::size_t newSize);
    bool resizeSendBuffer(std::size_t newSize);

    bool pendingBufferCreation();

    int getNextFreeReceive();
    uint32_t getOwnSendToRemoteReceiveRatio();
    void setOpcode(size_t index, rdma_handler_communication opcode, bool sendToRemote);
    uint64_t generatePackageID();

    bool throughputTest(std::string logName);

   private:
    bool globalAbort;

    std::function< void (bool*) > check_receive;
    std::function< void (bool*) > check_regions;

    std::thread* readWorker;
    std::thread* creationWorker;
};

#endif  // MEMORDMA_RDMA_CONNECTION