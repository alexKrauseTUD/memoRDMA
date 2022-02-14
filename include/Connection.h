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
    std::vector<struct ibv_mr *> remote_mr;  // MR handle for buf
    std::vector<char *> own_buffer;          // memory buffer pointer, used for RDMA send ops
    std::vector<char *> remote_buffer;       // memory buffer pointer, used for RDMA send ops
    int sock;                                // TCP socket file descriptor
};

class Connection {
   public:
    Connection();
    Connection(config_t _config, buffer_config_t _bufferConfig);
    ~Connection();

    config_t config;
    buffer_config_t bufferConfig;
    struct resources res;

    SendBuffer *ownSendBuffer;
    SendBuffer remoteSendBuffer;

    std::vector<ReceiveBuffer *> ownReceiveBuffer;
    std::vector<ReceiveBuffer> remoteReceiveBuffer;

    std::array<uint16_t, 16> metaInfo;
    struct ibv_mr *metaInfoMR;

    void setupSendBuffer();
    void setupReceiveBuffer();

    struct ibv_mr *registerMemoryRegion(struct ibv_pd *pd, void *buffer, size_t size);

    void initTCP();
    void createResources();
    struct ibv_context *createContext();
    struct ibv_qp *createQueuePair(struct ibv_pd *pd, struct ibv_cq *cq);
    bool changeQueuePairStateToInit(struct ibv_qp *queue_pair);
    bool changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id);
    int changeQueuePairStateToRTS(struct ibv_qp *qp);
    void connectQpTCP(config_t &config);
    int poll_completion();

    bool sendData(std::string &data);
    bool close();

    bool addReceiveBuffer(unsigned int quantity);
    bool removeReceiveBuffer(unsigned int quantity);
    bool resizeReceiveBuffer(std::size_t newSize);
    bool resizeSendBuffer(std::size_t newSize);

    bool pendingBufferCreation();

   private:
    bool globalAbort;
};

#endif  // MEMORDMA_RDMA_CONNECTION