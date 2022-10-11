#include "Connection.h"

#include <ConnectionManager.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <csignal>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <vector>

#include "DataProvider.h"
#include "Logger.h"
#include "Utility.h"

using namespace memordma;

Connection::Connection(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId) : globalReceiveAbort(false), globalSendAbort(false) {
    config = _config;
    validateBufferConfig(_bufferConfig);
    bufferConfig = _bufferConfig;
    localConId = _localConId;
    res.sock = -1;

    // for resetting the buffer -> this is needed for the callbacks as they do not have access to the necessary structures
    reset_buffer = [this](const size_t i) -> void {
        // ownReceiveBuffer[i]->clearBuffer();
        setReceiveOpcode(i, rdma_ready, true);
    };

    // for the receiving threads -> check whether a RB is ready to be consumed
    check_receive = [this](std::atomic<bool> *abort, size_t tid, size_t thrdcnt) -> void {
        LOG_INFO("[check_receive] Starting monitoring thread " << tid + 1 << "/" << +thrdcnt << " for receiving on connection!" << std::endl);
        size_t metaSizeHalf = metaInfoReceive.size() / 2;

        // currently this works with (busy) waiting -> TODO: conditional variable with wait
        while (!*abort) {
            // std::this_thread::sleep_for(1000ms);
            for (size_t i = tid; i < metaSizeHalf; i += thrdcnt) {
                if (ConnectionManager::getInstance().hasCallback(metaInfoReceive[i])) {
                    // LOG_DEBUG1("[Connection] Invoking custom callback for code " << (size_t)metaInfoReceive[i] << std::endl);

                    // Handle the call
                    auto cb = ConnectionManager::getInstance().getCallback(metaInfoReceive[i]);
                    cb(localConId, ownReceiveBuffer[i].get(), std::bind(reset_buffer, i));

                    continue;
                }
                switch (metaInfoReceive[i]) {
                    case rdma_no_op:
                    case rdma_ready:
                    case rdma_working: {
                        continue;
                    }; break;
                    case rdma_data_finished: {
                        consumeData(i);
                    }; break;
                    case rdma_pull_read: {
                        readDataFromRemote(i, false);
                    } break;
                    case rdma_pull_consume: {
                        readDataFromRemote(i, true);
                    } break;
                    case rdma_reconfigure: {
                        auto recFunc = [this](size_t index) {
                            receiveReconfigureBuffer(index);
                        };
                        setReceiveOpcode(i, rdma_reconfiguring, false);
                        std::thread(recFunc, i).detach();
                    } break;
                    case rdma_reconfigure_ack: {
                        ackReconfigureBuffer(i);
                    } break;
                    case rdma_shutdown: {
                        auto shutdown = []() { std::raise(SIGUSR1); };
                        std::thread(shutdown).detach();
                    }; break;
                    default: {
                        continue;
                    }; break;
                }
            }
        }

        LOG_INFO("[check_receive] Ending thread " << tid + 1 << "/" << +thrdcnt << " through global abort." << std::endl);
    };

    // for the sending threads -> check whether a SB is ready to be send
    check_send = [this](std::atomic<bool> *abort, size_t tid, size_t thrdcnt) -> void {
        LOG_INFO("[check_send] Starting monitoring thread " << tid + 1 << "/" << +thrdcnt << " for sending on connection!" << std::endl);
        size_t metaSizeHalf = metaInfoReceive.size() / 2;

        while (!*abort) {
            // std::this_thread::sleep_for(100ms);
            for (size_t i = tid; i < metaSizeHalf; i += thrdcnt) {
                switch (metaInfoSend[i]) {
                    case rdma_no_op:
                    case rdma_ready:
                    case rdma_working: {
                        continue;
                    }; break;
                    case rdma_ready_to_push: {
                        __sendData(i, Strategies::push);
                    }; break;
                    case rdma_ready_to_pull: {
                        __sendData(i, Strategies::pull);
                    }; break;
                    default: {
                        continue;
                    }; break;
                }
            }
        }

        LOG_INFO("[check_send] Ending thread " << tid + 1 << "/" << +thrdcnt << " through global abort." << std::endl);
    };

    init();
}

/**
 * @brief Initialization of the relevant structures. TCP connection is used for initial handshake. Buffers are created and informations exchanged.
 *
 */
void Connection::init() {
    metaInfoReceive = std::array<uint8_t, 16>{0};
    metaInfoSend = std::array<uint8_t, 16>{0};

    // initial TCP handshake to exchange very first informations
    initTCP();

    // exchange of buffer informations over TCP
    exchangeBufferInfo();

    // done with TCP so socket is closed -> sometimes this seems to not work 100 percent
    sockCloseFd(res.sock);

    // the buffers are set locally and remotly to ready (usable) -> this can not be done on creation as the remote meta structure is not known at this moment
    for (size_t rbi = 0; rbi < bufferConfig.num_own_receive; ++rbi) {
        setReceiveOpcode(rbi, rdma_ready, true);
    }
    for (size_t sbi = 0; sbi < bufferConfig.num_own_send; ++sbi) {
        setSendOpcode(sbi, rdma_ready, true);
    }

    // as much threads as wanted are spawned
    for (size_t tid = 0; tid < bufferConfig.num_own_receive_threads; ++tid) {
        readWorkerPool.emplace_back(std::make_unique<std::thread>(check_receive, &globalReceiveAbort, tid, bufferConfig.num_own_receive_threads));
    }

    for (size_t tid = 0; tid < bufferConfig.num_own_send_threads; ++tid) {
        sendWorkerPool.emplace_back(std::make_unique<std::thread>(check_send, &globalSendAbort, tid, bufferConfig.num_own_send_threads));
    }
}

/**
 * @brief Free all reserved resources when giving up a connection. Note: There might be a memory leak because something is not freed...
 *
 */
void Connection::destroyResources() {
    // end all receiving and sending threads
    globalReceiveAbort = true;
    std::for_each(readWorkerPool.begin(), readWorkerPool.end(), [](std::unique_ptr<std::thread> &t) { t->join(); });
    readWorkerPool.clear();
    globalReceiveAbort = false;
    globalSendAbort = true;
    std::for_each(sendWorkerPool.begin(), sendWorkerPool.end(), [](std::unique_ptr<std::thread> &t) { t->join(); });
    sendWorkerPool.clear();
    globalSendAbort = false;

    // delete RDMA buffers -> their destructors have to take care of their attributes!
    ownReceiveBuffer.clear();
    ownSendBuffer.clear();

    // deregister the meta info buffers
    if (metaInfoReceiveMR) {
        ibv_dereg_mr(metaInfoReceiveMR);
    }
    if (metaInfoSendMR) {
        ibv_dereg_mr(metaInfoSendMR);
    }

    // destroy RDMA resources
    ibv_destroy_qp(res.dataQp);
    ibv_destroy_qp(res.metaQp);
    ibv_destroy_cq(res.dataCq);
    ibv_destroy_cq(res.metaCq);
    ibv_dealloc_pd(res.dataPd);
    ibv_dealloc_pd(res.metaPd);
    ibv_close_device(res.ib_ctx);
}

/**
 * @brief Formatted print of buffer configuration.
 *
 */
void Connection::printConnectionInfo() const {
    LOG_INFO("Connection Information:\n"
             << "Remote IP:\t" << config.serverName << ":" << config.tcpPort << "\n"
             << "\t\tOwn S Threads:\t\t" << +bufferConfig.num_own_send_threads << "\n"
             << "\t\tOwn SB Number:\t\t" << +bufferConfig.num_own_send << "\n"
             << "\t\tOwn SB Size:\t\t" << bufferConfig.size_own_send << "\n"
             << "\t\tOwn R Threads:\t\t" << +bufferConfig.num_own_receive_threads << "\n"
             << "\t\tOwn RB Number:\t\t" << +bufferConfig.num_own_receive << "\n"
             << "\t\tOwn RB Size:\t\t" << bufferConfig.size_own_receive << "\n"
             << "\t\tRemote S Threads:\t" << +bufferConfig.num_remote_send_threads << "\n"
             << "\t\tRemote SB Number:\t" << +bufferConfig.num_remote_send << "\n"
             << "\t\tRemote SB Size:\t\t" << bufferConfig.size_remote_send << "\n"
             << "\t\tRemote R Threads:\t" << +bufferConfig.num_remote_receive_threads << "\n"
             << "\t\tRemote RB Number:\t" << +bufferConfig.num_remote_receive << "\n"
             << "\t\tRemote RB Size:\t\t" << bufferConfig.size_remote_receive << "\n"
             << std::endl);
}

/**
 * @brief Creating desired number of SBs and setting their opcode (locally as the remote meta info struct might not be known) to ready.
 *
 */
void Connection::setupSendBuffer() {
    for (size_t i = 0; i < bufferConfig.num_own_send; ++i) {
        ownSendBuffer.push_back(std::make_unique<SendBuffer>(bufferConfig.size_own_send));
        setSendOpcode(i, rdma_ready, false);
    }
}

/**
 * @brief Creating desired number of RBs and setting their opcode (locally as the remote meta info struct might not be known) to ready.
 *
 */
void Connection::setupReceiveBuffer() {
    for (size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
        ownReceiveBuffer.push_back(std::make_unique<ReceiveBuffer>(bufferConfig.size_own_receive));
        setReceiveOpcode(i, rdma_ready, false);
    }
}

/**
 * @brief Initial tcp connection between server and client.
 *
 */
void Connection::initTCP() {
    if (!config.clientMode) {
        // @server
        res.sock = sockConnect(config.serverName, &config.tcpPort);
        if (res.sock < 0) {
            LOG_ERROR("Failed to establish TCP connection to server " << config.serverName.c_str() << ", port " << config.tcpPort << std::endl);
            exit(EXIT_FAILURE);
        }
    } else {
        // @client
        res.sock = sockConnect("", &config.tcpPort);
        if (res.sock < 0) {
            LOG_ERROR("Failed to establish TCP connection with client on port " << +config.tcpPort << std::endl);
            exit(EXIT_FAILURE);
        }
    }
    LOG_INFO("TCP connection was established!" << std::endl);
}

/**
 * @brief Exchange the RDMA buffer infos with the remote server. This function is rather complex and might be splitted into several more readable subfunctions in the future.
 *
 */
void Connection::exchangeBufferInfo() {
    struct ConnectionData localConnectionData;
    struct ConnectionData remoteConnectionData;
    struct ConnectionData tempConnectionData;

    if (config.clientMode) {
        // Client waits on Server-Information as it is needed to create the buffers
        Utility::checkOrDie(receiveTcp(res.sock, sizeof(struct ConnectionData), (char *)&tempConnectionData));

        bufferConfig = invertBufferConfig(tempConnectionData.bufferConnectionData.bufferConfig);

        metaInfoReceive = std::array<uint8_t, 16>{0};
        metaInfoSend = std::array<uint8_t, 16>{0};
    }

    // Creating the local buffers for RDMA communication.
    setupSendBuffer();
    setupReceiveBuffer();

    // Creating the necessary RDMA resources locally.
    createResources();

    union ibv_gid myGid;
    memset(&myGid, 0, sizeof(myGid));

    if (config.gidIndex >= 0) {
        Utility::checkOrDie(ibv_query_gid(res.ib_ctx, config.infiniBandPort, config.gidIndex, &myGid));
    }

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    localConnectionData.metaReceiveBuffer = Utility::htonll((uintptr_t)&metaInfoReceive);
    localConnectionData.metaReceiveRkey = htonl(metaInfoReceiveMR->rkey);
    localConnectionData.metaSendBuffer = Utility::htonll((uintptr_t)&metaInfoSend);
    localConnectionData.metaSendRkey = htonl(metaInfoSendMR->rkey);

    // collect buffer information for RB
    auto pos = 0;
    for (const auto &rb : ownReceiveBuffer) {
        localConnectionData.bufferConnectionData.receiveBuffers[pos] = Utility::htonll((uintptr_t)rb->getBufferPtr());
        localConnectionData.bufferConnectionData.receiveRkeys[pos++] = htonl(rb->getMrPtr()->rkey);
    }

    // collect buffer information for SB
    pos = 0;
    for (const auto &sb : ownSendBuffer) {
        localConnectionData.bufferConnectionData.sendBuffers[pos] = Utility::htonll((uintptr_t)sb->getBufferPtr());
        localConnectionData.bufferConnectionData.sendRkeys[pos++] = htonl(sb->getMrPtr()->rkey);
    }

    localConnectionData.bufferConnectionData.bufferConfig = bufferConfig;

    localConnectionData.dataQpNum = htonl(res.dataQp->qp_num);
    localConnectionData.metaQpNum = htonl(res.metaQp->qp_num);
    localConnectionData.lid = htons(res.port_attr.lid);
    memcpy(localConnectionData.gid, &myGid, 16);

    if (!config.clientMode) {
        // Server sends information to Client and waits on Client-Information
        Utility::checkOrDie(sockSyncData(res.sock, sizeof(struct ConnectionData), (char *)&localConnectionData, (char *)&tempConnectionData));
    } else {
        // Client responds to server with own information
        Utility::checkOrDie(sendTcp(res.sock, sizeof(struct ConnectionData), (char *)&localConnectionData));
    }

    remoteConnectionData.metaReceiveBuffer = Utility::ntohll(tempConnectionData.metaReceiveBuffer);
    remoteConnectionData.metaReceiveRkey = ntohl(tempConnectionData.metaReceiveRkey);
    remoteConnectionData.metaSendBuffer = Utility::ntohll(tempConnectionData.metaSendBuffer);
    remoteConnectionData.metaSendRkey = ntohl(tempConnectionData.metaSendRkey);
    remoteConnectionData.bufferConnectionData.bufferConfig = invertBufferConfig(tempConnectionData.bufferConnectionData.bufferConfig);
    remoteConnectionData.dataQpNum = ntohl(tempConnectionData.dataQpNum);
    remoteConnectionData.metaQpNum = ntohl(tempConnectionData.metaQpNum);
    remoteConnectionData.lid = ntohs(tempConnectionData.lid);
    memcpy(remoteConnectionData.gid, tempConnectionData.gid, 16);

    // save the remote side attributes, we will need it for the post SR
    res.remote_props = remoteConnectionData;

    std::vector<uint64_t> tempReceiveBuf(tempConnectionData.bufferConnectionData.receiveBuffers, tempConnectionData.bufferConnectionData.receiveBuffers + 8);
    std::vector<uint32_t> tempReceiveRkey(tempConnectionData.bufferConnectionData.receiveRkeys, tempConnectionData.bufferConnectionData.receiveRkeys + 8);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (size_t i = 0; i < tempReceiveBuf.size(); ++i) {
        if (tempReceiveBuf[i] != 0 && tempReceiveRkey[i]) {
            res.remote_receive_buffer.push_back(Utility::ntohll(tempReceiveBuf[i]));
            res.remote_receive_rkeys.push_back(ntohl(tempReceiveRkey[i]));
        }
    }

    std::vector<uint64_t> tempSendBuf(tempConnectionData.bufferConnectionData.sendBuffers, tempConnectionData.bufferConnectionData.sendBuffers + 8);
    std::vector<uint32_t> tempSendRkey(tempConnectionData.bufferConnectionData.sendRkeys, tempConnectionData.bufferConnectionData.sendRkeys + 8);

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (size_t i = 0; i < tempSendBuf.size(); ++i) {
        if (tempSendBuf[i] != 0 && tempSendRkey[i]) {
            res.remote_send_buffer.push_back(Utility::ntohll(tempSendBuf[i]));
            res.remote_send_rkeys.push_back(ntohl(tempSendRkey[i]));
        }
    }

    /* Change the queue pair state */
    Utility::checkOrDie(changeQueuePairStateToInit(res.dataQp));
    Utility::checkOrDie(changeQueuePairStateToInit(res.metaQp));
    Utility::checkOrDie(changeQueuePairStateToRTR(res.dataQp, remoteConnectionData.dataQpNum, remoteConnectionData.lid, remoteConnectionData.gid));
    Utility::checkOrDie(changeQueuePairStateToRTR(res.metaQp, remoteConnectionData.metaQpNum, remoteConnectionData.lid, remoteConnectionData.gid));
    Utility::checkOrDie(changeQueuePairStateToRTS(res.dataQp));
    Utility::checkOrDie(changeQueuePairStateToRTS(res.metaQp));
}

/**
 * @brief Creating the necessary resources for RDMA communication. The protection domain on which the buffers are registered, together with the completion queue and the queue pair.
 *
 */
void Connection::createResources() {
    // https://insujang.github.io/2020-02-09/introduction-to-programming-infiniband/

    struct ibv_context *context = createContext();

    // query port properties
    Utility::checkOrDie(ibv_query_port(context, config.infiniBandPort, &res.port_attr));

    /* Create a protection domain */
    struct ibv_pd *dataPd = ibv_alloc_pd(context);
    struct ibv_pd *metaPd = ibv_alloc_pd(context);

    for (auto &sb : ownSendBuffer) {
        sb->registerMemoryRegion(dataPd);
    }

    for (auto &rb : ownReceiveBuffer) {
        rb->registerMemoryRegion(dataPd);
    }

    metaInfoReceiveMR = registerMemoryRegion(metaPd, &metaInfoReceive, metaInfoReceive.size() * sizeof(uint8_t));
    metaInfoSendMR = registerMemoryRegion(metaPd, &metaInfoSend, metaInfoSend.size() * sizeof(uint8_t));

    /* Create a completion queue */
    int cq_size = 0xF0;
    struct ibv_cq *dataCq = ibv_create_cq(context, cq_size, nullptr, nullptr, 0);
    struct ibv_cq *metaCq = ibv_create_cq(context, cq_size, nullptr, nullptr, 0);

    /* Create a queue pair */
    struct ibv_qp *dataQp = createQueuePair(dataPd, dataCq);
    struct ibv_qp *metaQp = createQueuePair(metaPd, metaCq);

    res.dataPd = dataPd;
    res.metaPd = metaPd;
    res.dataCq = dataCq;
    res.metaCq = metaCq;
    res.dataQp = dataQp;
    res.metaQp = metaQp;
    res.ib_ctx = context;
}

/**
 * @brief Register Memory Region for meta information structures.
 *
 * @param pd The protection domain where the mr should be registered.
 * @param buf Pointer to the buffer that should be registered as memory region.
 * @param bufferSize The size of the buffer in bytes.
 * @return struct ibv_mr* A pointer to the registered memory region object.
 */
struct ibv_mr *Connection::registerMemoryRegion(struct ibv_pd *pd, void *buf, size_t bufferSize) {
    auto mr = ibv_reg_mr(pd, buf, bufferSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    assert(mr != NULL);
    return mr;
}

/**
 * @brief Creating the RDMA context.
 *
 * @return struct ibv_context* The created RDMA context.
 */
struct ibv_context *Connection::createContext() {
    const std::string &device_name = config.deviceName;
    /* There is no way to directly open the device with its name; we should get the list of devices first. */
    struct ibv_context *context = nullptr;
    int num_devices;
    struct ibv_device **device_list = ibv_get_device_list(&num_devices);

    for (int i = 0; i < num_devices; i++) {
        /* match device name. open the device and return it */
        if (device_name.compare(ibv_get_device_name(device_list[i])) == 0) {
            context = ibv_open_device(device_list[i]);
            break;
        }
    }

    /* it is important to free the device list; otherwise memory will be leaked. */
    ibv_free_device_list(device_list);
    if (context == nullptr) {
        LOG_ERROR("Unable to find the device " << device_name << std::endl);
    }
    return context;
}

/**
 * @brief Creating the queue pair. This is mostly copied from the IBV source examples.
 *
 * @param pd The protection domain where the qp should be registered.
 * @param cq The completion queue for the qp.
 * @return struct ibv_qp* The created qp.
 */
struct ibv_qp *Connection::createQueuePair(struct ibv_pd *pd, struct ibv_cq *cq) {
    struct ibv_qp_init_attr queue_pair_init_attr;
    memset(&queue_pair_init_attr, 0, sizeof(queue_pair_init_attr));
    queue_pair_init_attr.qp_type = IBV_QPT_RC;
    queue_pair_init_attr.sq_sig_all = 0;          // if not set 0, all work requests submitted to SQ will always generate a Work Completion.
    queue_pair_init_attr.send_cq = cq;            // completion queue can be shared or you can use distinct completion queues.
    queue_pair_init_attr.recv_cq = cq;            // completion queue can be shared or you can use distinct completion queues.
    queue_pair_init_attr.cap.max_send_wr = 0xFF;  // increase if you want to keep more send work requests in the SQ.
    queue_pair_init_attr.cap.max_recv_wr = 0xFF;  // increase if you want to keep more receive work requests in the RQ.
    queue_pair_init_attr.cap.max_send_sge = 1;    // increase if you allow send work requests to have multiple scatter gather entry (SGE).
    queue_pair_init_attr.cap.max_recv_sge = 1;    // increase if you allow receive work requests to have multiple scatter gather entry (SGE).

    return ibv_create_qp(pd, &queue_pair_init_attr);
}

/**
 * @brief Changing the state of the qp to init -> there but not really able to do things.
 *
 * @param queue_pair The qp that should be changed;
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::changeQueuePairStateToInit(struct ibv_qp *queue_pair) {
    struct ibv_qp_attr init_attr;
    int flags;

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    init_attr.port_num = config.infiniBandPort;
    init_attr.pkey_index = 0;
    init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                                IBV_ACCESS_REMOTE_READ |
                                IBV_ACCESS_REMOTE_WRITE;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    return ibv_modify_qp(queue_pair, &init_attr, flags);
}

/**
 * @brief Changing the state of the qp from init to ready to recive -> the qp is able to receive but not yet to write.
 *
 * @param queue_pair The qp that should be changed;
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id) {
    struct ibv_qp_attr rtr_attr;
    int flags;

    memset(&rtr_attr, 0, sizeof(rtr_attr));

    rtr_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    rtr_attr.path_mtu = ibv_mtu::IBV_MTU_1024;  // This implies splitting message into 256 Byte chunks
    rtr_attr.rq_psn = 0;
    rtr_attr.max_dest_rd_atomic = 1;
    rtr_attr.min_rnr_timer = 0x12;
    rtr_attr.ah_attr.is_global = 0;
    rtr_attr.ah_attr.sl = 0;
    rtr_attr.ah_attr.src_path_bits = 0;
    rtr_attr.ah_attr.port_num = config.infiniBandPort;
    rtr_attr.dest_qp_num = destination_qp_number;
    rtr_attr.ah_attr.dlid = destination_local_id;

    if (config.gidIndex >= 0) {
        rtr_attr.ah_attr.is_global = 1;
        rtr_attr.ah_attr.port_num = 1;
        memcpy(&rtr_attr.ah_attr.grh.dgid, destination_global_id, 16);
        rtr_attr.ah_attr.grh.flow_label = 0;
        rtr_attr.ah_attr.grh.hop_limit = 1;
        rtr_attr.ah_attr.grh.sgid_index = config.gidIndex;
        rtr_attr.ah_attr.grh.traffic_class = 0;
    }

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    return ibv_modify_qp(queue_pair, &rtr_attr, flags);
}

/**
 * @brief Changing the state of the qp from rtr to ready to send -> can send and receive.
 *
 * @param queue_pair The qp that should be changed;
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::changeQueuePairStateToRTS(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12;  // 18
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    return ibv_modify_qp(qp, &attr, flags);
}

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

/**
 * @brief Calculating how much bytes of payload can fit into one remote RB.
 *
 * @param customMetaDataSize The size of the custom meta data. This is only known to the workload and not to package or buffer.
 * @return size_t As much as we can fit into the RB including metadata in bytes.
 */
size_t Connection::maxBytesInPayload(const size_t customMetaDataSize) const {
    return bufferConfig.size_remote_receive - package_t::metaDataSize() - customMetaDataSize;
}

/**
 * @brief                   Function for distributing the data to send on the available SBs. The real sending process is triggered by the opcode and done in an other function.
 *
 * @param data              Pointer to the start of the payload data that should be sent.
 * @param dataSize          The size of the whole payload data that should be sent.
 * @param appMetaData       Pointer to the application specific meta data that is written into each package.
 * @param appMetaDataSize   The size of the application specific meta data in bytes.
 * @param opcode            The opcode that should be written to remote for every package.
 * @param strat             Whether using push or pull.
 * @return int              Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::sendData(char *data, size_t dataSize, char *appMetaData, size_t appMetaDataSize, uint8_t opcode, Strategies strat) {
    int nextFreeSend;

    uint64_t remainingSize = dataSize;                                                                                                   // Whats left to write
    uint64_t maxPayloadSize = bufferConfig.size_own_send - package_t::metaDataSize() - appMetaDataSize;                                  // As much as we can fit into the SB excluding metadata
    uint64_t maxDataToWrite = remainingSize <= maxPayloadSize ? remainingSize : (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);  // Only write full 64bit elements -- should be adjusted to underlying datatype, e.g. float or uint8_t
    uint64_t packageID = generatePackageID();                                                                                            // Some randomized identifier

    size_t packageCounter = 0;
    package_t package(packageID, maxDataToWrite, packageCounter, 0, dataSize, appMetaDataSize, data);

    while (remainingSize > 0) {
        nextFreeSend = findNextFreeSendAndBlock();
        auto &sb = ownSendBuffer[nextFreeSend];

        if (remainingSize < maxDataToWrite) {
            maxDataToWrite = remainingSize;
            package.setCurrentPackageSize(remainingSize);
        }

        package.setCurrentPackageNumber(packageCounter++);

        sb->loadPackage(sb->getBufferPtr(), &package, appMetaData);

        sb->sendOpcode = opcode;

        package.advancePayloadPtr(maxDataToWrite);

        if (strat == Strategies::push) {
            setSendOpcode(nextFreeSend, rdma_ready_to_push, false);
        } else if (strat == Strategies::pull) {
            setSendOpcode(nextFreeSend, rdma_ready_to_pull, true);
        }

        remainingSize -= maxDataToWrite;
    }

    return remainingSize == 0 ? 0 : 1;
}

/**
 * @brief Searching for a ready remote RB and block this for further usage before returning the buffer index.
 *
 * @return int The remote RB index. This is not the actual index in the meta info structure. This is the actual index minus half of meta strcture size (16/2).
 */
int Connection::findNextFreeReceiveAndBlock() {
    int nextFreeRec = -1;

    std::lock_guard<std::mutex> lk(receiveBufferBlockMutex);
    // ideally this will be done with a conditional variabel and not with busy waiting.
    while (nextFreeRec == -1) {
        nextFreeRec = getNextFreeReceive();
    }

    setReceiveOpcode(nextFreeRec + (metaInfoReceive.size() / 2), rdma_working, false);

    return nextFreeRec;
}

/**
 * @brief Searching for a ready local SB and block this for further usage before returning the buffer index.
 *
 * @return int The local SB index. This is the actual index in the meta info structure.
 */
int Connection::findNextFreeSendAndBlock() {
    int nextFreeSend = -1;

    std::lock_guard<std::mutex> lk(sendBufferBlockMutex);
    // ideally this will be done with a conditional variabel and not with busy waiting.
    while (nextFreeSend == -1) {
        nextFreeSend = getNextFreeSend();
    }

    setSendOpcode(nextFreeSend, rdma_working, false);

    return nextFreeSend;
}

/**
 * @brief Search the send meta info struct once for a ready buffer. Only once as this should be a locked process and the lock should be returned in small intervalls.
 *
 * @return int The local index of the found SB.
 */
int Connection::getNextReadyToPullSend() {
    std::shared_lock<std::shared_mutex> _lk(sendBufferCheckMutex);
    size_t metaSizeHalf = metaInfoReceive.size() / 2;
    for (size_t i = metaSizeHalf; i < metaInfoSend.size(); ++i) {
        if (metaInfoSend[i] == rdma_ready_to_pull) return i - metaSizeHalf;
    }

    return -1;
}

/**
 * @brief Searching for a ready local SB and block this for further usage before returning the buffer index.
 *
 * @return int The local SB index. This is the actual index in the meta info structure.
 */
int Connection::findNextReadyToPullSendAndBlock() {
    int nextReadyToPullSend = -1;

    std::lock_guard<std::mutex> lk(remoteSendBufferBlockMutex);
    // ideally this will be done with a conditional variabel and not with busy waiting.
    while (nextReadyToPullSend == -1) {
        nextReadyToPullSend = getNextReadyToPullSend();
    }

    setSendOpcode(nextReadyToPullSend, rdma_working, false);

    return nextReadyToPullSend;
}

/**
 * @brief The actual sending process. It assumes that the data is already in the indicated SB and is called when the corresponding opcode is set.
 *
 * @param index The local index of the SB.
 * @param strat Whether to push or pull.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::__sendData(const size_t index, Strategies strat) {
    setSendOpcode(index, rdma_working, false);
    auto &sb = ownSendBuffer[index];
    int nextFreeRec = findNextFreeReceiveAndBlock();

    if (strat == Strategies::push) {
        uint64_t wrId = nextFreeRec;
        sb->sendPackage(res.remote_receive_buffer[nextFreeRec], res.remote_receive_rkeys[nextFreeRec], res.dataQp, sb->getBufferPtr(), 10 * index + nextFreeRec);
        wrId = pollCompletion<CompletionType::useDataCq>();

        auto &doneSb = ownSendBuffer[wrId / 10];

        setReceiveOpcode((wrId % 10) + (metaInfoReceive.size() / 2), doneSb->sendOpcode, doneSb->sendOpcode != rdma_ready);  // do not send opcode if rdma_ready -> throughput test
        setSendOpcode(wrId / 10, rdma_ready, false);
    } else {
        setReceiveOpcode(nextFreeRec + (metaInfoReceive.size() / 2), sb->sendOpcode, sb->sendOpcode != rdma_ready);
    }

    return 0;
}

/**
 * @brief Sending a custom opcode to a free remote RB. This is used when there is no data or information to send with this opcode. (e.g. Request for information on all available columns)
 *
 * @param opcode The opcode to send. Be sure that this is a valid opcode. If not, this might block a remote RB completely.
 * @param sendToRemote Whether this opcode should be sent to remote or only set locally.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::sendOpcode(uint8_t opcode, bool sendToRemote) {
    setReceiveOpcode(metaInfoReceive.size() / 2 + findNextFreeReceiveAndBlock(), opcode, sendToRemote);

    return 0;
}

/**
 * @brief Generating a 'random' package id. Does not need to be highly exclusive. No collision for a few minutes should be enough, therefore, it is rather simple.
 *
 * @return uint64_t The generated 'random' id.
 */
uint64_t Connection::generatePackageID() {
    std::lock_guard<std::mutex> lg(idGeneratorMutex);
    return randGen();
}

/**
 * @brief Search the receive meta info struct once for a ready buffer. Only once as this should be a locked process and the lock should be returned in small intervalls.
 *
 * @return int The local index of the found RB (index - 16/2).
 */
int Connection::getNextFreeReceive() {
    std::shared_lock<std::shared_mutex> _lk(receiveBufferCheckMutex);
    size_t metaSizeHalf = metaInfoReceive.size() / 2;
    for (size_t i = metaSizeHalf; i < metaInfoReceive.size(); ++i) {
        if (metaInfoReceive[i] == rdma_ready) return i - metaSizeHalf;
    }

    return -1;
}

/**
 * @brief Search the send meta info struct once for a ready buffer. Only once as this should be a locked process and the lock should be returned in small intervalls.
 *
 * @return int The local index of the found SB.
 */
int Connection::getNextFreeSend() {
    std::shared_lock<std::shared_mutex> _lk(sendBufferCheckMutex);
    for (size_t i = 0; i < bufferConfig.num_own_send; ++i) {
        if (metaInfoSend[i] == rdma_ready) return i;
    }

    return -1;
}

/**
 * @brief Setting the desired opcode in the local and possibly remote meta information structure. This process is exclusively locked to prevent read/write conflicts.
 *
 * @param index The actual index in the meta info structure that should be changed.
 * @param opcode The opcode that should be applied.
 * @param sendToRemote Whether the opcode should be sent to remote.
 */
void Connection::setReceiveOpcode(const size_t index, uint8_t opcode, bool sendToRemote) {
    std::unique_lock<std::shared_mutex> lk(receiveBufferCheckMutex);
    metaInfoReceive[index] = opcode;

    if (sendToRemote) {
        size_t remoteIndex = (index + (metaInfoReceive.size() / 2)) % metaInfoReceive.size();

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;

        size_t entrySize = sizeof(metaInfoReceive[0]);

        // prepare the scatter / gather entry
        memset(&sge, 0, sizeof(sge));

        sge.addr = (uintptr_t)(&(metaInfoReceive[index]));
        sge.length = entrySize;
        sge.lkey = metaInfoReceiveMR->lkey;

        // prepare the send work request
        memset(&sr, 0, sizeof(sr));

        sr.next = NULL;
        sr.wr_id = index;
        sr.sg_list = &sge;

        sr.num_sge = 1;
        sr.opcode = IBV_WR_RDMA_WRITE;
        sr.send_flags = IBV_SEND_SIGNALED;
        // sr.send_flags = IBV_SEND_INLINE;

        sr.wr.rdma.remote_addr = res.remote_props.metaReceiveBuffer + entrySize * remoteIndex;
        sr.wr.rdma.rkey = res.remote_props.metaReceiveRkey;

        ibv_post_send(res.metaQp, &sr, &bad_wr);

        pollCompletion<CompletionType::useMetaCq>();
    }
}

/**
 * @brief Setting the desired opcode in the local and possibly remote meta information structure. This process is exclusively locked to prevent read/write conflicts.
 *
 * @param index The actual index in the meta info structure that should be changed.
 * @param opcode The opcode that should be applied.
 * @param sendToRemote Whether the opcode should be sent to remote.
 */
void Connection::setSendOpcode(size_t index, uint8_t opcode, bool sendToRemote) {
    std::unique_lock<std::shared_mutex> lk(sendBufferCheckMutex);
    metaInfoSend[index] = opcode;

    if (sendToRemote) {
        size_t remoteIndex = (index + (metaInfoSend.size() / 2)) % metaInfoSend.size();

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;

        size_t entrySize = sizeof(metaInfoSend[0]);

        // prepare the scatter / gather entry
        memset(&sge, 0, sizeof(sge));

        sge.addr = (uintptr_t)(&(metaInfoSend[index]));
        sge.length = entrySize;
        sge.lkey = metaInfoSendMR->lkey;

        // prepare the send work request
        memset(&sr, 0, sizeof(sr));

        sr.next = NULL;
        sr.wr_id = index;
        sr.sg_list = &sge;

        sr.num_sge = 1;
        sr.opcode = IBV_WR_RDMA_WRITE;
        sr.send_flags = IBV_SEND_SIGNALED;

        sr.wr.rdma.remote_addr = res.remote_props.metaSendBuffer + entrySize * remoteIndex;
        sr.wr.rdma.rkey = res.remote_props.metaSendRkey;

        ibv_post_send(res.metaQp, &sr, &bad_wr);

        pollCompletion<CompletionType::useMetaCq>();
    }
}

/**
 * @brief Read the data from the remote SB.
 *
 * @param index The RB index where the data is located (or should be read into).
 * @param consu Whether the data should actually be consumed.
 */
void Connection::readDataFromRemote(const size_t index, bool consu) {
    setReceiveOpcode(index, rdma_working, false);

    // Pretty sure this does not work atm -> Will fix eventually
    int sbIndex = findNextReadyToPullSendAndBlock();

    ownReceiveBuffer[index]->postRequest(bufferConfig.size_own_receive, IBV_WR_RDMA_READ, res.remote_props.bufferConnectionData.sendBuffers[sbIndex], res.remote_props.bufferConnectionData.sendRkeys[sbIndex], res.dataQp, ownReceiveBuffer[index]->getBufferPtr(), 10 * index + sbIndex);
    uint64_t wrId = pollCompletion<CompletionType::useDataCq>();

    setSendOpcode((wrId % 10) + metaInfoSend.size() / 2, rdma_ready, true);

    if (consu) {
        setReceiveOpcode(wrId / 10, rdma_data_finished, false);
    } else {
        setReceiveOpcode(wrId / 10, rdma_ready, true);
    }
}

/**
 * @brief Copying the data from the RB into local memory and set it free again.
 *
 * @param index The RB index where the data is located (or should be read into).
 */
void Connection::consumeData(const size_t index) {
    setReceiveOpcode(index, rdma_working, false);

    char *ptr = ownReceiveBuffer[index]->getBufferPtr();

    package_t::header_t *header = reinterpret_cast<package_t::header_t *>(ptr);

    LOG_DEBUG2(header->id << "\t" << header->total_data_size << "\t" << header->current_payload_size << "\t" << header->package_number << std::endl);

    uint64_t *localPtr = reinterpret_cast<uint64_t *>(malloc(header->current_payload_size));
    memset(localPtr, 0, header->current_payload_size);
    memcpy(localPtr, ptr + package_t::metaDataSize(), header->current_payload_size);

    free(localPtr);

    setReceiveOpcode(index, rdma_ready, true);
}

/**
 * @brief Close the connection and free all allocated resources.
 *
 * @param sendRemote Whether the shutdown should be sent to the remote machine.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::closeConnection(bool sendRemote) {
    std::lock_guard<std::mutex> lk(closingMutex);
    if (!connectionClosed) {
        connectionClosed = true;
        setReceiveOpcode(metaInfoReceive.size() / 2, rdma_shutdown, sendRemote);
        destroyResources();
    }

    return 0;
}

/**
 * @brief Reconfiguration of the local buffer setup to the (possibly) new setup. Note: trying to be as lazy as possible, there are only changes if the configuration changed.
 *
 * @param bufConfig The new buffer configuration that should be applied.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
BufferConnectionData Connection::reconfigureBuffer(buffer_config_t &bufConfig) {
    std::size_t numBlockedRec = 0;
    std::size_t numBlockedSend = 0;
    bool allBlocked = false;

    validateBufferConfig(bufConfig);

    while (!allBlocked) {
        while (numBlockedRec < bufferConfig.num_own_receive) {
            for (std::size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
                if (metaInfoReceive[i] == rdma_ready || metaInfoReceive[i] == rdma_reconfiguring) {
                    setReceiveOpcode(i, rdma_blocked, true);
                    ++numBlockedRec;
                } else {
                    continue;
                }
            }
        }

        while (numBlockedSend < bufferConfig.num_own_send) {
            for (std::size_t i = 0; i < bufferConfig.num_own_send; ++i) {
                if (metaInfoSend[i] == rdma_ready || metaInfoSend[i] == rdma_reconfiguring) {
                    setSendOpcode(i, rdma_blocked, true);
                    ++numBlockedSend;
                } else {
                    continue;
                }
            }
        }

        allBlocked = true;

        for (std::size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
            allBlocked = allBlocked && metaInfoReceive[i] == rdma_blocked;
        }

        for (std::size_t i = 0; i < bufferConfig.num_own_send; ++i) {
            allBlocked = allBlocked && metaInfoSend[i] == rdma_blocked;
        }
    }

    if (bufConfig.size_own_send != bufferConfig.size_own_send || bufConfig.num_own_send != bufferConfig.num_own_send) {
        ownSendBuffer.clear();

        bufferConfig.size_own_send = bufConfig.size_own_send;
        bufferConfig.num_own_send = bufConfig.num_own_send;

        setupSendBuffer();

        for (auto &sb : ownSendBuffer) {
            sb->registerMemoryRegion(res.dataPd);
        }
    }

    if (bufConfig.size_own_receive != bufferConfig.size_own_receive || bufConfig.num_own_receive != bufferConfig.num_own_receive) {
        ownReceiveBuffer.clear();

        bufferConfig.size_own_receive = bufConfig.size_own_receive;
        bufferConfig.num_own_receive = bufConfig.num_own_receive;

        setupReceiveBuffer();

        for (auto &rb : ownReceiveBuffer) {
            rb->registerMemoryRegion(res.dataPd);
        }
    }

    cpu_set_t cpuset;

    if (bufConfig.num_own_receive_threads != bufferConfig.num_own_receive_threads) {
        globalReceiveAbort = true;
        std::for_each(readWorkerPool.begin(), readWorkerPool.end(), [](std::unique_ptr<std::thread> &t) { t->join(); });
        readWorkerPool.clear();
        globalReceiveAbort = false;

        for (size_t tid = 0; tid < bufConfig.num_own_receive_threads; ++tid) {
            readWorkerPool.emplace_back(std::make_unique<std::thread>(check_receive, &globalReceiveAbort, tid, bufConfig.num_own_receive_threads));
            CPU_ZERO(&cpuset);
            CPU_SET(tid, &cpuset);
            int rc = pthread_setaffinity_np(readWorkerPool.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                LOG_FATAL("Error calling pthread_setaffinity_np: " << rc << "\n"
                                                                   << std::endl);
                exit(-10);
            }
        }

        bufferConfig.num_own_receive_threads = bufConfig.num_own_receive_threads;
    }

    if (bufConfig.num_own_send_threads != bufferConfig.num_own_send_threads) {
        globalSendAbort = true;
        std::for_each(sendWorkerPool.begin(), sendWorkerPool.end(), [](std::unique_ptr<std::thread> &t) { t->join(); });
        sendWorkerPool.clear();
        globalSendAbort = false;

        for (size_t tid = 0; tid < bufConfig.num_own_send_threads; ++tid) {
            sendWorkerPool.emplace_back(std::make_unique<std::thread>(check_send, &globalSendAbort, tid, bufConfig.num_own_send_threads));
            CPU_ZERO(&cpuset);
            CPU_SET(tid + bufConfig.num_own_receive_threads, &cpuset);
            int rc = pthread_setaffinity_np(sendWorkerPool.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                LOG_FATAL("Error calling pthread_setaffinity_np: " << rc << "\n"
                                                                   << std::endl);
                exit(-10);
            }
        }

        bufferConfig.num_own_send_threads = bufConfig.num_own_send_threads;
    }

    bufferConfig = bufConfig;

    BufferConnectionData bufConData = {.bufferConfig = bufConfig};

    auto pos = 0;
    for (const auto &rb : ownReceiveBuffer) {
        bufConData.receiveBuffers[pos] = (uintptr_t)rb->getBufferPtr();
        bufConData.receiveRkeys[pos] = rb->getMrPtr()->rkey;
        ++pos;
    }

    pos = 0;
    for (const auto &sb : ownSendBuffer) {
        bufConData.sendBuffers[pos] = (uintptr_t)sb->getBufferPtr();
        bufConData.sendRkeys[pos] = sb->getMrPtr()->rkey;
        ++pos;
    }

    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (i < bufferConfig.num_own_receive)
            setReceiveOpcode(i, rdma_ready, true);
        else
            setReceiveOpcode(i, rdma_no_op, true);
    }

    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (i < bufferConfig.num_own_send)
            setSendOpcode(i, rdma_ready, true);
        else
            setSendOpcode(i, rdma_no_op, true);
    }

    LOG_INFO("Reconfigured Buffers to: " << std::endl);
    printConnectionInfo();

    return bufConData;
}

/**
 * @brief Reconfiguration of the local buffer setup to the (possibly) new setup. Note: trying to be as lazy as possible, there are only changes if the configuration changed.
 *
 * @param bufConfig The new buffer configuration that should be applied.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::sendReconfigureBuffer(buffer_config_t &bufConfig) {
    BufferConnectionData bufConData = reconfigureBuffer(bufConfig);

    sendData(reinterpret_cast<char *>(&bufConData), sizeof(BufferConnectionData), nullptr, 0, rdma_reconfigure, Strategies::push);

    std::unique_lock<std::mutex> reconfigureLock(reconfigureMutex);
    reconfigureCV.wait(reconfigureLock);

    return 0;
}

/**
 * @brief Receiving the task to reconfigure. Apply the given information to the own buffer structure.
 *
 * @param index The index of the RB where the new buffer information is stored.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::receiveReconfigureBuffer(const uint8_t index) {
    char *ptr = ownReceiveBuffer[index]->getBufferPtr() + package_t::metaDataSize();

    BufferConnectionData *bufConData = reinterpret_cast<BufferConnectionData *>(malloc(sizeof(BufferConnectionData)));
    memcpy(bufConData, ptr, sizeof(BufferConnectionData));
    bufConData->bufferConfig = invertBufferConfig(bufConData->bufferConfig);

    setReceiveOpcode(index, rdma_ready, true);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (bufConData->receiveBuffers[i] == 0) continue;

        res.remote_receive_buffer.push_back(bufConData->receiveBuffers[i]);
        res.remote_receive_rkeys.push_back(bufConData->receiveRkeys[i]);
    }

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (bufConData->sendBuffers[i] == 0) continue;

        res.remote_send_buffer.push_back(bufConData->sendBuffers[i]);
        res.remote_send_rkeys.push_back(bufConData->sendRkeys[i]);
    }

    BufferConnectionData reconfData = reconfigureBuffer(bufConData->bufferConfig);

    free(bufConData);

    sendData(reinterpret_cast<char *>(&reconfData), sizeof(BufferConnectionData), nullptr, 0, rdma_reconfigure_ack, Strategies::push);

    return 0;
}

/**
 * @brief Reconfiguration of the local buffer setup to the (possibly) new setup. Note: trying to be as lazy as possible, there are only changes if the configuration changed.
 *
 * @param bufConfig The new buffer configuration that should be applied.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
void Connection::ackReconfigureBuffer(size_t index) {
    char *ptr = ownReceiveBuffer[index]->getBufferPtr() + package_t::metaDataSize();

    BufferConnectionData *recData = reinterpret_cast<BufferConnectionData *>(malloc(sizeof(BufferConnectionData)));
    memcpy(recData, ptr, sizeof(BufferConnectionData));
    recData->bufferConfig = invertBufferConfig(recData->bufferConfig);

    setReceiveOpcode(index, rdma_ready, true);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (recData->receiveBuffers[i] == 0) continue;

        res.remote_receive_buffer.push_back(recData->receiveBuffers[i]);
        res.remote_receive_rkeys.push_back(recData->receiveRkeys[i]);
    }

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (recData->sendBuffers[i] == 0) continue;

        res.remote_send_buffer.push_back(recData->sendBuffers[i]);
        res.remote_send_rkeys.push_back(recData->sendRkeys[i]);
    }

    std::unique_lock<std::mutex> reconfigureLock(reconfigureMutex);
    reconfigureCV.notify_all();

    setReceiveOpcode(index, rdma_ready, true);
}

void Connection::validateBufferConfig(buffer_config_t &bufConfig) {
    // Checking meta info size -> atm this is not really needed, as this value is currently unused. if it is used, the corresponding parts of this function should be changed.
    if (bufConfig.meta_info_size < 2) {
        LOG_WARNING("The metaInfo struct needs at least a size of 2 to work. You violated this rule! Therefore, the meta info size is set to 2." << std::endl);
        bufConfig.meta_info_size = 2;
    } else if (bufConfig.meta_info_size % 2 != 0) {
        LOG_WARNING("The metaInfoSize needs to be divisible by 2 to work. You violated this rule! Therefore, the meta info size is round up." << std::endl);
        bufConfig.meta_info_size++;
    }

    // Checking size of the RBs
    if (bufConfig.size_own_receive > (1ull << 30)) {
        LOG_WARNING("The maximal size for an RB is " << (1ull << 30) << "! You violated this rule for the local buffers! Therefore, the size of RB is set to " << (1ull << 30) << std::endl);
        bufConfig.size_own_receive = 1ull << 30;
    } else if (bufConfig.size_own_receive < 640) {
        LOG_WARNING("The minimal size for an RB is 640! You violated this rule for the local buffers! Therefore, the size of RB is set to 640." << std::endl);
        bufConfig.size_own_receive = 640;
    }

    if (bufConfig.size_remote_receive > (1ull << 30)) {
        LOG_WARNING("The maximal size for an RB is " << (1ull << 30) << "! You violated this rule for the remote buffers! Therefore, the size of RB is set to " << (1ull << 30) << std::endl);
        bufConfig.size_remote_receive = 1ull << 30;
    } else if (bufConfig.size_remote_receive < 640) {
        LOG_WARNING("The minimal size for an RB is 640! You violated this rule for the remote buffers! Therefore, the size of RB is set to 640." << std::endl);
        bufConfig.size_remote_receive = 640;
    }

    // Checking size of the SBs
    if (bufConfig.size_own_send > (1ull << 30)) {
        LOG_WARNING("The maximal size for an SB is " << (1ull << 30) << "! You violated this rule for the local buffers! Therefore, the size of SB is set to " << (1ull << 30) << std::endl);
        bufConfig.size_own_send = 1ull << 30;
    } else if (bufConfig.size_own_send < 640) {
        LOG_WARNING("The minimal size for an SB is 640! You violated this rule for the local buffers! Therefore, the size of SB is set to 640." << std::endl);
        bufConfig.size_own_send = 640;
    }

    if (bufConfig.size_remote_send > (1ull << 30)) {
        LOG_WARNING("The maximal size for an SB is " << (1ull << 30) << "! You violated this rule for the remote buffers! Therefore, the size of SB is set to " << (1ull << 30) << std::endl);
        bufConfig.size_remote_send = 1ull << 30;
    } else if (bufConfig.size_remote_send < 640) {
        LOG_WARNING("The minimal size for an SB is 640! You violated this rule for the remote buffers! Therefore, the size of SB is set to 640." << std::endl);
        bufConfig.size_remote_send = 640;
    }

    // Checking number of RBs
    if (bufConfig.num_own_receive > (metaInfoReceive.size() / 2)) {
        LOG_WARNING("It is only possible to have " << (metaInfoReceive.size() / 2) << " Receive Buffers on each side! You violated this rule for the local buffers! Therefore, the number of RB is set to " << (metaInfoReceive.size() / 2) << std::endl);
        bufConfig.num_own_receive = (metaInfoReceive.size() / 2);
    } else if (bufConfig.num_own_receive < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of local RB is set to 1." << std::endl);
        bufConfig.num_own_receive = 1;
    }

    if (bufConfig.num_remote_receive > (metaInfoReceive.size() / 2)) {
        LOG_WARNING("It is only possible to have " << (metaInfoReceive.size() / 2) << " Receive Buffers on each side! You violated this rule for the remote buffers! Therefore, the number of RB is set to " << (metaInfoReceive.size() / 2) << std::endl);
        bufConfig.num_remote_receive = (metaInfoReceive.size() / 2);
    } else if (bufConfig.num_remote_receive < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of remote RB is set to 1." << std::endl);
        bufConfig.num_remote_receive = 1;
    }

    // Checking number of SBs
    if (bufConfig.num_own_send > (metaInfoSend.size() / 2)) {
        LOG_WARNING("It is only possible to have " << (metaInfoSend.size() / 2) << " Send Buffers on each side! You violated this rule for the local buffers! Therefore, the number of SB is set to " << (metaInfoSend.size() / 2) << std::endl);
        bufConfig.num_own_send = (metaInfoSend.size() / 2);
    } else if (bufConfig.num_own_send < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of local SB is set to 1." << std::endl);
        bufConfig.num_own_send = 1;
    }

    if (bufConfig.num_remote_send > (metaInfoSend.size() / 2)) {
        LOG_WARNING("It is only possible to have " << (metaInfoSend.size() / 2) << " Send Buffers on each side! You violated this rule for the remote buffers! Therefore, the number of SB is set to " << (metaInfoSend.size() / 2) << std::endl);
        bufConfig.num_remote_send = (metaInfoSend.size() / 2);
    } else if (bufConfig.num_remote_send < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of remote SB is set to 1." << std::endl);
        bufConfig.num_remote_send = 1;
    }

    // Checking number of Receive Threads
    if (bufConfig.num_own_receive_threads > bufConfig.num_own_receive) {
        LOG_WARNING("It is not possible to have more Receive Threads then RBs. You violated this rule for the local threads! Therefore, the number of Receive Threads is set to " << bufConfig.num_own_receive << std::endl);
        bufConfig.num_own_receive_threads = bufConfig.num_own_receive;
    } else if (bufConfig.num_own_receive_threads < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of local Receive Threads is set to 1." << std::endl);
        bufConfig.num_own_receive_threads = 1;
    }

    if (bufConfig.num_remote_receive_threads > bufConfig.num_remote_receive) {
        LOG_WARNING("It is not possible to have more Receive Threads then RBs. You violated this rule for the remote threads! Therefore, the number of Receive Threads is set to " << bufConfig.num_remote_receive << std::endl);
        bufConfig.num_remote_receive_threads = bufConfig.num_remote_receive;
    } else if (bufConfig.num_remote_receive_threads < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of remote Receive Threads is set to 1." << std::endl);
        bufConfig.num_remote_receive_threads = 1;
    }

    // Checking number of Send Threads
    if (bufConfig.num_own_send_threads > bufConfig.num_own_send) {
        LOG_WARNING("It is not possible to have more Send Threads then SBs. You violated this rule for the local threads! Therefore, the number of Send Threads is set to " << bufConfig.num_own_send << std::endl);
        bufConfig.num_own_send_threads = bufConfig.num_own_send;
    } else if (bufConfig.num_own_send_threads < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of local Send Threads is set to 1." << std::endl);
        bufConfig.num_own_send_threads = 1;
    }

    if (bufConfig.num_remote_send_threads > bufConfig.num_remote_send) {
        LOG_WARNING("It is not possible to have more Send Threads then SBs. You violated this rule for the remote threads! Therefore, the number of Send Threads is set to " << bufConfig.num_remote_send << std::endl);
        bufConfig.num_remote_send_threads = bufConfig.num_remote_send;
    } else if (bufConfig.num_remote_send_threads < 1) {
        LOG_WARNING("Congratulation! You reached a state that should not be possible! The number of remote Send Threads is set to 1." << std::endl);
        bufConfig.num_remote_send_threads = 1;
    }
}

Connection::~Connection() {
    closeConnection();
}

/**
 * @brief Connect a socket.
 *
 * @param client_name If servername is specified a client connection will be initiated to the indicated server and port. Otherwise listen on the indicated port for an incoming connection.
 * @param port If servername is specified a client connection will be initiated to the indicated server and port. Otherwise listen on the indicated port for an incoming connection.
 * @return int The socket file descriptor.
 */
int Connection::sockConnect(std::string client_name, uint32_t *port) {
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;

    struct addrinfo hints = {.ai_flags = AI_PASSIVE,
                             .ai_family = AF_INET,
                             .ai_socktype = SOCK_STREAM,
                             .ai_protocol = 0,
                             .ai_addrlen = 0,
                             .ai_addr = nullptr,
                             .ai_canonname = nullptr,
                             .ai_next = nullptr};

    if (client_name.empty()) {
        int err;
        for (int k = 0; k < 20; ++k) {
            // resolve DNS address, user sockfd as temp storage
            sprintf(service, "%d", *port + k);

            Utility::checkOrDie(getaddrinfo(NULL, service, &hints, &resolved_addr));

            for (iterator = resolved_addr; iterator != NULL; iterator = iterator->ai_next) {
                sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
                assert(sockfd >= 0);

                // Client mode: setup listening socket and accept a connection
                listenfd = sockfd;
                err = bind(listenfd, iterator->ai_addr, iterator->ai_addrlen);
                if (err == 0) {
                    err = listen(listenfd, 1);
                    if (err == 0) {
                        LOG_INFO("Waiting on port " << *port + k << " for TCP connection" << std::endl;)
                        sockfd = accept(listenfd, NULL, 0);
                    }
                }
            }

            if (err == 0) {
                *port = *port + k;
                return sockfd;
            }
        }
    } else {
        int err;
        for (int k = 0; k < 20; ++k) {
            // resolve DNS address, user sockfd as temp storage
            sprintf(service, "%d", *port + k);
            LOG_DEBUG2("Trying Port " << *port + k << " for TCP Init..." << std::endl;)
            Utility::checkOrDie(getaddrinfo(client_name.c_str(), service, &hints, &resolved_addr));

            for (iterator = resolved_addr; iterator != NULL; iterator = iterator->ai_next) {
                sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
                assert(sockfd >= 0);

                // Server mode: initial connection to remote
                err = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen);
            }

            if (err == 0) {
                LOG_INFO("Connecting on port " << *port + k << " for TCP connection" << std::endl;)
                *port = *port + k;
                return sockfd;
            }
        }
    }

    return -1;
}

/**
 * @brief Sync data across a socket. It is assumned that the two sides are in sync and call this function in the proper order. Chaos will ensure if they are not. Also note this is a blocking function and will wait for the full data to be received from the remote.
 *
 * @param sockfd The socket file descriptor.
 * @param xfer_size The expected transfer size in Bytes.
 * @param local_data The indicated local data will be sent to the remote.
 * @param remote_data It will then wait for the remote to send its data back and write it on the indicated position.
 * @return int Success indicator (number of occured errors).
 */
int Connection::sockSyncData(int sockfd, int xfer_size, char *local_data, char *remote_data) {
    uint8_t result = sendTcp(sockfd, xfer_size, local_data);
    result += receiveTcp(sockfd, xfer_size, remote_data);

    return result;
}

/**
 * @brief Receive data from socket communication.
 *
 * @param sockfd The socket file descriptor.
 * @param xfer_size The expected transfer size in Bytes.
 * @param remote_data Location where to write the received data to.
 * @return int Success indicator (number of occured errors).
 */
int Connection::receiveTcp(int sockfd, int xfer_size, char *remote_data) {
    int read_bytes = read(sockfd, remote_data, xfer_size);
    if (read_bytes != xfer_size) {
        return 1;
        LOG_WARNING("The transfered size (read) did not match the expected one!" << std::endl);
    }
    return 0;
}

/**
 * @brief Send data over socket communication.
 *
 * @param sockfd The socket file descriptor.
 * @param xfer_size The expected transfer size in Bytes.
 * @param local_data The indicated local data will be sent to the remote.
 * @return int Success indicator (number of occured errors).
 */
int Connection::sendTcp(int sockfd, int xfer_size, char *local_data) {
    int write_bytes = write(sockfd, local_data, xfer_size);
    if (write_bytes != xfer_size) {
        return 1;
        LOG_WARNING("The transfered size (write) did not match the expected one!" << std::endl);
    }
    return 0;
}

void Connection::sockCloseFd(int &sockfd) {
    Utility::checkOrDie(close(sockfd));
}

buffer_config_t Connection::invertBufferConfig(buffer_config_t bufferConfig) {
    return {.num_own_send_threads = bufferConfig.num_remote_send_threads,
            .num_own_receive_threads = bufferConfig.num_remote_receive_threads,
            .num_remote_send_threads = bufferConfig.num_own_send_threads,
            .num_remote_receive_threads = bufferConfig.num_own_receive_threads,
            .num_own_receive = bufferConfig.num_remote_receive,
            .size_own_receive = bufferConfig.size_remote_receive,
            .num_remote_receive = bufferConfig.num_own_receive,
            .size_remote_receive = bufferConfig.size_own_receive,
            .num_own_send = bufferConfig.num_remote_send,
            .size_own_send = bufferConfig.size_remote_send,
            .num_remote_send = bufferConfig.num_own_send,
            .size_remote_send = bufferConfig.size_own_send,
            .meta_info_size = bufferConfig.meta_info_size};
}