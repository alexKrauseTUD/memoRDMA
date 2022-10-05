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
#include <thread>
#include <tuple>
#include <vector>

#include "DataProvider.h"
#include "Logger.h"
#include "Utility.h"

using namespace memordma;

Connection::Connection(config_t _config, buffer_config_t _bufferConfig, uint32_t _localConId) : globalReceiveAbort(false), globalSendAbort(false), completionAbort(false) {
    config = _config;
    bufferConfig = _bufferConfig;
    localConId = _localConId;
    res.sock = -1;

    // for resetting the buffer -> this is needed for the callbacks as they do not have access to the necessary structures
    reset_buffer = [this](const size_t i) -> void {
        ownReceiveBuffer[i]->clearBuffer();
        setReceiveOpcode(1000 + i, rdma_ready, true);
    };

    // for the receiving threads -> check whether a RB is ready to be consumed
    check_receive = [this](std::atomic<bool> *abort, size_t tid, size_t thrdcnt) -> void {
        // using namespace std::chrono_literals;

        // std::ios_base::fmtflags f(std::cout.flags());
        Logger::getInstance() << LogLevel::INFO << "[check_receive] Starting monitoring thread " << tid + 1 << "/" << +thrdcnt << " for receiving on connection!" << std::endl;
        size_t metaSizeHalf = metaInfoReceive.size() / 2;

        // currently this works with (busy) waiting -> TODO: conditional variable with wait
        while (!*abort) {
            // std::this_thread::sleep_for(1000ms);
            for (size_t i = tid; i < metaSizeHalf; i += thrdcnt) {
                // std::cout << i << std::endl;
                if (ConnectionManager::getInstance().hasCallback(metaInfoReceive[i])) {
                    // std::cout << "[Connection] Invoking custom callback for code " << (size_t)metaInfoReceive[i] << std::endl;

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
                        receiveDataFromRemote(i, true, Strategies::push);
                    }; break;
                    case rdma_pull_read: {
                        receiveDataFromRemote(i, false, Strategies::pull);
                    } break;
                    case rdma_pull_consume: {
                        receiveDataFromRemote(i, true, Strategies::pull);
                    } break;
                    case rdma_reconfigure: {
                        // package_t::header_t *header = reinterpret_cast<package_t::header_t *>(ownReceiveBuffer[i]->getBufferPtr());
                        // DEBUG2("GOT RECONFIGURE\t" << +i << " ID: " << header->id << " Package Number: " << header->package_number << std::endl);

                        auto recFunc = [this](size_t index) {
                            receiveReconfigureBuffer(index);
                        };
                        setReceiveOpcode(1000 + i, rdma_reconfiguring, false);
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
                // std::cout.flags(f);
            }
        }

        Logger::getInstance() << LogLevel::INFO << "[check_receive] Ending thread " << tid + 1 << "/" << +thrdcnt << " through global abort." << std::endl;
    };

    // for the sending threads -> check whether a SB is ready to be send
    check_send = [this](std::atomic<bool> *abort, size_t tid, size_t thrdcnt) -> void {
        // using namespace std::chrono_literals;

        // std::ios_base::fmtflags f(std::cout.flags());
        Logger::getInstance() << LogLevel::INFO << "[check_send] Starting monitoring thread " << tid + 1 << "/" << +thrdcnt << " for sending on connection!" << std::endl;
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
                // std::cout.flags(f);
            }
        }

        Logger::getInstance() << LogLevel::INFO << "[check_send] Ending thread " << tid + 1 << "/" << +thrdcnt << " through global abort." << std::endl;
    };

    pollCompletion = [this](std::atomic<bool> *abort) -> void {
        INFO("[pollCompletion] Starting thread!" << std::endl);

        while (!*abort) {
            struct ibv_wc wc;
            int poll_result = ibv_poll_cq(res.cq, 1, &wc);

            if (poll_result == 0) {
                continue;
            } else if (poll_result < 0) {
                // poll CQ failed
                ERROR("poll CQ failed\n");
            }

            if (wc.status != IBV_WC_SUCCESS) {
                ERROR("Got bad completion with status: " << ibv_wc_status_str(wc.status) << " (" << std::hex << wc.status << ") vendor syndrome: 0x" << std::hex << wc.vendor_err << std::endl;)
                exit(EXIT_FAILURE);
            }

            // DEBUG2("Current wr_id: " << wc.wr_id << std::endl);

            if (wc.wr_id < 10) {
                // This is no important completion atm -> no need to do anything -> TODO: fix the corresponding positions to be meaningfull
                // DEBUG2("<10: " << wc.wr_id << std::endl);
                continue;
            } else if (wc.wr_id < 100) {
                const size_t sbIndex = (wc.wr_id / 10) - 1;
                const size_t rbIndex = wc.wr_id % 10;
                const size_t notify_remote_rb_id = (100 * (sbIndex + 1)) /* 100, 200, ..., 800 */ + (rbIndex + (metaInfoReceive.size() / 2)) /* 8 <= something <= 15 */;
                auto &completed_sb = ownSendBuffer[sbIndex];
                // DEBUG2(notify_remote_rb_id << std::endl);
                completed_sb->clearBuffer();
                setReceiveOpcode(notify_remote_rb_id, completed_sb->sendOpcode, completed_sb->sendOpcode != rdma_ready);
                if (completed_sb->sendOpcode == rdma_ready) {
                    setSendOpcode(1000 + sbIndex, rdma_ready, false);
                }
            } else if (wc.wr_id < 1000) {
                setSendOpcode(1000 + ((wc.wr_id / 100) - 1), rdma_ready, false);
            } else {
                // DEBUG2(">=1000: " << wc.wr_id << std::endl);
                continue;
            }
        }

        INFO("[pollCompletion] Ending through abort!" << std::endl);
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
    sock_close(res.sock);

    pollCompletionThreadPool.emplace_back(std::make_unique<std::thread>(pollCompletion, &completionAbort));

    // the buffers are set locally and remotly to ready (usable) -> this can not be done on creation as the remote meta structure is not known at this moment
    for (size_t rbi = 0; rbi < bufferConfig.num_own_receive; ++rbi) {
        setReceiveOpcode(1000 + rbi, rdma_ready, true);
    }
    for (size_t sbi = 0; sbi < bufferConfig.num_own_send; ++sbi) {
        setSendOpcode(1000 + sbi, rdma_ready, true);
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
    completionAbort = true;
    std::for_each(pollCompletionThreadPool.begin(), pollCompletionThreadPool.end(), [](std::unique_ptr<std::thread> &t) { t->join(); });
    pollCompletionThreadPool.clear();
    completionAbort = false;

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
    ibv_destroy_qp(res.qp);
    ibv_destroy_cq(res.cq);
    ibv_dealloc_pd(res.pd);
    ibv_close_device(res.ib_ctx);
}

/**
 * @brief Formatted print of buffer configuration.
 *
 */
void Connection::printConnectionInfo() const {
    INFO("Connection Information:\n"
         << "Remote IP:\t\t\t" << config.server_name << ":" << config.tcp_port << "\n"
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
        setSendOpcode(1000 + i, rdma_ready, false);
    }
}

/**
 * @brief Creating desired number of RBs and setting their opcode (locally as the remote meta info struct might not be known) to ready.
 *
 */
void Connection::setupReceiveBuffer() {
    for (size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
        ownReceiveBuffer.push_back(std::make_unique<ReceiveBuffer>(bufferConfig.size_own_receive));
        setReceiveOpcode(1000 + i, rdma_ready, false);
    }
}

/**
 * @brief Initial tcp connection between server and client.
 *
 */
void Connection::initTCP() {
    if (!config.client_mode) {
        // @server
        res.sock = sock_connect(config.server_name, &config.tcp_port);
        if (res.sock < 0) {
            Logger::getInstance() << LogLevel::ERROR << "Failed to establish TCP connection to server " << config.server_name.c_str() << ", port " << config.tcp_port << std::endl;
            exit(EXIT_FAILURE);
        }
    } else {
        // @client
        res.sock = sock_connect("", &config.tcp_port);
        if (res.sock < 0) {
            Logger::getInstance() << LogLevel::ERROR << "Failed to establish TCP connection with client on port " << +config.tcp_port << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    Logger::getInstance() << LogLevel::INFO << "TCP connection was established!" << std::endl;
}

/**
 * @brief Exchange the RDMA buffer infos with the remote server. This function is rather complex and might be splitted into several more readable subfunctions in the future.
 *
 */
void Connection::exchangeBufferInfo() {
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;

    if (config.client_mode) {
        // Client waits on Server-Information as it is needed to create the buffers
        receive_tcp(res.sock, sizeof(struct cm_con_data_t), (char *)&tmp_con_data);

        bufferConfig = invertBufferConfig(tmp_con_data.buffer_config);

        metaInfoReceive = std::array<uint8_t, 16>{0};
        metaInfoSend = std::array<uint8_t, 16>{0};
    }

    // Creating the local buffers for RDMA communication.
    setupSendBuffer();
    setupReceiveBuffer();

    // Creating the necessary RDMA resources locally.
    createResources();

    union ibv_gid my_gid;
    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        Utility::check_or_die(ibv_query_gid(res.ib_ctx, config.ib_port, config.gid_idx, &my_gid));
    }

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    local_con_data.meta_receive_buf = Utility::htonll((uintptr_t)&metaInfoReceive);
    local_con_data.meta_receive_rkey = htonl(metaInfoReceiveMR->rkey);
    local_con_data.meta_send_buf = Utility::htonll((uintptr_t)&metaInfoSend);
    local_con_data.meta_send_rkey = htonl(metaInfoSendMR->rkey);
    local_con_data.receive_num = ownReceiveBuffer.size();
    local_con_data.send_num = ownSendBuffer.size();

    // collect buffer information for RB
    auto pos = 0;
    for (const auto &rb : ownReceiveBuffer) {
        local_con_data.receive_buf[pos] = Utility::htonll((uintptr_t)rb->getBufferPtr());
        local_con_data.receive_rkey[pos++] = htonl(rb->getMrPtr()->rkey);
    }

    // collect buffer information for SB
    pos = 0;
    for (const auto &sb : ownSendBuffer) {
        local_con_data.send_buf[pos] = Utility::htonll((uintptr_t)sb->getBufferPtr());
        local_con_data.send_rkey[pos++] = htonl(sb->getMrPtr()->rkey);
    }

    local_con_data.buffer_config = bufferConfig;

    local_con_data.qp_num = htonl(res.qp->qp_num);
    local_con_data.lid = htons(res.port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);

    if (!config.client_mode) {
        // Server sends information to Client and waits on Client-Information
        sock_sync_data(res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data);
    } else {
        // Client responds to server with own information
        send_tcp(res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data);
    }

    remote_con_data.meta_receive_buf = Utility::ntohll(tmp_con_data.meta_receive_buf);
    remote_con_data.meta_receive_rkey = ntohl(tmp_con_data.meta_receive_rkey);
    remote_con_data.meta_send_buf = Utility::ntohll(tmp_con_data.meta_send_buf);
    remote_con_data.meta_send_rkey = ntohl(tmp_con_data.meta_send_rkey);
    remote_con_data.receive_num = tmp_con_data.receive_num;
    remote_con_data.send_num = tmp_con_data.send_num;
    remote_con_data.buffer_config = invertBufferConfig(tmp_con_data.buffer_config);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    // save the remote side attributes, we will need it for the post SR
    res.remote_props = remote_con_data;

    std::vector<uint64_t> temp_receive_buf(tmp_con_data.receive_buf, tmp_con_data.receive_buf + remote_con_data.receive_num);
    std::vector<uint32_t> temp_receive_rkey(tmp_con_data.receive_rkey, tmp_con_data.receive_rkey + remote_con_data.receive_num);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (size_t i = 0; i < temp_receive_buf.size(); ++i) {
        res.remote_receive_buffer.push_back(Utility::ntohll(temp_receive_buf[i]));
        res.remote_receive_rkeys.push_back(ntohl(temp_receive_rkey[i]));
    }

    std::vector<uint64_t> temp_send_buf(tmp_con_data.send_buf, tmp_con_data.send_buf + remote_con_data.send_num);
    std::vector<uint32_t> temp_send_rkey(tmp_con_data.send_rkey, tmp_con_data.send_rkey + remote_con_data.send_num);

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (size_t i = 0; i < temp_send_buf.size(); ++i) {
        res.remote_send_buffer.push_back(Utility::ntohll(temp_send_buf[i]));
        res.remote_send_rkeys.push_back(ntohl(temp_receive_rkey[i]));
    }

    /* Change the queue pair state */
    Utility::check_or_die(changeQueuePairStateToInit(res.qp));
    Utility::check_or_die(changeQueuePairStateToRTR(res.qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid));
    Utility::check_or_die(changeQueuePairStateToRTS(res.qp));
}

/**
 * @brief Creating the necessary resources for RDMA communication. The protection domain on which the buffers are registered, together with the completion queue and the queue pair.
 *
 */
void Connection::createResources() {
    // https://insujang.github.io/2020-02-09/introduction-to-programming-infiniband/

    struct ibv_context *context = createContext();

    // query port properties
    Utility::check_or_die(ibv_query_port(context, config.ib_port, &res.port_attr));

    /* Create a protection domain */
    struct ibv_pd *protection_domain = ibv_alloc_pd(context);

    for (auto &sb : ownSendBuffer) {
        sb->registerMemoryRegion(protection_domain);
    }

    for (auto &rb : ownReceiveBuffer) {
        rb->registerMemoryRegion(protection_domain);
    }

    metaInfoReceiveMR = registerMemoryRegion(protection_domain, &metaInfoReceive, metaInfoReceive.size() * sizeof(uint8_t));
    metaInfoSendMR = registerMemoryRegion(protection_domain, &metaInfoSend, metaInfoSend.size() * sizeof(uint8_t));

    /* Create a completion queue */
    int cq_size = 0xF0;
    struct ibv_cq *completion_queue = ibv_create_cq(context, cq_size, nullptr, nullptr, 0);

    /* Create a queue pair */
    struct ibv_qp *queue_pair = createQueuePair(protection_domain, completion_queue);

    res.pd = protection_domain;
    res.cq = completion_queue;
    res.qp = queue_pair;
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
    const std::string &device_name = config.dev_name;
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
        std::cerr << "Unable to find the device " << device_name << std::endl;
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
    queue_pair_init_attr.sq_sig_all = 1;          // if not set 0, all work requests submitted to SQ will always generate a Work Completion.
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
    init_attr.port_num = config.ib_port;
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
    rtr_attr.ah_attr.port_num = config.ib_port;
    rtr_attr.dest_qp_num = destination_qp_number;
    rtr_attr.ah_attr.dlid = destination_local_id;

    if (config.gid_idx >= 0) {
        rtr_attr.ah_attr.is_global = 1;
        rtr_attr.ah_attr.port_num = 1;
        memcpy(&rtr_attr.ah_attr.grh.dgid, destination_global_id, 16);
        rtr_attr.ah_attr.grh.flow_label = 0;
        rtr_attr.ah_attr.grh.hop_limit = 1;
        rtr_attr.ah_attr.grh.sgid_index = config.gid_idx;
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

        // DEBUG1("Loaded package id: " << package.get_header().id << " with package number: " << package.get_header().package_number << " on SB: " << nextFreeSend << std::endl);

        sb->sendOpcode = opcode;

        package.advancePayloadPtr(maxDataToWrite);

        if (strat == Strategies::push) {
            setSendOpcode(1000 + nextFreeSend, rdma_ready_to_push, false);
        } else if (strat == Strategies::pull) {
            setSendOpcode(1000 + nextFreeSend, rdma_ready_to_pull, true);
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

    setReceiveOpcode(1000 + (nextFreeRec + (metaInfoReceive.size() / 2)), rdma_working, false);

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

    setSendOpcode(1000 + nextFreeSend, rdma_working, false);

    return nextFreeSend;
}

/**
 * @brief The actual sending process. It assumes that the data is already in the indicated SB and is called when the corresponding opcode is set.
 *
 * @param index The local index of the SB.
 * @param strat Whether to push or pull.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::__sendData(const size_t index, Strategies strat) {
    setSendOpcode(1000 + index, rdma_working, false);
    auto &sb = ownSendBuffer[index];
    int nextFreeRec = findNextFreeReceiveAndBlock();

    if (strat == Strategies::push) {
        sb->sendPackage(res.remote_receive_buffer[nextFreeRec], res.remote_receive_rkeys[nextFreeRec], res.qp, sb->getBufferPtr(), 10 * (index + 1) + nextFreeRec);

        // DEBUG1("Sent Package from SB " << index << " to Remote RB " << nextFreeRec << std::endl);
        // pollCompletion();
    }

    // setReceiveOpcode(nextFreeRec + (metaInfoReceive.size() / 2), sb->sendOpcode, sb->sendOpcode != rdma_ready);  // do not send opcode if rdma_ready -> throughput test

    // if (strat == Strategies::push) {
    //     // sb->clearBuffer();
    //     setSendOpcode(index, rdma_ready, false);
    // }

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
    setReceiveOpcode(1000 + (metaInfoReceive.size() / 2 + findNextFreeReceiveAndBlock()), opcode, sendToRemote);

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
    std::lock_guard<std::mutex> _lk(receiveBufferCheckMutex);
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
    std::lock_guard<std::mutex> _lk(sendBufferCheckMutex);
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
    std::lock_guard<std::mutex> lk(receiveBufferCheckMutex);
    const size_t localRbIndex = index % 100;
    metaInfoReceive[localRbIndex] = opcode;

    if (sendToRemote) {
        size_t remoteIndex = (localRbIndex + (metaInfoReceive.size() / 2)) % metaInfoReceive.size();

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;

        size_t entrySize = sizeof(metaInfoReceive[0]);

        // prepare the scatter / gather entry
        memset(&sge, 0, sizeof(sge));

        sge.addr = (uintptr_t)(&(metaInfoReceive[localRbIndex]));
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

        sr.wr.rdma.remote_addr = res.remote_props.meta_receive_buf + entrySize * remoteIndex;
        sr.wr.rdma.rkey = res.remote_props.meta_receive_rkey;

        ibv_post_send(res.qp, &sr, &bad_wr);

        // pollCompletion();
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
    std::lock_guard<std::mutex> lk(sendBufferCheckMutex);
    const size_t localSbIndex = index % 100;
    metaInfoSend[localSbIndex] = opcode;

    if (sendToRemote) {
        size_t remoteIndex = (localSbIndex + (metaInfoSend.size() / 2)) % metaInfoSend.size();

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;

        size_t entrySize = sizeof(metaInfoSend[0]);

        // prepare the scatter / gather entry
        memset(&sge, 0, sizeof(sge));

        sge.addr = (uintptr_t)(&(metaInfoSend[localSbIndex]));
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

        sr.wr.rdma.remote_addr = res.remote_props.meta_send_buf + entrySize * remoteIndex;
        sr.wr.rdma.rkey = res.remote_props.meta_send_rkey;

        ibv_post_send(res.qp, &sr, &bad_wr);

        // pollCompletion();
    }
}

/**
 * @brief Copying the data from the RB into local memory and set it free again.
 *
 * @param index The RB index where the data is located (or should be read into).
 * @param consu Whether the data should actually be consumed.
 * @param strat Whether the data is already written to the RB (push) or must be read from the remote SB (pull).
 */
void Connection::receiveDataFromRemote(const size_t index, bool consu, Strategies strat) {
    setReceiveOpcode(1000 + index, rdma_working, false);

    if (strat == Strategies::pull) {
        // Pretty sure this does not work atm -> Will fix eventually
        int sbIndex = index;
        while (metaInfoSend[sbIndex] != rdma_ready_to_pull) {
            sbIndex -= bufferConfig.num_remote_send_threads;

            if (sbIndex < 0) sbIndex = index;
        }

        ownReceiveBuffer[index]->postRequest(bufferConfig.size_own_receive, IBV_WR_RDMA_READ, res.remote_props.send_buf[sbIndex], res.remote_props.send_rkey[sbIndex], res.qp, ownReceiveBuffer[index]->getBufferPtr(), 0);
        // pollCompletion();
        setSendOpcode(1000 + sbIndex, rdma_ready, true);
    }

    if (consu) {
        char *ptr = ownReceiveBuffer[index]->getBufferPtr();

        package_t::header_t *header = reinterpret_cast<package_t::header_t *>(ptr);

        // std::cout << header->id << "\t" << header->total_data_size << "\t" << header->current_payload_size << "\t" << header->package_number << "\t" << header->data_type << std::endl;

        uint64_t *localPtr = reinterpret_cast<uint64_t *>(malloc(header->current_payload_size));
        memset(localPtr, 0, header->current_payload_size);
        memcpy(localPtr, ptr + package_t::metaDataSize(), header->current_payload_size);

        free(localPtr);
    }

    setReceiveOpcode(1000 + index, rdma_ready, true);
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
        setReceiveOpcode(1000 + (metaInfoReceive.size() / 2), rdma_shutdown, sendRemote);
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
reconfigure_data Connection::reconfigureBuffer(buffer_config_t &bufConfig) {
    std::size_t numBlockedRec = 0;
    std::size_t numBlockedSend = 0;
    bool allBlocked = false;

    while (!allBlocked) {
        while (numBlockedRec < bufferConfig.num_own_receive) {
            for (std::size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
                if (metaInfoReceive[i] == rdma_ready || metaInfoReceive[i] == rdma_reconfiguring) {
                    setReceiveOpcode(1000 + i, rdma_blocked, true);
                    ++numBlockedRec;
                } else {
                    continue;
                }
            }
        }

        while (numBlockedSend < bufferConfig.num_own_send) {
            for (std::size_t i = 0; i < bufferConfig.num_own_send; ++i) {
                if (metaInfoSend[i] == rdma_ready || metaInfoSend[i] == rdma_reconfiguring) {
                    setSendOpcode(1000 + i, rdma_blocked, true);
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
            sb->registerMemoryRegion(res.pd);
        }
    }

    if (bufConfig.size_own_receive != bufferConfig.size_own_receive || bufConfig.num_own_receive != bufferConfig.num_own_receive) {
        ownReceiveBuffer.clear();

        bufferConfig.size_own_receive = bufConfig.size_own_receive;
        bufferConfig.num_own_receive = bufConfig.num_own_receive;

        setupReceiveBuffer();

        for (auto &rb : ownReceiveBuffer) {
            rb->registerMemoryRegion(res.pd);
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
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
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
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
                exit(-10);
            }
        }

        bufferConfig.num_own_send_threads = bufConfig.num_own_send_threads;
    }

    bufferConfig = bufConfig;

    reconfigure_data recData = {.buffer_config = bufConfig};

    auto pos = 0;
    for (const auto &rb : ownReceiveBuffer) {
        recData.receive_buf[pos] = (uintptr_t)rb->getBufferPtr();
        recData.receive_rkey[pos] = rb->getMrPtr()->rkey;
        ++pos;
    }

    pos = 0;
    for (const auto &sb : ownSendBuffer) {
        recData.send_buf[pos] = (uintptr_t)sb->getBufferPtr();
        recData.send_rkey[pos] = sb->getMrPtr()->rkey;
        ++pos;
    }

    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (i < bufferConfig.num_own_receive)
            setReceiveOpcode(1000 + i, rdma_ready, true);
        else
            setReceiveOpcode(1000 + i, rdma_no_op, true);
    }

    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (i < bufferConfig.num_own_send)
            setSendOpcode(1000 + i, rdma_ready, true);
        else
            setSendOpcode(1000 + i, rdma_no_op, true);
    }

    INFO("Reconfigured Buffers to: " << std::endl);
    printConnectionInfo();

    return recData;
}

/**
 * @brief Reconfiguration of the local buffer setup to the (possibly) new setup. Note: trying to be as lazy as possible, there are only changes if the configuration changed.
 *
 * @param bufConfig The new buffer configuration that should be applied.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::sendReconfigureBuffer(buffer_config_t &bufConfig) {
    reconfigure_data recData = reconfigureBuffer(bufConfig);

    sendData(reinterpret_cast<char *>(&recData), sizeof(reconfigure_data), nullptr, 0, rdma_reconfigure, Strategies::push);

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

    reconfigure_data *recData = reinterpret_cast<reconfigure_data *>(malloc(sizeof(reconfigure_data)));
    memcpy(recData, ptr, sizeof(reconfigure_data));
    recData->buffer_config = invertBufferConfig(recData->buffer_config);

    setReceiveOpcode(1000 + index, rdma_ready, true);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (recData->receive_buf[i] == 0) continue;

        res.remote_receive_buffer.push_back(recData->receive_buf[i]);
        res.remote_receive_rkeys.push_back(recData->receive_rkey[i]);
    }

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (recData->send_buf[i] == 0) continue;

        res.remote_send_buffer.push_back(recData->send_buf[i]);
        res.remote_send_rkeys.push_back(recData->send_rkey[i]);
    }

    reconfigure_data reconfData = reconfigureBuffer(recData->buffer_config);

    free(recData);

    sendData(reinterpret_cast<char *>(&reconfData), sizeof(reconfigure_data), nullptr, 0, rdma_reconfigure_ack, Strategies::push);

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

    reconfigure_data *recData = reinterpret_cast<reconfigure_data *>(malloc(sizeof(reconfigure_data)));
    memcpy(recData, ptr, sizeof(reconfigure_data));
    recData->buffer_config = invertBufferConfig(recData->buffer_config);

    setReceiveOpcode(1000 + index, rdma_ready, true);

    res.remote_receive_buffer.clear();
    res.remote_receive_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoReceive.size() / 2; ++i) {
        if (recData->receive_buf[i] == 0) continue;

        res.remote_receive_buffer.push_back(recData->receive_buf[i]);
        res.remote_receive_rkeys.push_back(recData->receive_rkey[i]);
    }

    res.remote_send_buffer.clear();
    res.remote_send_rkeys.clear();
    for (uint8_t i = 0; i < metaInfoSend.size() / 2; ++i) {
        if (recData->send_buf[i] == 0) continue;

        res.remote_send_buffer.push_back(recData->send_buf[i]);
        res.remote_send_rkeys.push_back(recData->send_rkey[i]);
    }

    std::unique_lock<std::mutex> reconfigureLock(reconfigureMutex);
    reconfigureCV.notify_all();

    setReceiveOpcode(1000 + index, rdma_ready, true);
}

/**
 * @brief Wrapper for adding a number of RBs to the configuration.
 *
 * @param quantity Number of RBs to add.
 * @param own Whether to add them locally or remote.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::addReceiveBuffer(std::size_t quantity = 1, bool own = true) {
    buffer_config_t bufConfig = bufferConfig;
    if (own) {
        bufConfig.num_own_receive += quantity;
        if (bufConfig.num_own_receive > (metaInfoReceive.size() / 2)) {
            std::cout << "It is only possible to have " << (metaInfoReceive.size() / 2) << " Receive Buffer on each side!  You violated this rule! Therefore, the number of RB is set to " << (metaInfoReceive.size() / 2) << std::endl;
            bufConfig.num_own_receive = (metaInfoReceive.size() / 2);
        }
        if (bufConfig.num_own_receive < 1) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_own_receive = 1;
        }
    } else {
        bufConfig.num_remote_receive += quantity;
        if (bufConfig.num_remote_receive > (metaInfoReceive.size() / 2)) {
            std::cout << "It is only possible to have " << (metaInfoReceive.size() / 2) << " Receive Buffer on each side!  You violated this rule! Therefore, the number of RB is set to " << (metaInfoReceive.size() / 2) << std::endl;
            bufConfig.num_remote_receive = (metaInfoReceive.size() / 2);
        }
        if (bufConfig.num_remote_receive < 1) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_remote_receive = 1;
        }
    }

    return sendReconfigureBuffer(bufConfig);
}

/**
 * @brief Wrapper for removing a number of RBs to the configuration.
 *
 * @param quantity Number of RBs to remove (1 must be left).
 * @param own Whether to remove them locally or remote.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::removeReceiveBuffer(std::size_t quantity = 1, bool own = true) {
    buffer_config_t bufConfig = bufferConfig;
    if (own) {
        bufConfig.num_own_receive -= quantity;
        if (bufConfig.num_own_receive > (metaInfoReceive.size() / 2)) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_own_receive = 1;
        }
        if (bufConfig.num_own_receive < 1) {
            std::cout << "There has to be at least 1 RB on each side! You violated this rule! Therefore, the number of RB is set to 1." << std::endl;
            bufConfig.num_own_receive = 1;
        }
    } else {
        bufConfig.num_remote_receive -= quantity;
        if (bufConfig.num_remote_receive > (metaInfoReceive.size() / 2)) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_remote_receive = 1;
        }
        if (bufConfig.num_remote_receive < 1) {
            std::cout << "There has to be at least 1 RB on each side! You violated this rule! Therefore, the number of RB is set to 1." << std::endl;
            bufConfig.num_remote_receive = 1;
        }
    }

    return sendReconfigureBuffer(bufConfig);
}

/**
 * @brief Wrapper for resizing all RBs.
 *
 * @param newSize The new size for all RBs.
 * @param own Whether to resize them locally or remote.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::resizeReceiveBuffer(std::size_t newSize, bool own) {
    buffer_config_t bufConfig = bufferConfig;
    if (own) {
        bufConfig.size_own_receive = newSize;
        if (bufConfig.size_own_receive > (1ull < 30)) {
            std::cout << "The maximal size for an RB is " << (1ull < 30) << "! You violated this rule! Therefore, the size of RB is set to " << (1ull < 30) << std::endl;
            bufConfig.size_own_receive = (1ull < 30);
        }
        if (bufConfig.size_own_receive < 64) {
            std::cout << "There has to be at least 64 Byte for each RB! You violated this rule! Therefore, the size of RB is set to 64." << std::endl;
            bufConfig.size_own_receive = 64;
        }
    } else {
        bufConfig.size_remote_receive = newSize;
        if (bufConfig.size_remote_receive > (1ull < 30)) {
            std::cout << "The maximal size for an RB is " << (1ull < 30) << "! You violated this rule! Therefore, the size of RB is set to " << (1ull < 30) << std::endl;
            bufConfig.size_remote_receive = 64;
        }
        if (bufConfig.size_remote_receive < 64) {
            std::cout << "There has to be at least 64 Byte for each RB! You violated this rule! Therefore, the size of RB is set to 64." << std::endl;
            bufConfig.size_remote_receive = 64;
        }
    }

    return sendReconfigureBuffer(bufConfig);
}

/**
 * @brief Wrapper for resizing all SBs.
 *
 * @param newSize The new size for all SBs.
 * @param own Whether to resize them locally or remote.
 * @return int Indication whether it succeeded. 0 for success and everything else is failure indication.
 */
int Connection::resizeSendBuffer(std::size_t newSize, bool own) {
    buffer_config_t bufConfig = bufferConfig;
    if (own) {
        bufConfig.size_own_send = newSize;
        if (bufConfig.size_own_send < 64) {
            std::cout << "There has to be at least 64 Byte for an SB! You violated this rule! Therefore, the size of SB is set to 64." << std::endl;
            bufConfig.size_own_send = 64;
        }
    } else {
        bufConfig.size_remote_send = newSize;
        if (bufConfig.size_remote_send < 64) {
            std::cout << "There has to be at least 64 Byte for an SB! You violated this rule! Therefore, the size of SB is set to 64." << std::endl;
            bufConfig.size_remote_send = 64;
        }
    }

    return sendReconfigureBuffer(bufConfig);
}

Connection::~Connection() {
    closeConnection();
}

// \begin socket operation
//
// For simplicity, the example program uses TCP sockets to exchange control
// information. If a TCP/IP stack/connection is not available, connection
// manager (CM) may be used to pass this information. Use of CM is beyond the
// scope of this example.

// Connect a socket. If servername is specified a client connection will be
// initiated to the indicated server and port. Otherwise listen on the indicated
// port for an incoming connection.
int Connection::sock_connect(std::string client_name, uint32_t* port) {
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;

    // @man getaddrinfo:
    //  struct addrinfo {
    //      int             ai_flags;
    //      int             ai_family;
    //      int             ai_socktype;
    //      int             ai_protocol;
    //      socklen_t       ai_addrlen;
    //      struct sockaddr *ai_addr;
    //      char            *ai_canonname;
    //      struct addrinfo *ai_next;
    //  }
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

            Utility::check_or_die(getaddrinfo(NULL, service, &hints, &resolved_addr));

            for (iterator = resolved_addr; iterator != NULL; iterator = iterator->ai_next) {
                sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
                assert(sockfd >= 0);

                // Client mode: setup listening socket and accept a connection
                listenfd = sockfd;
                err = bind(listenfd, iterator->ai_addr, iterator->ai_addrlen);
                if (err == 0) {
                    err = listen(listenfd, 1);
                    if (err == 0) {
                        INFO("Waiting on port " << *port + k << " for TCP connection" << std::endl;)
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
            DEBUG2( "Trying Port " << *port + k << " for TCP Init..." << std::endl;)
            Utility::check_or_die(getaddrinfo(client_name.c_str(), service, &hints, &resolved_addr));

            for (iterator = resolved_addr; iterator != NULL; iterator = iterator->ai_next) {
                sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
                assert(sockfd >= 0);

                // Server mode: initial connection to remote
                err = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen);
            }

            if (err == 0) {
                INFO("Connecting on port " << *port + k << " for TCP connection" << std::endl;)
                *port = *port + k;
                return sockfd;
            }
        }
    }

    return -1;
}

// Sync data across a socket. The indicated local data will be sent to the
// remote. It will then wait for the remote to send its data back. It is
// assumned that the two sides are in sync and call this function in the proper
// order. Chaos will ensure if they are not. Also note this is a blocking
// function and will wait for the full data to be received from the remote.
int Connection::sock_sync_data(int sockfd, int xfer_size, char *local_data, char *remote_data) {
    int read_bytes = 0;
    int write_bytes = 0;

    write_bytes = write(sockfd, local_data, xfer_size);
    assert(write_bytes == xfer_size);

    read_bytes = read(sockfd, remote_data, xfer_size);
    assert(read_bytes == xfer_size);

    INFO("SYNCHRONIZED!\n\n");

    // FIXME: hard code that always returns no error
    return 0;
}
// \end socket operation

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

void Connection::sock_close(int &sockfd) {
    Utility::check_or_die(close(sockfd));
}

int Connection::receive_tcp(int sockfd, int xfer_size, char *remote_data) {
    int read_bytes = 0;

    read_bytes = read(sockfd, remote_data, xfer_size);
    assert(read_bytes == xfer_size);

    // FIXME: hard code that always returns no error
    return 0;
}
int Connection::send_tcp(int sockfd, int xfer_size, char *local_data) {
    int write_bytes = 0;

    write_bytes = write(sockfd, local_data, xfer_size);
    assert(write_bytes == xfer_size);

    // FIXME: hard code that always returns no error
    return 0;
}
