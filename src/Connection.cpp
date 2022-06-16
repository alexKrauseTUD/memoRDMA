#include "Connection.h"

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

#include "DataProvider.h"
#include "util.h"

// typedef std::chrono::_V2::system_clock::time_point timePoint;

Connection::Connection(config_t _config, buffer_config_t _bufferConfig) : globalAbort(false) {
    conStat = active;
    config = _config;
    bufferConfig = _bufferConfig;
    res.sock = -1;

    check_receive = [this](std::atomic<bool> *abort) -> void {
        using namespace std::chrono_literals;
        std::ios_base::fmtflags f(std::cout.flags());
        std::cout << "Starting monitoring thread for connection!" << std::flush;
        size_t metaSize = metaInfo.size();

        while (!*abort) {
            // std::this_thread::sleep_for(10ms);
            for (size_t i = 0; i < metaSize / 2; ++i) {
                switch (metaInfo[i]) {
                    case rdma_no_op:
                    case rdma_ready:
                    case rdma_data_consuming: {
                        continue;
                    }; break;
                    case rdma_data_finished: {
                        receiveDataFromRemote(i);
                    }; break;
                    case rdma_consume_test: {
                        consume(i);
                    } break;
                    case rdma_multi_thread: {
                        metaInfo[i] = rdma_no_op;
                        conStat = multi_thread;
                    } break;
                    case rdma_pull_read: {
                        pullDataFromRemote(i, false);
                    } break;
                    case rdma_pull_consume: {
                        pullDataFromRemote(i, true);
                    } break;
                    case rdma_reconfigure: {
                        receiveReconfigureBuffer(i);
                    } break;
                    case rdma_shutdown: {
                        conStat = closing;
                    }; break;
                    default: {
                        continue;
                    }; break;
                }
                std::cout.flags(f);
            }
        }
        std::cout << "[check_receive] Ending through global abort." << std::endl;
    };

    init();
}

void Connection::init() {
    globalAbort = false;
    conStat = active;

    metaInfo = std::array<uint8_t, 16>{0};

    initTCP();

    exchangeBufferInfo();

    sock_close(res.sock);

    for (size_t rbi = 0; rbi < bufferConfig.num_own_receive; ++rbi) {
        setOpcode(rbi, rdma_ready, true);
    }

    readWorker = new std::thread(check_receive, &globalAbort);
}

void Connection::destroyResources() {
    if (readWorker) {
        globalAbort = true;
        readWorker->join();
        globalAbort = false;
    }

    printf("Freeing...");
    for (auto mr : res.own_mr) {
        ibv_dereg_mr(mr);
    }
    for (auto rb : res.own_buffer) {
        free(rb);
    }
    if (metaInfoMR) {
        ibv_dereg_mr(metaInfoMR);
    }
    ibv_destroy_qp(res.qp);
    ownReceiveBuffer.clear();
    receiveMap.clear();
    ibv_destroy_cq(res.cq);
    ibv_dealloc_pd(res.pd);
    ibv_close_device(res.ib_ctx);
    close(res.sock);
    printf("Done.");
}

void Connection::printConnectionInfo() {
    std::cout << "Remote IP:\t\t\t" << config.server_name << "\n"
              << "\tOwn SB Size:\t\t" << bufferConfig.size_own_send << "\n"
              << "\tOwn RB Number:\t\t" << +bufferConfig.num_own_receive << "\n"
              << "\tOwn RB Size:\t\t" << bufferConfig.size_own_receive << "\n"
              << "\tRemote SB Size:\t\t" << bufferConfig.size_remote_send << "\n"
              << "\tRemote RB Number:\t" << +bufferConfig.num_remote_receive << "\n"
              << "\tRemote RB Size:\t\t" << bufferConfig.size_remote_receive << "\n"
              << std::endl;
}

void Connection::setupSendBuffer() {
    ownSendBuffer = new SendBuffer(bufferConfig.size_own_send);
    ownSendBuffer->metaInfo = rdma_ready;
}

void Connection::setupReceiveBuffer() {
    for (size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
        ownReceiveBuffer.push_back(new ReceiveBuffer(bufferConfig.size_own_receive));
        setOpcode(i, rdma_ready, false);
    }
}

struct ibv_mr *Connection::registerMemoryRegion(struct ibv_pd *pd, void *buffer, size_t size) {
    return ibv_reg_mr(pd, buffer, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
}

void Connection::initTCP() {
    if (!config.client_mode) {
        // @server
        res.sock = sock_connect(config.server_name, config.tcp_port);
        if (res.sock < 0) {
            ERROR("Failed to establish TCP connection to server %s, port %d\n",
                  config.server_name.c_str(), config.tcp_port);
            exit(EXIT_FAILURE);
        }
    } else {
        // @client
        res.sock = sock_connect("", config.tcp_port);
        if (res.sock < 0) {
            ERROR("Failed to establish TCP connection with client on port %d\n",
                  config.tcp_port);
            exit(EXIT_FAILURE);
        }
    }
    INFO("TCP connection was established\n");
}

void Connection::exchangeBufferInfo() {
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;

    if (config.client_mode) {
        // Client waits on Server-Information as it is needed to create the buffers
        receive_tcp(res.sock, sizeof(struct cm_con_data_t), (char *)&tmp_con_data);

        bufferConfig = invertBufferConfig(tmp_con_data.buffer_config);

        metaInfo = std::array<uint8_t, 16>{0};
    }

    setupSendBuffer();
    setupReceiveBuffer();

    createResources();

    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        CHECK(ibv_query_gid(res.ib_ctx, config.ib_port, config.gid_idx, &my_gid));
    }

    std::vector<uintptr_t> receive_buf;
    std::vector<uint32_t> receive_rkey;

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    local_con_data.meta_buf = htonll((uintptr_t)&metaInfo);
    local_con_data.meta_rkey = htonl(metaInfoMR->rkey);
    local_con_data.send_buf = htonll((uintptr_t)ownSendBuffer->buf);
    local_con_data.send_rkey = htonl(ownSendBuffer->mr->rkey);
    local_con_data.receive_num = ownReceiveBuffer.size();
    for (const auto &rb : ownReceiveBuffer) {
        receive_buf.push_back(htonll((uintptr_t)rb->buf));
        receive_rkey.push_back(htonl(rb->mr->rkey));
    }
    auto pos = 0;
    for (auto &ptr : receive_buf) {
        local_con_data.receive_buf[pos++] = ptr;
    }
    pos = 0;
    for (auto &ptr : receive_rkey) {
        local_con_data.receive_rkey[pos++] = ptr;
    }
    local_con_data.qp_num = htonl(res.qp->qp_num);
    local_con_data.lid = htons(res.port_attr.lid);
    local_con_data.buffer_config = bufferConfig;
    memcpy(local_con_data.gid, &my_gid, 16);

    if (!config.client_mode) {
        // Server sends information to Client and waits on Client-Information
        sock_sync_data(res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data);
    } else {
        // Client responds to server with own information
        send_tcp(res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data);
    }

    remote_con_data.meta_buf = ntohll(tmp_con_data.meta_buf);
    remote_con_data.meta_rkey = ntohl(tmp_con_data.meta_rkey);
    remote_con_data.send_buf = ntohll(tmp_con_data.send_buf);
    remote_con_data.send_rkey = ntohl(tmp_con_data.send_rkey);
    remote_con_data.receive_num = tmp_con_data.receive_num;
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    remote_con_data.buffer_config = invertBufferConfig(tmp_con_data.buffer_config);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    // save the remote side attributes, we will need it for the post SR
    res.remote_props = remote_con_data;

    std::vector<uint64_t> temp_buf(tmp_con_data.receive_buf, tmp_con_data.receive_buf + remote_con_data.receive_num);
    std::vector<uint32_t> temp_rkey(tmp_con_data.receive_rkey, tmp_con_data.receive_rkey + remote_con_data.receive_num);

    for (size_t i = 0; i < temp_buf.size(); ++i) {
        temp_buf[i] = ntohll(temp_buf[i]);
        temp_rkey[i] = ntohl(temp_rkey[i]);
    }

    res.remote_buffer = temp_buf;
    res.remote_rkeys = temp_rkey;

    /* Change the queue pair state */
    CHECK(changeQueuePairStateToInit(res.qp));
    CHECK(changeQueuePairStateToRTR(res.qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid));
    CHECK(changeQueuePairStateToRTS(res.qp));
}

void Connection::createResources() {
    // https://insujang.github.io/2020-02-09/introduction-to-programming-infiniband/

    struct ibv_context *context = createContext();

    // query port properties
    CHECK(ibv_query_port(context, config.ib_port, &res.port_attr));

    /* Create a protection domain */
    struct ibv_pd *protection_domain = ibv_alloc_pd(context);

    ownSendBuffer->mr = registerMemoryRegion(protection_domain, ownSendBuffer->buf, ownSendBuffer->getBufferSize());
    assert(ownSendBuffer->mr != NULL);
    res.own_mr.push_back(ownSendBuffer->mr);
    res.own_buffer.push_back(ownSendBuffer->buf);

    for (auto rb : ownReceiveBuffer) {
        rb->mr = registerMemoryRegion(protection_domain, rb->buf, rb->getBufferSize());
        assert(rb->mr != NULL);
        res.own_mr.push_back(rb->mr);
        res.own_buffer.push_back(rb->buf);
    }

    metaInfoMR = registerMemoryRegion(protection_domain, &metaInfo, metaInfo.size() * sizeof(uint8_t));

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

int Connection::changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id) {
    struct ibv_qp_attr rtr_attr;
    int flags;

    memset(&rtr_attr, 0, sizeof(rtr_attr));

    rtr_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    rtr_attr.path_mtu = ibv_mtu::IBV_MTU_256;
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

// Transition a QP from the RTR to RTS state
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

// Poll the CQ for a single event. This function will continue to poll the queue
// until MAX_POLL_TIMEOUT ms have passed.
int Connection::poll_completion() {
    struct ibv_wc wc;
    unsigned long start_time_ms;
    unsigned long curr_time_ms;
    struct timeval curr_time;
    int poll_result;

    // poll the completion for a while before giving up of doing it
    gettimeofday(&curr_time, NULL);
    start_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    do {
        poll_result = ibv_poll_cq(res.cq, 1, &wc);
        gettimeofday(&curr_time, NULL);
        curr_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    } while ((poll_result == 0) &&
             ((curr_time_ms - start_time_ms) < MAX_POLL_CQ_TIMEOUT));

    if (poll_result < 0) {
        // poll CQ failed
        ERROR("poll CQ failed\n");
        goto die;
    } else if (poll_result == 0) {
        ERROR("Completion wasn't found in the CQ after timeout\n");
        goto die;
    } else {
        // CQE found
        // INFO("Completion was found in CQ with status 0x%x\n", wc.status);
    }

    if (wc.status != IBV_WC_SUCCESS) {
        ERROR("Got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
              wc.status, wc.vendor_err);
        goto die;
    }

    // FIXME: ;)
    return 0;
die:
    exit(EXIT_FAILURE);
}

int Connection::sendData(std::string &data) {
    if (ownSendBuffer->metaInfo == rdma_no_op) {
        std::cout << "There is no buffer for sending initialized!" << std::endl;
        return false;
    }

    busy = true;

    while (ownSendBuffer->metaInfo != rdma_ready) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ns);
        continue;
    }

    uint64_t packageID = generatePackageID();

    ownSendBuffer->metaInfo = rdma_sending;

    const char *dataCString = data.c_str();

    size_t dataSize = std::strlen(dataCString);
    uint32_t ownSendToRemoteReceiveRatio = getOwnSendToRemoteReceiveRatio();
    size_t sendPackages = std::ceil(dataSize / (ownSendBuffer->getBufferSize() - (package_t::metaDataSize() * ownSendToRemoteReceiveRatio)));
    sendPackages = sendPackages > 0 ? sendPackages : 1;
    size_t alreadySentSize = 0;
    uint64_t currentSize = 0;
    int nextFree;

    for (size_t i = 0; i < sendPackages; ++i) {
        ownSendBuffer->clearBuffer();
        for (size_t k = 0; k < ownSendToRemoteReceiveRatio; ++k) {
            int c = 0;
            nextFree = getNextFreeReceive();

            while (nextFree == -1) {
                ++c;
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(100ns);
                nextFree = getNextFreeReceive();
                if (c >= 100) {
                    std::cout << "There was no free remote receive buffer found for a longer period!" << std::endl;
                    busy = false;
                    return 1;
                }

                continue;
            }

            setOpcode(metaInfo.size() / 2 + nextFree, rdma_data_receiving, false);

            currentSize = dataSize - alreadySentSize <= bufferConfig.size_remote_receive - package_t::metaDataSize() ? dataSize - alreadySentSize : bufferConfig.size_remote_receive - package_t::metaDataSize();
            ownSendBuffer->loadData(dataCString + alreadySentSize, ownSendBuffer->buf + (k * bufferConfig.size_remote_receive), dataSize, currentSize, i * ownSendToRemoteReceiveRatio + k, type_string, packageID);
            ownSendBuffer->post_request(currentSize + package_t::metaDataSize(), IBV_WR_RDMA_WRITE, res.remote_buffer[nextFree], res.remote_rkeys[nextFree], res.qp, ownSendBuffer->buf + (k * bufferConfig.size_remote_receive), 0);

            poll_completion();

            alreadySentSize += currentSize;

            setOpcode(metaInfo.size() / 2 + nextFree, rdma_data_finished, true);

            if (alreadySentSize == dataSize) break;
        }
    }

    ownSendBuffer->metaInfo = rdma_ready;

    busy = false;
    return 0;
}

uint64_t Connection::generatePackageID() {
    return std::rand();
}

int Connection::getNextFreeReceive() {
    size_t metaSize = metaInfo.size();
    for (size_t i = (metaSize / 2); i < metaSize; ++i) {
        if (metaInfo[i] == rdma_ready) return i - (metaSize / 2);
    }

    return -1;
}

uint32_t Connection::getOwnSendToRemoteReceiveRatio() {
    return std::floor(ownSendBuffer->getBufferSize() / bufferConfig.size_remote_receive);
}

void Connection::setOpcode(size_t index, rdma_handler_communication opcode, bool sendToRemote) {
    metaInfo[index] = opcode;

    if (sendToRemote) {
        size_t remoteIndex = (index + (metaInfo.size() / 2)) % metaInfo.size();

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;

        size_t entrySize = sizeof(metaInfo[0]);

        // prepare the scatter / gather entry
        memset(&sge, 0, sizeof(sge));

        sge.addr = (uintptr_t)(&(metaInfo[index]));
        sge.length = entrySize;
        sge.lkey = metaInfoMR->lkey;

        // prepare the send work request
        memset(&sr, 0, sizeof(sr));

        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;

        sr.num_sge = 1;
        sr.opcode = IBV_WR_RDMA_WRITE;
        sr.send_flags = IBV_SEND_SIGNALED;

        sr.wr.rdma.remote_addr = res.remote_props.meta_buf + entrySize * remoteIndex;
        sr.wr.rdma.rkey = res.remote_props.meta_rkey;

        ibv_post_send(res.qp, &sr, &bad_wr);

        poll_completion();
    }
}

void Connection::receiveDataFromRemote(size_t index) {
    setOpcode(index, rdma_data_consuming, false);

    char *ptr = ownReceiveBuffer[index]->buf;
    size_t bufferPayloadSize = ownReceiveBuffer[index]->getMaxPayloadSize();
    uint64_t dataId = *((uint64_t *)ptr);
    uint64_t dataSize = *((uint64_t *)(ptr + 32));

    if (!receiveMap.contains(dataId)) {
        uint64_t *localPtr = (uint64_t *)malloc(dataSize);
        receive_data rd = {.localPtr = localPtr,
                           .size = dataSize};

        receiveMap.insert(std::make_pair(dataId, rd));
        receiveMap[dataId].dt = (data_types)(*(uint64_t *)(ptr + 24));
    }

    uint64_t currentPackageSize = *((uint64_t *)(ptr + 8));
    uint64_t currentPackageNumber = *((uint64_t *)(ptr + 16));

    if (currentPackageSize == dataSize) {
        memcpy(receiveMap[dataId].localPtr, ptr + package_t::metaDataSize(), currentPackageSize);
        receiveMap[dataId].done = true;
        receiveMap[dataId].endTime = std::chrono::high_resolution_clock::now();
    } else {
        memcpy(receiveMap[dataId].localPtr + currentPackageNumber * bufferPayloadSize, ptr + package_t::metaDataSize(), currentPackageSize);
    }
    ownReceiveBuffer[index]->clearBuffer();

    // TODO: find suitable solution to check whether all packages have arived
    if (currentPackageNumber * bufferPayloadSize + currentPackageSize == dataSize) {
        receiveMap[dataId].done = true;
        receiveMap[dataId].endTime = std::chrono::high_resolution_clock::now();
    }

    setOpcode(index, rdma_ready, true);
}

int Connection::closeConnection(bool sendRemote) {
    globalAbort = true;

    setOpcode(metaInfo.size() / 2, rdma_shutdown, sendRemote);

    destroyResources();

    return 0;
}

int Connection::reconfigureBuffer(buffer_config_t &bufConfig) {
    std::size_t numBlocked = 0;
    bool allBlocked = false;

    while (!allBlocked) {
        while (numBlocked < bufferConfig.num_own_receive) {
            for (std::size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
                if (metaInfo[i] == rdma_ready || metaInfo[i] == rdma_reconfigure) {
                    setOpcode(i, rdma_blocked, true);
                    ++numBlocked;
                } else {
                    continue;
                }
            }
        }

        if (ownSendBuffer->metaInfo == rdma_ready) ownSendBuffer->metaInfo = rdma_blocked;

        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10us);

        allBlocked = true;

        for (std::size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
            allBlocked = allBlocked && metaInfo[i] == rdma_blocked;
        }

        allBlocked = allBlocked && ownSendBuffer->metaInfo == rdma_blocked;
    }

    res.own_mr.clear();
    res.own_buffer.clear();

    if (bufConfig.size_own_send != bufferConfig.size_own_send) {
        free(ownSendBuffer->buf);
        ibv_dereg_mr(ownSendBuffer->mr);
        delete ownSendBuffer;

        bufferConfig.size_own_send = bufConfig.size_own_send;

        setupSendBuffer();

        ownSendBuffer->mr = registerMemoryRegion(res.pd, ownSendBuffer->buf, ownSendBuffer->getBufferSize());
        assert(ownSendBuffer->mr != NULL);
    }

    res.own_mr.push_back(ownSendBuffer->mr);
    res.own_buffer.push_back(ownSendBuffer->buf);

    if (bufConfig.size_own_receive != bufferConfig.size_own_receive || bufConfig.num_own_receive != bufferConfig.num_own_receive) {
        for (auto rb : ownReceiveBuffer) {
            free(rb->buf);
            ibv_dereg_mr(rb->mr);
        }
        ownReceiveBuffer.clear();

        bufferConfig.size_own_receive = bufConfig.size_own_receive;
        bufferConfig.num_own_receive = bufConfig.num_own_receive;

        setupReceiveBuffer();

        for (auto rb : ownReceiveBuffer) {
            rb->mr = registerMemoryRegion(res.pd, rb->buf, rb->getBufferSize());
            assert(rb->mr != NULL);
        }
    }

    bufferConfig = bufConfig;

    reconfigure_data recData = {.buffer_config = bufConfig,
                                .send_buf = (uintptr_t)ownSendBuffer->buf,
                                .send_rkey = ownSendBuffer->mr->rkey};

    auto pos = 0;
    for (const auto rb : ownReceiveBuffer) {
        res.own_mr.push_back(rb->mr);
        res.own_buffer.push_back(rb->buf);
        recData.receive_buf[pos] = (uintptr_t)rb->buf;
        recData.receive_rkey[pos] = rb->mr->rkey;
        ++pos;
    }

    for (uint8_t i = 0; i < metaInfo.size() / 2; ++i) {
        if (i < bufferConfig.num_own_receive)
            setOpcode(i, rdma_ready, true);
        else
            setOpcode(i, rdma_no_op, true);
    }

    ownSendBuffer->metaInfo = rdma_ready;

    int nextFree = -1;

    while (nextFree == -1) {
        nextFree = getNextFreeReceive();
    }

    ownSendBuffer->sendReconfigure(recData, res.remote_buffer[nextFree], res.remote_rkeys[nextFree], res.qp);
    poll_completion();

    setOpcode((metaInfo.size() / 2) + nextFree, rdma_reconfigure, true);

    std::cout << "Reconfigured Buffers to: " << std::endl;
    printConnectionInfo();

    return 0;
}

int Connection::sendReconfigureBuffer(buffer_config_t &bufConfig) {
    reconfiguring = true;
    return reconfigureBuffer(bufConfig);
}

int Connection::receiveReconfigureBuffer(std::size_t index) {
    reconfiguring = true;

    char *ptr = ownReceiveBuffer[index]->buf;

    reconfigure_data *recData = (reconfigure_data *)malloc(sizeof(reconfigure_data));
    memcpy(recData, ptr, sizeof(reconfigure_data));
    recData->buffer_config = invertBufferConfig(recData->buffer_config);

    res.remote_buffer.clear();
    res.remote_rkeys.clear();
    for (uint8_t i = 0; i < metaInfo.size() / 2; ++i) {
        if (recData->receive_buf[i] == 0) continue;

        res.remote_buffer.push_back(recData->receive_buf[i]);
        res.remote_rkeys.push_back(recData->receive_rkey[i]);
    }

    res.remote_props.send_buf = recData->send_buf;
    res.remote_props.send_rkey = recData->send_rkey;

    if (recData->buffer_config.num_own_receive == bufferConfig.num_own_receive && recData->buffer_config.size_own_receive == bufferConfig.size_own_receive && recData->buffer_config.size_own_send == bufferConfig.size_own_send) {
        bufferConfig = recData->buffer_config;
        reconfiguring = false;
        setOpcode(index, rdma_ready, true);
        ownSendBuffer->metaInfo = rdma_ready;
        return 0;
    }

    reconfigureBuffer(recData->buffer_config);

    return 0;
}

int Connection::addReceiveBuffer(std::size_t quantity = 1, bool own = true) {
    buffer_config_t bufConfig = bufferConfig;
    if (own) {
        bufConfig.num_own_receive += quantity;
        if (bufConfig.num_own_receive > (metaInfo.size() / 2)) {
            std::cout << "It is only possible to have " << (metaInfo.size() / 2) << " Receive Buffer on each side!  You violated this rule! Therefore, the number of RB is set to " << (metaInfo.size() / 2) << std::endl;
            bufConfig.num_own_receive = (metaInfo.size() / 2);
        }
        if (bufConfig.num_own_receive < 1) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_own_receive = 1;
        }
    } else {
        bufConfig.num_remote_receive += quantity;
        if (bufConfig.num_remote_receive > (metaInfo.size() / 2)) {
            std::cout << "It is only possible to have " << (metaInfo.size() / 2) << " Receive Buffer on each side!  You violated this rule! Therefore, the number of RB is set to " << (metaInfo.size() / 2) << std::endl;
            bufConfig.num_remote_receive = (metaInfo.size() / 2);
        }
        if (bufConfig.num_remote_receive < 1) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_remote_receive = 1;
        }
    }

    return sendReconfigureBuffer(bufConfig);
}

int Connection::removeReceiveBuffer(std::size_t quantity = 1, bool own = true) {
    buffer_config_t bufConfig = bufferConfig;
    if (own) {
        bufConfig.num_own_receive -= quantity;
        if (bufConfig.num_own_receive > (metaInfo.size() / 2)) {
            std::cout << "Congratulation! You reached a state that should not be possible! The number of RB is set to 1." << std::endl;
            bufConfig.num_own_receive = 1;
        }
        if (bufConfig.num_own_receive < 1) {
            std::cout << "There has to be at least 1 RB on each side! You violated this rule! Therefore, the number of RB is set to 1." << std::endl;
            bufConfig.num_own_receive = 1;
        }
    } else {
        bufConfig.num_remote_receive -= quantity;
        if (bufConfig.num_remote_receive > (metaInfo.size() / 2)) {
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

int Connection::pendingBufferCreation() {
    return !reconfiguring;
}

Connection::~Connection() {
    closeConnection();
}

void Connection::pullDataFromRemote(std::size_t index, bool consume) {
    ownReceiveBuffer[index]->post_request(bufferConfig.size_own_receive, IBV_WR_RDMA_READ, res.remote_props.send_buf + (index * bufferConfig.size_own_receive), res.remote_props.send_rkey, res.qp, ownReceiveBuffer[index]->buf, 0);
    poll_completion();

    if (consume) {
        this->consume(index);
    } else {
        setOpcode(index, rdma_ready, true);
        ownReceiveBuffer[index]->clearBuffer();
    }
}

int Connection::throughputTest(std::string logName, strategies strat) {
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

    uint64_t packageID = generatePackageID();

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ThroughputTest] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ThroughputTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;

            package_t package(packageID, maxDataToWrite, 1, type_package, remainingSize, copy);

            timePoint s_ts, e_ts;

            if (strat == push) {
                std::tie(s_ts, e_ts) = throughputTestPush(package, remainingSize, maxPayloadSize, maxDataToWrite);
            } else if (strat == pull) {
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

int Connection::consumingTest(std::string logName, strategies strat) {
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

    uint64_t packageID = generatePackageID();

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ConsumeTest] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ConsumeTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;

            package_t package(packageID, maxDataToWrite, 1, type_package, remainingSize, copy);

            timePoint s_ts, e_ts;

            if (strat == push) {
                std::tie(s_ts, e_ts) = consumingTestPush(package, remainingSize, maxPayloadSize, maxDataToWrite);
            } else if (strat == pull) {
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
    size_t packNum = 0;
    size_t maxPackNum;

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

void Connection::consume(size_t index) {
    setOpcode(index, rdma_data_consuming, false);

    char *ptr = ownReceiveBuffer[index]->buf;
    size_t bufferPayloadSize = ownReceiveBuffer[index]->getMaxPayloadSize();
    uint64_t dataId = *((uint64_t *)ptr);
    uint64_t dataSize = *((uint64_t *)(ptr + 32));
    uint64_t *localPtr = (uint64_t *)malloc(dataSize);
    uint64_t currentPackageSize = *((uint64_t *)(ptr + 8));
    uint64_t currentPackageNumber = *((uint64_t *)(ptr + 16));
    data_types dataType = (data_types)(*(uint64_t *)(ptr + 24));

    // std::cout << bufferPayloadSize << "\t" << dataId << "\t" << dataSize << "\t" << currentPackageSize << "\t" << currentPackageNumber << "\t" << dataType << std::endl;

    memcpy(localPtr, ptr + package_t::metaDataSize(), currentPackageSize);

    ownReceiveBuffer[index]->clearBuffer();

    setOpcode(index, rdma_ready, true);
    free(localPtr);
}

int Connection::throughputTestMultiThread(std::string logName, strategies strat) {
    while (reconfiguring) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
    }

    ownSendBuffer->clearBuffer();

    if (strat == pull) {
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

    uint64_t packageID = generatePackageID();

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            memset(ready_vec, 0, thread_cnt * sizeof(bool));
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ThroughputTest] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ThroughputTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;
            size_t maxPackNum;
            work_queue.resize(thread_cnt);

            package_t package(packageID, maxDataToWrite, 1, type_package, remainingSize, copy);

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
                if (strat == push) {
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
                } else if (strat == pull) {
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
    free(ready_vec);

    if (strat == pull) {
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

int Connection::consumingTestMultiThread(std::string logName, strategies strat) {
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

    uint64_t packageID = generatePackageID();

    std::ios_base::fmtflags f(std::cout.flags());
    for (std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1) {
        for (std::size_t iteration = 0; iteration < TEST_ITERATIONS; ++iteration) {
            memset(ready_vec, 0, thread_cnt * sizeof(bool));
            uint64_t remainingSize = elementCount * sizeof(uint64_t);
            uint64_t maxPayloadSize = bufferConfig.size_remote_receive - package_t::metaDataSize();
            uint64_t maxDataToWrite = (maxPayloadSize / sizeof(uint64_t)) * sizeof(uint64_t);
            std::cout << "[ThroughputTest] Max Payload is: " << maxPayloadSize << std::endl;
            std::cout << "[ThroughputTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
            uint64_t *copy = d.data;
            size_t maxPackNum;
            work_queue.resize(thread_cnt);

            package_t package(packageID, maxDataToWrite, 1, type_package, remainingSize, copy);

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
                if (strat == push) {
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
                } else if (strat == pull) {
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

void Connection::workMultiThread() {
    conStat = active;
    std::vector<std::thread *> pool;

    if (readWorker) {
        globalAbort = true;
        readWorker->join();
        globalAbort = false;
    }

    size_t thread_cnt = (int)(bufferConfig.num_own_receive / 2);
    thread_cnt = thread_cnt > 0 ? thread_cnt : 1;

    auto mtConsume = [this](size_t index, size_t thrdcnt) -> void {
        bool abort = false;
        std::ios_base::fmtflags f(std::cout.flags());

        while (!abort) {
            for (size_t id = index; id < bufferConfig.num_own_receive; id += thrdcnt) {
                switch (metaInfo[id]) {
                    case rdma_data_finished: {
                        consume(id);
                    } break;
                    case rdma_test_finished: {
                        setOpcode(id, rdma_ready, true);
                        abort = true;
                    } break;
                    case rdma_pull_read: {
                        pullDataFromRemote(id, false);
                    } break;
                    case rdma_pull_consume: {
                        pullDataFromRemote(id, true);
                    } break;
                    default:
                        continue;
                }
            }
            std::cout.flags(f);
        }
        std::cout << "[mtConsume] Ending through abort." << std::endl;
    };

    for (size_t tid = 0; tid < thread_cnt; ++tid) {
        pool.emplace_back(new std::thread(mtConsume, tid, thread_cnt));
        setOpcode(tid, rdma_ready, true);
    }

    std::for_each(pool.begin(), pool.end(), [](std::thread *t) { t->join(); delete t; });
    pool.clear();

    readWorker = new std::thread(check_receive, &globalAbort);
}