#include "Connection.h"

#include <stdlib.h>

#include <cmath>
#include <cstring>

#include "util.h"

Connection::Connection(config_t _config, buffer_config_t _bufferConfig) : globalAbort(false) {
    config = _config;
    bufferConfig = _bufferConfig;
    res.sock = -1;

    initTCP();

    exchangeBufferInfo();

    sock_close(res.sock);

    setOpcode(0, rdma_data_ready, true);
    for (size_t rbi = 0; rbi < ownReceiveBuffer.size(); ++rbi) {
        setOpcode(rbi, rdma_data_ready, true);
    }

    check_receive = [this](bool *abort) -> void {
        using namespace std::chrono_literals;
        std::ios_base::fmtflags f(std::cout.flags());
        std::cout << "Starting monitoring thread for connection!" << std::endl;
        size_t metaSize = std::size(metaInfo);

        while (!*abort) {
            std::this_thread::sleep_for(10ms);
            for (size_t i = 1; i < metaSize / 2; ++i) {
                switch (metaInfo[i]) {
                    case rdma_create_region: {
                        // createRdmaRegion(config, communicationRegion);
                    }; break;
                    case rdma_delete_region: {
                        // deleteRdmaRegion(communicationRegion);
                        // return;
                    }; break;
                    case rdma_data_ready: {
                        // readCommittedData(communicationRegion);
                    }; break;
                    case rdma_data_fetch: {
                        // sendDataToRemote(communicationRegion);
                    } break;
                    case rdma_data_finished: {
                        receiveDataFromRemote(i - 1);
                    }; break;
                    case rdma_data_receive: {
                        // receiveDataFromRemote(communicationRegion, false);
                    } break;
                    case rdma_tput_test: {
                        // throughputTest(communicationRegion);
                    } break;
                    case rdma_consume_test: {
                        // consumingTest(communicationRegion);
                    } break;
                    case rdma_mt_tput_test: {
                        // mt_throughputTest(communicationRegion);
                    } break;
                    case rdma_mt_consume_test: {
                        // mt_consumingTest(communicationRegion);
                    } break;
                    case rdma_shutdown: {
                        closeConnection();
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

    readWorker = new std::thread(check_receive, &globalAbort);
}

void Connection::setupSendBuffer() {
    ownSendBuffer = new SendBuffer(bufferConfig.size_own_send);
    metaInfo[0] = rdma_ready;
}

void Connection::setupReceiveBuffer() {
    for (size_t i = 0; i < bufferConfig.num_own_receive; ++i) {
        ownReceiveBuffer.push_back(new ReceiveBuffer(bufferConfig.size_own_receive));
        metaInfo[i + 1] = rdma_ready;
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
                  config.server_name, config.tcp_port);
            exit(EXIT_FAILURE);
        }
    } else {
        // @client
        INFO("Waiting on port %d for TCP connection\n", config.tcp_port);
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
    char temp_char;

    if (config.client_mode) {
        receive_tcp(res.sock, sizeof(struct cm_con_data_t), (char *)&tmp_con_data);

        bufferConfig = invertBufferConfig(tmp_con_data.buffer_config);
    }

    setupSendBuffer();
    setupReceiveBuffer();

    createResources();

    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        CHECK(ibv_query_gid(res.ib_ctx, config.ib_port, config.gid_idx, &my_gid));
    }

    std::vector<uint64_t> receive_buf;
    std::vector<uint32_t> receive_rkey;

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    local_con_data.meta_buf = htonll((uintptr_t)&metaInfo);
    local_con_data.meta_rkey = htonl(metaInfoMR->rkey);
    local_con_data.send_buf = htonll((uintptr_t)&ownSendBuffer->buf);
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
        sock_sync_data(res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data);
    } else {
        send_tcp(res.sock, sizeof(struct cm_con_data_t), (char *)&local_con_data);
    }

    remote_con_data.meta_buf = ntohll(tmp_con_data.meta_buf);
    remote_con_data.meta_rkey = ntohl(tmp_con_data.meta_rkey);
    remote_con_data.send_buf = ntohll(tmp_con_data.send_buf);
    remote_con_data.send_rkey = ntohl(tmp_con_data.send_rkey);
    remote_con_data.receive_num = tmp_con_data.receive_num;
    // remote_con_data.receive_buf = tmp_con_data.receive_buf;
    // remote_con_data.receive_rkey = tmp_con_data.receive_rkey;
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    remote_con_data.buffer_config = tmp_con_data.buffer_config;
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    // save the remote side attributes, we will need it for the post SR
    res.remote_props = remote_con_data;

    std::vector<uint64_t> temp_buf(tmp_con_data.receive_buf, tmp_con_data.receive_buf + remote_con_data.receive_num);
    std::vector<uint32_t> temp_rkey(tmp_con_data.receive_rkey, tmp_con_data.receive_rkey + remote_con_data.receive_num);

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

    metaInfoMR = registerMemoryRegion(protection_domain, &metaInfo, sizeof(metaInfo));

    /* Create a completion queue */
    int cq_size = 0x10;
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
    queue_pair_init_attr.sq_sig_all = 1;        // if not set 0, all work requests submitted to SQ will always generate a Work Completion.
    queue_pair_init_attr.send_cq = cq;          // completion queue can be shared or you can use distinct completion queues.
    queue_pair_init_attr.recv_cq = cq;          // completion queue can be shared or you can use distinct completion queues.
    queue_pair_init_attr.cap.max_send_wr = 1;   // increase if you want to keep more send work requests in the SQ.
    queue_pair_init_attr.cap.max_recv_wr = 1;   // increase if you want to keep more receive work requests in the RQ.
    queue_pair_init_attr.cap.max_send_sge = 1;  // increase if you allow send work requests to have multiple scatter gather entry (SGE).
    queue_pair_init_attr.cap.max_recv_sge = 1;  // increase if you allow receive work requests to have multiple scatter gather entry (SGE).

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

bool Connection::sendData(std::string &data) {
    if (metaInfo[0] == rdma_no_op) {
        std::cout << "There is no buffer for sending initialized!" << std::endl;
        return false;
    }

    busy = true;

    while (metaInfo[0] != rdma_ready) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ns);
        continue;
    }

    uint64_t packageID = generatePackageID();

    setOpcode(0, rdma_sending, false);

    const char *dataCString = data.c_str();

    size_t dataSize = std::strlen(dataCString);
    uint32_t ownSendToRemoteReceiveRatio = getOwnSendToRemoteReceiveRatio();
    size_t sendPackages = std::ceil(dataSize / (ownSendBuffer->getBufferSize() - (META_INFORMATION_SIZE * ownSendToRemoteReceiveRatio)));
    size_t alreadySentSize = 0;
    int currentSize = bufferConfig.size_remote_receive - META_INFORMATION_SIZE;
    uint8_t nextFree;

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
                    return false;
                }

                continue;
            }

            setOpcode(std::size(metaInfo) + 1 + nextFree, rdma_data_receive, false);

            if (dataSize < alreadySentSize + currentSize) currentSize = dataSize - alreadySentSize;
            ownSendBuffer->loadData(dataCString + alreadySentSize, ownSendBuffer->buf + k * bufferConfig.size_remote_receive, dataSize, currentSize, i * ownSendToRemoteReceiveRatio + k, type_string, packageID);
            ownSendBuffer->post_send(currentSize + META_INFORMATION_SIZE, IBV_WR_RDMA_WRITE, (char *)res.remote_buffer[nextFree], res.remote_rkeys[nextFree], res.qp);

            poll_completion();

            alreadySentSize += currentSize;
            setOpcode(std::size(metaInfo) + 1 + nextFree, rdma_data_finished, true);
        }
    }

    setOpcode(0, rdma_ready, true);

    busy = false;
    return true;
}

uint64_t Connection::generatePackageID() {
    return std::rand();
}

int Connection::getNextFreeReceive() {
    size_t metaSize = std::size(metaInfo);
    for (size_t i = (metaSize / 2) + 1; i < metaSize; ++i) {
        if (metaInfo[i] == rdma_ready) return i - (metaSize / 2) - 1;
    }

    return -1;
}

uint32_t Connection::getOwnSendToRemoteReceiveRatio() {
    return std::floor(ownSendBuffer->getBufferSize() / bufferConfig.size_remote_receive);
}

void Connection::setOpcode(size_t index, rdma_handler_communication opcode, bool sendToRemote) {
    metaInfo[index] = opcode;

    if (sendToRemote) {
        size_t remoteIndex = ((index + std::size(metaInfo)) / 2) % std::size(metaInfo);

        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr *bad_wr = NULL;

        size_t entrySize = sizeof metaInfo[0];

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
    setOpcode(index + 1, rdma_data_consuming, false);

    char *ptr = ownReceiveBuffer[index]->buf;
    size_t bufferPayloadSize = ownReceiveBuffer[index]->getMaxPayloadSize();
    uint64_t dataId = (uint64_t)ptr;
    uint64_t dataSize = (uint64_t)(ptr + 32);

    if (!receiveMap.contains(dataId)) {
        uint64_t *localPtr = (uint64_t *)malloc(dataSize);
        receive_data rd = {.localPtr = localPtr};

        receiveMap.insert(std::make_pair(dataId, rd));
        receiveMap[dataId].dt = (data_types)(uint64_t)(ptr + 24);
    }

    uint64_t currentPackageSize = (uint64_t)(ptr + 8);
    uint64_t currentPackageNumber = (uint64_t)(ptr + 16);

    if (currentPackageSize == dataSize) {
        memcpy(receiveMap[dataId].localPtr, ptr + META_INFORMATION_SIZE, currentPackageSize);
        receiveMap[dataId].done = true;

        std::cout << receiveMap[dataId].localPtr << std::endl;
    } else {
        memcpy(receiveMap[dataId].localPtr + currentPackageNumber * bufferPayloadSize, ptr + META_INFORMATION_SIZE, currentPackageSize);
    }

    // TODO: find suitable solution to check whether all packages have arived
    if (currentPackageNumber * bufferPayloadSize + currentPackageSize == dataSize) {
        receiveMap[dataId].done = true;
    }

    setOpcode(index + 1, rdma_data_ready, true);
}

bool Connection::closeConnection() {
    std::cout << "This function closes a connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::addReceiveBuffer(unsigned int quantity = 1) {
    std::cout << "This function adds a receive buffer to this connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::removeReceiveBuffer(unsigned int quantity = 1) {
    std::cout << "This function removes a receive buffer from this connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::resizeReceiveBuffer(size_t newSize) {
    std::cout << "This function resizes all receive buffer of this connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::resizeSendBuffer(size_t newSize) {
    std::cout << "This function resizes the send buffer of this connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::pendingBufferCreation() {
    std::cout << "This function tells whether a buffer creation is pending for this connection but isn't implemented yet. Sorry!" << std::endl;
    return true;

    // const std::lock_guard< std::mutex > lock( poolMutex );
    // return !pool.empty();
}

Connection::~Connection() {
    // stop();
    // closeConnection();
}