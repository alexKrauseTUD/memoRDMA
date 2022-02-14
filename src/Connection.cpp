#include "Connection.h"

Connection::Connection() {}

Connection::Connection(config_t _config, buffer_config_t _bufferConfig) : globalAbort(false) {
    config = _config;
    bufferConfig = _bufferConfig;
    res.sock = -1;

    setupSendBuffer();
    setupReceiveBuffer();
    initTCP();
    createResources();

    // check_receive = [this]( Buffer* communicationRegion, config_t* config, bool* abort ) -> void {
    // 	using namespace std::chrono_literals;
    // 	std::ios_base::fmtflags f( std::cout.flags() );
    // 	std::cout << "Starting monitoring thread for region " << communicationRegion << std::endl;
    // 	while( !*abort ) {
    // 		std::this_thread::sleep_for( 10ms );
    // 		switch( communicationRegion->receivePtr()[0] ) {
    // 			case rdma_create_region: {
    // 				createRdmaRegion( config, communicationRegion );
    // 			}; break;
    // 			case rdma_delete_region: {
    // 				deleteRdmaRegion( communicationRegion );
    // 				return;
    // 			}; break;
    // 			case rdma_data_ready: {
    // 				readCommittedData( communicationRegion );
    // 			}; break;
    // 			case rdma_data_fetch: {
    // 				sendDataToRemote( communicationRegion );
    // 			} break;
    // 			case rdma_data_finished: {
    // 				receiveDataFromRemote( communicationRegion, true );
    // 			}; break;
    // 			case rdma_data_receive: {
    // 				receiveDataFromRemote( communicationRegion, false );
    // 			} break;
    // 			case rdma_tput_test: {
    // 				throughputTest( communicationRegion );
    // 			} break;
    // 			case rdma_consume_test: {
    // 				consumingTest( communicationRegion );
    // 			} break;
    // 			case rdma_mt_tput_test: {
    // 				mt_throughputTest( communicationRegion );
    // 			} break;
    // 			case rdma_mt_consume_test: {
    // 				mt_consumingTest( communicationRegion );
    // 			} break;
    // 			case rdma_shutdown: {
    // 				if ( config->client_mode ) {
    // 					std::cout << "[CommRegion] Received RDMA_SHUTDOWN" << std::endl;
    // 					RDMACommunicator::getInstance().stop();
    // 				} else {
    // 					communicationRegion->clearCompleteBuffer();
    // 				}
    // 			}; break;
    // 			default: {
    // 				continue;
    // 			}; break;
    // 		}
    // 		std::cout.flags( f );
    // 	}
    // 	std::cout << "[check_receive] Ending through global abort." << std::endl;
    // };

    // check_regions = [this]( bool* abort ) -> void {
    // 	using namespace std::chrono_literals;
    // 	while (!*abort) {
    // 		for ( auto it = pool.begin(); it != pool.end(); ) {
    // 			if ( *std::get<0>(it->second) ) {
    // 				const std::lock_guard< std::mutex > lock( poolMutex );
    // 				/* Memory region created, old thread can be let go */
    // 				std::cout << "Joining thread " << it->first << std::endl;
    // 				std::get<2>(it->second)->join();
    // 				/* Spawning new thread to listen to it */
    // 				auto newRegion = RDMAHandler::getInstance().getRegion( *std::get<1>(it->second) );
    // 				regionThreads.emplace_back( new std::thread(check_receive, newRegion, &config, abort) );
    // 				std::cout << "Spawned new listener thread for region: " << *std::get<1>(it->second) << std::endl;
    // 				/* Cleanup & remove element from global map */
    // 				free( std::get<2>( it->second ) ); // thread pointer
    // 				free( std::get<1>( it->second ) ); // regionid pointer
    // 				it = pool.erase( it );
    // 			}
    // 		}
    // 		std::this_thread::sleep_for( 50ms );
    // 	}
    // 	std::cout << "[check_regions] Ending through global abort." << std::endl;
    // };
}

void Connection::setupSendBuffer() {
    SendBuffer *buffer = new SendBuffer(bufferConfig.size_own_send);

    ownSendBuffer = buffer;
}

void Connection::setupReceiveBuffer() {
    for (size_t i = 0; i < bufferConfig.size_own_receive; ++i) {
        ReceiveBuffer *buffer = new ReceiveBuffer(bufferConfig.size_own_receive);

        ownReceiveBuffer.push_back(buffer);
    }
}

struct ibv_mr *Connection::registerMemoryRegion(struct ibv_pd *pd, void *buffer, size_t size) {
    return ibv_reg_mr(pd, buffer, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
}

void Connection::initTCP() {
    if (!config.server_name.empty()) {
        // @client
        res.sock = sock_connect(config.server_name, config.tcp_port);
        if (res.sock < 0) {
            ERROR("Failed to establish TCP connection to server %s, port %d\n",
                  config.server_name, config.tcp_port);
            exit(EXIT_FAILURE);
        }
    } else {
        // @server
        INFO("Waiting on port %d for TCP connection\n", config.tcp_port);
        res.sock = sock_connect(NULL, config.tcp_port);
        if (res.sock < 0) {
            ERROR("Failed to establish TCP connection with client on port %d\n",
                  config.tcp_port);
            exit(EXIT_FAILURE);
        }
    }
    INFO("TCP connection was established\n");
}

void Connection::createResources() {
    // https://insujang.github.io/2020-02-09/introduction-to-programming-infiniband/

    struct ibv_context *context = createContext();

    // query port properties
    CHECK(ibv_query_port(res.ib_ctx, config.ib_port, &res.port_attr));

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

    metaInfoMR = registerMemoryRegion(protection_domain, &metaInfo, sizeof(metaInfo) * 8);

    /* Create a completion queue */
    int cq_size = 0x10;
    struct ibv_cq *completion_queue = ibv_create_cq(context, cq_size, nullptr, nullptr, 0);

    /* Create a queue pair */
    struct ibv_qp *queue_pair = createQueuePair(protection_domain, completion_queue);

    /* Exchange identifier information to establish connection and change the queue pair state */
    changeQueuePairStateToInit(queue_pair);
    // changeQueuePairStateToRTR(queue_pair, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid);
    // changeQueuePairStateToRTS(queue_pair);

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

bool Connection::changeQueuePairStateToInit(struct ibv_qp *queue_pair) {
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

    return ibv_modify_qp(queue_pair, &init_attr, flags) == 0 ? true : false;
}

bool Connection::changeQueuePairStateToRTR(struct ibv_qp *queue_pair, uint32_t destination_qp_number, uint16_t destination_local_id, uint8_t *destination_global_id) {
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

    return ibv_modify_qp(queue_pair, &rtr_attr, flags) == 0 ? true : false;
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

    return ibv_modify_qp(qp, &attr, flags) == 0 ? true : false;
}

// Connect the QP, then transition the server side to RTR, sender side to RTS.
void Connection::connectQpTCP(config_t &config) {
    std::cout << "End of current implementation." << std::endl;

    return;

    // struct cm_con_data_t local_con_data;
    // struct cm_con_data_t remote_con_data;
    // struct cm_con_data_t tmp_con_data;
    // char temp_char;
    // union ibv_gid my_gid;

    // memset(&my_gid, 0, sizeof(my_gid));

    // if (config.gid_idx >= 0)
    // {
    // 	CHECK(ibv_query_gid(res.ib_ctx, config.ib_port, config.gid_idx,
    // 						&my_gid));
    // }

    // // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // // etc. exchange using TCP sockets info required to connect QPs
    // local_con_data.addr = htonll((uintptr_t)region.res.buf);
    // local_con_data.rkey = htonl(region.res.mr->rkey);
    // local_con_data.qp_num = htonl(region.res.qp->qp_num);
    // local_con_data.lid = htons(region.res.port_attr.lid);
    // memcpy(local_con_data.gid, &my_gid, 16);

    // INFO("\n Local LID      = 0x%x\n", region.res.port_attr.lid);

    // sock_sync_data(region.res.sock, sizeof(struct cm_con_data_t),
    // 			   (char *)&local_con_data, (char *)&tmp_con_data);

    // remote_con_data.addr = ntohll(tmp_con_data.addr);
    // remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    // remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    // remote_con_data.lid = ntohs(tmp_con_data.lid);
    // memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    // // save the remote side attributes, we will need it for the post SR
    // region.res.remote_props = remote_con_data;
    // // \end exchange required info

    // INFO("Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);
    // INFO("Remote rkey = 0x%x\n", remote_con_data.rkey);
    // INFO("Remote QP number = 0x%x\n", remote_con_data.qp_num);
    // INFO("Remote LID = 0x%x\n", remote_con_data.lid);

    // if (config.gid_idx >= 0)
    // {
    // 	uint8_t *p = remote_con_data.gid;
    // 	int i;
    // 	printf("Remote GID = ");
    // 	for (i = 0; i < 15; i++)
    // 		printf("%02x:", p[i]);
    // 	printf("%02x\n", p[15]);
    // }

    // // modify the QP to init
    // region.modify_qp_to_init(config, region.res.qp);

    // // modify the QP to RTR
    // region.modify_qp_to_rtr(config, region.res.qp, remote_con_data.qp_num, remote_con_data.lid,
    // 						remote_con_data.gid);

    // // modify QP state to RTS
    // region.modify_qp_to_rts(region.res.qp);

    // // sync to make sure that both sides are in states that they can connect to
    // // prevent packet lose
    // char Q = 'Q';
    // sock_sync_data(region.res.sock, 1, &Q, &temp_char);
    // return;
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
    std::cout << "This function sends data over a connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::close() {
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

bool Connection::resizeReceiveBuffer(std::size_t newSize) {
    std::cout << "This function resizes all receive buffer of this connection but isn't implemented yet. Sorry!" << std::endl;
    return true;
}

bool Connection::resizeSendBuffer(std::size_t newSize) {
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
    // closeAllConnections();
}