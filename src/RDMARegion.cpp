#include <util.h>
#include <RDMARegion.h>

RDMARegion::RDMARegion() {

}

RDMARegion::~RDMARegion() {
    resources_destroy();
}

int RDMARegion::resources_create(struct config_t& config, bool initTCP ) {
    memset(&res, 0, sizeof(res));
    res.sock = -1;

    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;

    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;

    if ( initTCP ) {
        if (config.server_name) {
            // @client
            res.sock = sock_connect(config.server_name, config.tcp_port);
            if (res.sock < 0) {
                ERROR("Failed to establish TCP connection to server %s, port %d\n",
                    config.server_name, config.tcp_port);
                goto die;
            }
        } else {
            // @server
            INFO("Waiting on port %d for TCP connection\n", config.tcp_port);
            res.sock = sock_connect(NULL, config.tcp_port);
            if (res.sock < 0) {
                ERROR("Failed to establish TCP connection with client on port %d\n",
                    config.tcp_port);
                goto die;
            }
        }
        INFO("TCP connection was established\n");
    }

    INFO("Searching for IB devices in host\n");

    // \begin acquire a specific device
    // get device names in the system
    dev_list = ibv_get_device_list(&num_devices);
    assert(dev_list != NULL);

    if (num_devices == 0) {
        ERROR("Found %d device(s)\n", num_devices);
        goto die;
    }

    INFO("Found %d device(s)\n", num_devices);

    // search for the specific device we want to work with
    for (i = 0; i < num_devices; i++) {
        if (!config.dev_name) {
            config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            INFO("Device not specified, using first one found: %s\n",
                 config.dev_name);
        }

        if (strcmp(ibv_get_device_name(dev_list[i]), config.dev_name) == 0) {
            ib_dev = dev_list[i];
            break;
        }
    }

    // device wasn't found in the host
    if (!ib_dev) {
        ERROR("IB device %s wasn't found\n", config.dev_name);
        goto die;
    }

    // get device handle
    res.ib_ctx = ibv_open_device(ib_dev);
    assert(res.ib_ctx != NULL);
    // \end acquire a specific device

    // query port properties
    CHECK(ibv_query_port(res.ib_ctx, config.ib_port, &res.port_attr));

    // PD
    res.pd = ibv_alloc_pd(res.ib_ctx);
    assert(res.pd != NULL);

    // Create a CQ with X entries
    cq_size = 5; // X
    res.cq = ibv_create_cq(res.ib_ctx, cq_size, NULL, NULL, 0);
    assert(res.cq != NULL);

    // a buffer to hold the data
    size = BUFF_SIZE;
    res.buf = (char*) calloc( 1, size );
    assert(res.buf != NULL);

    // register the memory buffer
    mr_flags =  IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE;
    res.mr = ibv_reg_mr(res.pd, res.buf, size, mr_flags);
    assert(res.mr != NULL);

    INFO(
        "MR was registered with addr=%p, lkey= 0x%x, rkey= 0x%x, flags= 0x%x\n",
        res.buf, res.mr->lkey, res.mr->rkey, mr_flags);

    // \begin create the QP
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = res.cq;
    qp_init_attr.recv_cq = res.cq;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    res.qp = ibv_create_qp(res.pd, &qp_init_attr);
    assert(res.qp != NULL);

    INFO("QP was created, QP number= 0x%x\n", res.qp->qp_num);
    // \end create the QP

    // FIXME: hard code here
    return 0;
die:
    exit(EXIT_FAILURE);
}

// Adding remote info to local region and set local QP to INIT->RTR->RTS
void RDMARegion::resources_sync_local( config_t* config, struct cm_con_data_t& tmp_con_data ) {
    struct cm_con_data_t remote_con_data;
    remote_con_data.addr = tmp_con_data.addr;
    remote_con_data.rkey = tmp_con_data.rkey;
    remote_con_data.qp_num = tmp_con_data.qp_num;
    remote_con_data.lid = tmp_con_data.lid;
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    res.remote_props = remote_con_data;

    if (config->gid_idx >= 0) {
        uint8_t *p = remote_con_data.gid;
        int i;
        printf("Remote GID = ");
        for (i = 0; i < 15; i++)
            printf("%02x:", p[i]);
        printf("%02x\n", p[15]);
    }

    modify_qp_to_init(*config, res.qp);
    modify_qp_to_rtr(*config, res.qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid);
    modify_qp_to_rts(res.qp);
}

// Transition a QP from the RESET to INIT state
int RDMARegion::modify_qp_to_init(struct config_t& config, struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = config.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags =  IBV_ACCESS_LOCAL_WRITE | 
                            IBV_ACCESS_REMOTE_READ |
                            IBV_ACCESS_REMOTE_WRITE;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    CHECK(ibv_modify_qp(qp, &attr, flags));

    INFO("Modify QP to INIT done!\n");

    // FIXME: ;)
    return 0;
}

// Transition a QP from the INIT to RTR state, using the specified QP number
int RDMARegion::modify_qp_to_rtr(struct config_t& config, struct ibv_qp *qp, uint32_t remote_qpn,uint16_t dlid, uint8_t *dgid) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_256;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = config.ib_port;

    if (config.gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = config.gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    CHECK(ibv_modify_qp(qp, &attr, flags));

    INFO("Modify QP to RTR done!\n");

    // FIXME: ;)
    return 0;
}

// Transition a QP from the RTR to RTS state
int RDMARegion::modify_qp_to_rts(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12; // 18
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    CHECK(ibv_modify_qp(qp, &attr, flags));

    INFO("Modify QP to RTS done!\n");

    // FIXME: ;)
    return 0;
}

// Cleanup and deallocate all resources used
int RDMARegion::resources_destroy() {
    printf("Freeing...");
	ibv_destroy_qp(res.qp);
    ibv_dereg_mr(res.mr);
    free(res.buf);
    ibv_destroy_cq(res.cq);
    ibv_dealloc_pd(res.pd);
    ibv_close_device(res.ib_ctx);
    close(res.sock);
    printf("Done.");

    // FIXME: ;)
    return 0;
}

char* RDMARegion::writePtr() {
    return res.buf;
}

char* RDMARegion::receivePtr() {
    return res.buf + maxWriteSize();
}

void RDMARegion::print() const {
    std::ios_base::fmtflags f( std::cout.flags() );
    std::cout << std::hex;
    std::cout << "\tRemote address = " << res.remote_props.addr << std::endl;
    std::cout << "\tRemote rkey = " <<  res.remote_props.rkey << std::endl;
    std::cout << "\tRemote QP number = " << res.remote_props.qp_num << std::endl;
    std::cout << "\tRemote LID = " << res.remote_props.lid << std::endl;
    std::cout.flags( f );
}

void RDMARegion::clearReadCode() {
    memset( receivePtr(), 0, 1 );
}

void RDMARegion::clearCompleteBuffer() {
    memset( writePtr(), 0, BUFF_SIZE );
}

void RDMARegion::clearWriteBuffer() {
    memset( writePtr(), 0, maxWriteSize()/2 );
}

void RDMARegion::clearReadBuffer() {
    memset( receivePtr(), 0, maxWriteSize()/2 );
}

void RDMARegion::setSendData( std::string s ) {
    strcpy( writePtr()+1, s.c_str() );
    post_send(&res, s.size()+1, IBV_WR_RDMA_WRITE, BUFF_SIZE/2) ;
    poll_completion(&res);
}

void RDMARegion::setSendData( uint64_t* data, uint64_t totalSize, uint64_t currentSize ) {
    clearCompleteBuffer();
    writePtr()[0] = (char)rdma_no_op;
    memcpy( writePtr()+1, &totalSize, sizeof(totalSize) );
    memcpy( writePtr()+9, &currentSize, sizeof(currentSize) );
    memcpy( writePtr()+17, data, currentSize );
    post_send(&res, currentSize+17, IBV_WR_RDMA_WRITE, BUFF_SIZE/2) ; // +1 for the "empty" commit code byte
    poll_completion(&res);
}

void RDMARegion::setCommitCode( rdma_handler_communication opcode ) {
    writePtr()[0] = (char)opcode;
    post_send(&res, sizeof(char), IBV_WR_RDMA_WRITE, BUFF_SIZE/2 );
    poll_completion(&res);
}
