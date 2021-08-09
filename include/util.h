#ifndef UTILS_H
#define UTILS_H

#include <RDMARegion.h>
#include <string>
#include <stdio.h>

#include <assert.h>
#include <byteswap.h>
#include <endian.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>

#define MAX_POLL_CQ_TIMEOUT 30000 // ms
#define BUFF_SIZE 2048

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

#define ERROR(fmt, args...)                                                    \
    { fprintf(stderr, "ERROR: %s(): " fmt, __func__, ##args); }
#define ERR_DIE(fmt, args...)                                                  \
    {                                                                          \
        ERROR(fmt, ##args);                                                    \
        exit(EXIT_FAILURE);                                                    \
    }
#define INFO(fmt, args...)                                                     \
    { printf("INFO: %s(): " fmt, __func__, ##args); }
#define WARN(fmt, args...)                                                     \
    { printf("WARN: %s(): " fmt, __func__, ##args); }

#define CHECK(expr)                                                            \
    {                                                                          \
        int rc = (expr);                                                       \
        if (rc != 0) {                                                         \
            perror(strerror(errno));                                           \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    }

// structure of test parameters
struct config_t {
    const char *dev_name; // IB device name
    char *server_name;    // server hostname
    uint32_t tcp_port;    // server TCP port
    int ib_port;          // local IB port to work with
    int gid_idx;          // GID index to use
};


// \begin socket operation
//
// For simplicity, the example program uses TCP sockets to exchange control
// information. If a TCP/IP stack/connection is not available, connection
// manager (CM) may be used to pass this information. Use of CM is beyond the
// scope of this example.

// Connect a socket. If servername is specified a client connection will be
// initiated to the indicated server and port. Otherwise listen on the indicated
// port for an incoming connection.
static int sock_connect(const char *server_name, int port) {
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
                             .ai_protocol = 0};

    // resolve DNS address, user sockfd as temp storage
    sprintf(service, "%d", port);
    CHECK(getaddrinfo(server_name, service, &hints, &resolved_addr));

    for (iterator = resolved_addr; iterator != NULL; iterator = iterator->ai_next) {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        assert(sockfd >= 0);

        if (server_name == NULL) {
            // Server mode: setup listening socket and accept a connection
            listenfd = sockfd;
            CHECK(bind(listenfd, iterator->ai_addr, iterator->ai_addrlen));
            CHECK(listen(listenfd, 1));
            sockfd = accept(listenfd, NULL, 0);
        } else {
            // Client mode: initial connection to remote
            CHECK(connect(sockfd, iterator->ai_addr, iterator->ai_addrlen));
        }
    }

    return sockfd;
}

// Sync data across a socket. The indicated local data will be sent to the
// remote. It will then wait for the remote to send its data back. It is
// assumned that the two sides are in sync and call this function in the proper
// order. Chaos will ensure if they are not. Also note this is a blocking
// function and will wait for the full data to be received from the remote.
static int sock_sync_data(int sockfd, int xfer_size, char *local_data, char *remote_data) {
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

// Poll the CQ for a single event. This function will continue to poll the queue
// until MAX_POLL_TIMEOUT ms have passed.
static int poll_completion(struct resources *res) {
    struct ibv_wc wc;
    unsigned long start_time_ms;
    unsigned long curr_time_ms;
    struct timeval curr_time;
    int poll_result;

    // poll the completion for a while before giving up of doing it
    gettimeofday(&curr_time, NULL);
    start_time_ms = (curr_time.tv_sec * 1000) + (curr_time.tv_usec / 1000);
    do {
        poll_result = ibv_poll_cq(res->cq, 1, &wc);
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
        INFO("Completion was found in CQ with status 0x%x\n", wc.status);
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

// This function will create and post a send work request.
static int post_send(struct resources *res, int len, ibv_wr_opcode opcode, size_t offset = 0) {
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;

    // prepare the scatter / gather entry
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)res->buf;
    sge.length = len+1;
    sge.lkey = res->mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));

    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;

    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = IBV_SEND_SIGNALED;

    if (opcode != IBV_WR_SEND) {
        sr.wr.rdma.remote_addr = res->remote_props.addr + offset;
        sr.wr.rdma.rkey = res->remote_props.rkey;
    }

    // there is a receive request in the responder side, so we won't get any
    // into RNR flow
    CHECK(ibv_post_send(res->qp, &sr, &bad_wr));

    switch (opcode) {
    case IBV_WR_SEND:
        INFO("Send request was posted\n");
        break;
    case IBV_WR_RDMA_READ:
        INFO("RDMA read request was posted\n");
        break;
    case IBV_WR_RDMA_WRITE:
        INFO("RDMA write request was posted\n");
        break;
    default:
        INFO("Unknown request was posted\n");
        break;
    }

    // FIXME: ;)
    return 0;
}

static int post_receive(struct resources *res) {
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;

    // prepare the scatter / gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->buf;
    sge.length = BUFF_SIZE;
    sge.lkey = res->mr->lkey;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));

    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;

    // post the receive request to the RQ
    CHECK(ibv_post_recv(res->qp, &rr, &bad_wr));
    INFO("Receive request was posted\n");

    return 0;
}

// Connect the QP, then transition the server side to RTR, sender side to RTS.
static int connect_qp_tcp(struct config_t& config, RDMARegion& region) {
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    char temp_char;
    union ibv_gid my_gid;

    memset(&my_gid, 0, sizeof(my_gid));

    if (config.gid_idx >= 0) {
        CHECK(ibv_query_gid(region.res.ib_ctx, config.ib_port, config.gid_idx,
                            &my_gid));
    }

    // \begin exchange required info like buffer (addr & rkey) / qp_num / lid,
    // etc. exchange using TCP sockets info required to connect QPs
    local_con_data.addr = htonll((uintptr_t)region.res.buf);
    local_con_data.rkey = htonl(region.res.mr->rkey);
    local_con_data.qp_num = htonl(region.res.qp->qp_num);
    local_con_data.lid = htons(region.res.port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);

    INFO("\n Local LID      = 0x%x\n", region.res.port_attr.lid);

    sock_sync_data(region.res.sock, sizeof(struct cm_con_data_t),
                   (char *)&local_con_data, (char *)&tmp_con_data);

    remote_con_data.addr = ntohll(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

    // save the remote side attributes, we will need it for the post SR
    region.res.remote_props = remote_con_data;
    // \end exchange required info

    INFO("Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);
    INFO("Remote rkey = 0x%x\n", remote_con_data.rkey);
    INFO("Remote QP number = 0x%x\n", remote_con_data.qp_num);
    INFO("Remote LID = 0x%x\n", remote_con_data.lid);

    if (config.gid_idx >= 0) {
        uint8_t *p = remote_con_data.gid;
        int i;
        printf("Remote GID = ");
        for (i = 0; i < 15; i++)
            printf("%02x:", p[i]);
        printf("%02x\n", p[15]);
    }

    // modify the QP to init
    region.modify_qp_to_init(config, region.res.qp);

    // let the client post RR to be prepared for incoming messages
    if (config.server_name) {
        post_receive(&region.res);
    }
    
    // modify the QP to RTR
    region.modify_qp_to_rtr(config, region.res.qp, remote_con_data.qp_num, remote_con_data.lid,
                     remote_con_data.gid);

    // modify QP state to RTS
    region.modify_qp_to_rts(region.res.qp);

    // sync to make sure that both sides are in states that they can connect to
    // prevent packet lose
	char Q = 'Q';
    sock_sync_data(region.res.sock, 1, &Q, &temp_char);

    // FIXME: ;)
    return 0;
}


static void print_config(struct config_t& config) {
    {
        INFO("Device name:          %s\n", config.dev_name);
        INFO("IB port:              %u\n", config.ib_port);
    }
    if (config.server_name) {
        INFO("IP:                   %s\n", config.server_name);
    }
    { INFO("TCP port:             %u\n", config.tcp_port); }
    if (config.gid_idx >= 0) {
        INFO("GID index:            %u\n", config.gid_idx);
    }
}

static void print_usage(const char *progname) {
    printf("Usage:\n");
    printf("%s          start a server and wait for connection\n", progname);
    printf("%s <host>   connect to server at <host>\n\n", progname);
    printf("Options:\n");
    printf("-p, --port <port>           listen on / connect to port <port> "
           "(default 20000)\n");
    printf("-d, --ib-dev <dev>          use IB device <dev> (default first "
           "device found)\n");
    printf("-i, --ib-port <port>        use port <port> of IB device (default "
           "1)\n");
    printf("-g, --gid_idx <gid index>   gid index to be used in GRH (default "
           "not used)\n");
    printf("-h, --help                  this message\n");
}
#endif // UTILS_H