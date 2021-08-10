#ifndef MEMORDMA_RDMA_REGION_H
#define MEMORDMA_RDMA_REGION_H

#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "util.h"

// structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint64_t addr;   // buffer address
    uint32_t rkey;   // remote key
    uint32_t qp_num; // QP number
    uint16_t lid;    // LID of the IB port
    uint8_t gid[16]; // GID
} __attribute__((packed));

// structure of system resources
struct resources {
    struct ibv_device_attr device_attr; // device attributes
    struct ibv_port_attr port_attr;     // IB port attributes
    struct cm_con_data_t remote_props;  // values to connect to remote side
    struct ibv_context *ib_ctx;         // device handle
    struct ibv_pd *pd;                  // PD handle
    struct ibv_cq *cq;                  // CQ handle
    struct ibv_qp *qp;                  // QP handle
    struct ibv_mr *mr;                  // MR handle for buf
    char *buf;                          // memory buffer pointer, used for RDMA send ops
    int sock;                           // TCP socket file descriptor
};

class RDMARegion {
    public:
        RDMARegion();
        int resources_create(struct config_t& config, bool initTCP = true );
        void resources_sync_local( config_t* config, struct cm_con_data_t& tmp_con_data );
        int modify_qp_to_init(struct config_t& config, struct ibv_qp *qp);
        int modify_qp_to_rtr(struct config_t& config, struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
        int modify_qp_to_rts(struct ibv_qp *qp);
        int resources_destroy();

        char* writePtr();
        char* receivePtr();
        uint64_t inline maxWriteSize() const;

        void print() const;

        void clearBuffer();

        struct resources res;
};

#endif // MEMORDMA_RDMA_REGION_H