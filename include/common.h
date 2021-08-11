#ifndef COMMON_H
#define COMMON_H

enum rdma_handler_communication {
    rdma_create_region  = 1,
    rdma_receive_region = 2,
    rdma_delete_region  = 3,
    rdma_fetch_data     = 4,
    rdma_receive_data   = 5,
    rdma_data_ready     = 6
};


#endif COMMON_H