#ifndef COMMON_H
#define COMMON_H

enum rdma_handler_communication {
    rdma_create_region  = 1,
    rdma_receive_region = 2,
    rdma_delete_region  = 3,
    rdma_data_fetch     = 4,
    rdma_data_receive   = 5,
    rdma_data_next      = 6,
    rdma_data_finished  = 7,
    rdma_data_ready     = 8
};


#endif COMMON_H