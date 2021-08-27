#ifndef COMMON_H
#define COMMON_H

enum rdma_handler_communication {
    rdma_shutdown       = -1,
    rdma_no_op          = 0,
    rdma_create_region  = 1,
    rdma_receive_region = 2,
    rdma_delete_region  = 3,
    rdma_data_fetch     = 4,
    rdma_data_receive   = 5,
    rdma_data_next      = 6,
    rdma_data_finished  = 7,
    rdma_data_ready     = 8,
    rdma_tput_test      = 9
};

// #define BUFF_SIZE 1024*1024*2

#endif // COMMON_H