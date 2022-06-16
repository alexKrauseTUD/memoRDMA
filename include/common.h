#ifndef COMMON_H
#define COMMON_H

enum rdma_handler_communication {
    rdma_no_op                  =  0,
    rdma_ready                  =  1,
    rdma_sending                =  2,
    rdma_data_finished          =  3,
    rdma_data_receiving         =  4,
    rdma_data_consuming         =  5,
    rdma_consume_test           =  6,
    rdma_mt_consume_test        =  7,
    rdma_test_finished          =  8,
    rdma_blocked                =  9,
    rdma_reconfigure            = 10,
    rdma_multi_thread           = 11,
    rdma_pull_read              = 15,
    rdma_pull_consume           = 16,
    rdma_shutdown               = 100
};

enum data_types {
    type_string      = 1,
    type_int         = 2,
    type_package     = 3
};

enum strategies {
    push =      1,
    pull =      2
};

#endif // COMMON_H