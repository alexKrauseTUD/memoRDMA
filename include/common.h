#ifndef COMMON_H
#define COMMON_H

enum rdma_handler_communication {
    rdma_no_op                  =  0,
    rdma_ready                  =  1,
    rdma_create_region          =  2,
    rdma_receive_region         =  3,
    rdma_delete_region          =  4,
    rdma_data_fetch             =  5,
    rdma_data_receive           =  6,
    rdma_data_consuming         =  7,
    rdma_data_finished          =  8,
    rdma_data_ready             =  9,
    rdma_sending                = 10,
    rdma_tput_test              = 11,
    rdma_consume_test           = 12,
    rdma_next_test              = 13,
    rdma_mt_tput_test           = 14,
    rdma_mt_consume_test        = 15,
    rdma_next_mt_consume_test   = 16,
    rdma_shutdown               = 17
};

enum data_types {
    type_string      = 1,
    type_int         = 2,
    type_package     = 3
};

#define META_INFORMATION_SIZE 40

// #define BUFF_SIZE 1024*1024*2

#endif // COMMON_H