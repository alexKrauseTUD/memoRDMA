#ifndef COMMON_H
#define COMMON_H

enum rdma_handler_communication: uint8_t {
    rdma_no_op,
    rdma_ready,
    rdma_working,
    rdma_data_finished,
    rdma_data_receiving,
    rdma_data_consuming,
    rdma_consume_test,
    rdma_mt_consume_test,
    rdma_test_finished,
    rdma_blocked,
    rdma_reconfigure,
    rdma_reconfiguring,
    rdma_multi_thread,
    rdma_pull_read,
    rdma_pull_consume,
    rdma_give_column,
    rdma_ready_to_push,
    rdma_ready_to_pull,
    rdma_functional_test,
    rdma_functional_test_ack,
    rdma_shutdown
};

enum class Strategies {
    push,
    pull
};

#endif // COMMON_H