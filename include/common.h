#ifndef COMMON_H
#define COMMON_H

// Should be:
// enum class Opcodes: uint8_t {
enum rdma_handler_communication: uint8_t {
    rdma_no_op,
    rdma_ready,
    rdma_working,
    rdma_data_finished,
    rdma_blocked,
    rdma_reconfigure,
    rdma_reconfigure_ack,
    rdma_reconfiguring,
    rdma_ready_to_send,
    rdma_functional_test,
    rdma_functional_test_ack,
    rdma_continuous_test,
    rdma_continuous_test_ack,
    rdma_shutdown
};

enum class ConnectionType: uint8_t {
    PushConnection,
    PullConnection
};

#endif // COMMON_H