#ifndef MEMORDMA_RDMA_HANDLER_H
#define MEMORDMA_RDMA_HANDLER_H

#include "util.h"
#include "RDMARegion.h"
#include <unordered_map>

enum rdma_handler_communication {
    rdma_create_region = 1 << 0,
    rdma_delete_region = 1 << 1,
    rdma_data_ready    = 1 << 2
};

class RDMAHandler
{
    public:
        static RDMAHandler& getInstance() {
            static RDMAHandler instance;                                            
            return instance;
        }

    private:
        /* Singleton-required */
        RDMAHandler() {
            current_id = 0;
        }; 

        /* Members */        
        uint32_t current_id;
        std::unordered_map< uint64_t, RDMARegion* > regions;

    public:
        RDMAHandler(RDMAHandler const&)  = delete;
        void operator=(RDMAHandler const&)  = delete;

        /* Functions */
        void setupCommunicationBuffer(config_t& config);
        uint32_t create_and_setup_region(config_t& config);
        void connect_qp_rdma(struct config_t& config, RDMARegion& region);
        RDMARegion* getRegion( uint32_t id );

        RDMARegion* communicationBuffer;
};

#endif // MEMORDMA_RDMA_HANDLER_H