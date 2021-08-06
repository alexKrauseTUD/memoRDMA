#ifndef MEMORDMA_RDMA_HANDLER_H
#define MEMORDMA_RDMA_HANDLER_H

#include "util.h"
#include "RDMARegion.h"
#include <unordered_map>

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
        uint32_t create_and_setup_region(config_t& config);
        RDMARegion* getRegion( uint32_t id );

        // Note: Scott Meyers mentions in his Effective Modern
        //       C++ book, that deleted functions should generally
        //       be public as it results in better error messages
        //       due to the compilers behavior to check accessibility
        //       before deleted status
};

#endif // MEMORDMA_RDMA_HANDLER_H