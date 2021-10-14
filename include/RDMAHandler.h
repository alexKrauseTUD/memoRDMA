#ifndef MEMORDMA_RDMA_HANDLER_H
#define MEMORDMA_RDMA_HANDLER_H

#include "common.h"
#include "util.h"
#include "RDMARegion.h"
#include <unordered_map>
#include <vector>
#include <mutex>

class RDMAHandler {
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
        std::mutex regionMutex;
        std::unordered_map< uint64_t, RDMARegion* > regions;

    public:
        RDMAHandler(RDMAHandler const&)  = delete;
        void operator=(RDMAHandler const&)  = delete;

        /* Functions */
        void setupCommunicationBuffer( config_t& config );
        void sendRegionInfo( config_t* config, std::size_t bufferSize, RDMARegion* communicationRegion, RDMARegion* newRegion, rdma_handler_communication opcode );
        void receiveRegionInfo( config_t* config, RDMARegion* communicationRegion, RDMARegion* newRegion );
        std::size_t receiveRegionSize( RDMARegion* communicationRegion ) const;
        
        void connectQpTCP( struct config_t& config, RDMARegion& region );
        void create_and_setup_region( config_t* config, std::size_t bufferSize, uint64_t* newRegionId = nullptr, bool* isReady = nullptr );

        uint64_t registerRegion( RDMARegion* region );
        void removeRegion( RDMARegion* region );
        
        RDMARegion* selectRegion( bool withDefault );

        void printRegions() const;

        std::vector< RDMARegion* > getAllRegions() const;
        RDMARegion* getRegion( uint64_t id ) const;

        RDMARegion* communicationBuffer;
};

#endif // MEMORDMA_RDMA_HANDLER_H