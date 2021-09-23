#ifndef RDMA_COMMUNCICATOR_H
#define RDMA_COMMUNCICATOR_H

#include <atomic>
#include <functional>
#include <map>
#include <stdint.h>
#include <stdio.h>
#include <thread>
#include <mutex>
#include <vector>

#include "common.h"
#include "RDMARegion.h"

class RDMACommunicator {
    public:
        static RDMACommunicator& getInstance() {
            static RDMACommunicator instance;                                            
            return instance;
        }
        ~RDMACommunicator();

        RDMACommunicator(RDMACommunicator const&)  = delete;
        void operator=(RDMACommunicator const&)  = delete;

        void init( config_t& config );

        bool pendingRegionCreation();
        void setupNewRegion( config_t& config, std::size_t bytes );
        std::size_t lastRegionId() const;

        void stop();

    private:
        /* Singleton-required */
        RDMACommunicator(); 

        void createRdmaRegion( config_t* config, RDMARegion* communicationRegion );
        void deleteRdmaRegion( RDMARegion* communicationRegion );
        void readCommittedData( RDMARegion* communicationRegion );
        void sendDataToRemote( RDMARegion* communicationRegion );
        void receiveDataFromRemote( RDMARegion* communicationRegion, bool soloPackage );
        void throughputTest( RDMARegion* communicationRegion );
        void consumingTest( RDMARegion* communicationRegion );
        
        std::map< uint32_t, std::tuple< bool*, uint64_t*, std::thread* > > pool;
	    std::vector< std::thread* > regionThreads;
	    std::atomic< size_t > global_id = {0};
	    std::function< void (RDMARegion*, config_t*, bool*) > check_receive;
        std::function< void (bool*) > check_regions;

        std::thread* readWorker;
        std::thread* creationWorker;
        std::mutex poolMutex;

        config_t config;
        bool globalAbort;
};

#endif // RDMA_COMMUNCICATOR_H