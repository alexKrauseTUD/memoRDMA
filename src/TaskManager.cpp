#include "TaskManager.h"
#include "RDMARegion.h"
#include "RDMAHandler.h"
#include "DataProvider.h"
#include "RDMACommunicator.h"

#include <iostream>
#include <fstream>

TaskManager::TaskManager() :
    globalId{ 1 }
{
    setup();

    registerTask( new Task( "executeMulti", "Execute multiple by ID with shutdown", [&]() -> void {
        std::vector< std::size_t > taskList;
        std::cout << "Space-separated list of tests to run: " << std::endl << "> " << std::flush;
        std::string content = "";
        const char delimiter = ' ';
        std::getline( std::cin, content );
        try {
            size_t last = 0; 
            size_t next = 0; 
            while ( ( next = content.find( delimiter, last ) ) != std::string::npos ) {
                taskList.emplace_back( stol( content.substr( last, next - last ) ) );
                last = next + 1; 
            } 
            taskList.emplace_back( stol( content.substr( last ) ) );
        } catch( ... ) {
            std::cout << "[Error] Invalid number(s) detected, nothing done." << std::endl;
            return;
        }
        
        for ( auto v : taskList ) {
            std::cout << "[Taskmanager] Executing Task [" << v << "]" << std::endl;
            executeById( v );
			
            using namespace std::chrono_literals;
			std::this_thread::sleep_for( 500ms );
        }

        globalAbort();
    }
    ) );

    globalAbort = [] () -> void { std::cout << "[TaskManager] No global Abort function set." << std::endl; };
}

TaskManager::~TaskManager() {
    for ( auto it = tasks.begin(); it != tasks.end(); ++it ) {
        delete it->second;
    }
}

void TaskManager::registerTask( Task* task ) {
    tasks.insert( {globalId++, task } );
}

void TaskManager::printAll() {
    for ( auto it = tasks.begin(); it != tasks.end(); ++it ) {
        std::cout << "[" << it->first << "] " << it->second->name << std::endl;
    }
}

void TaskManager::executeById( std::size_t id ) {
    auto it = tasks.find( id );
    if ( it != tasks.end() ) {
        it->second->run();
    }
}

void TaskManager::executeByIdent( std::string name ) {
    
}

void TaskManager::setup() {
    registerTask( new Task( "directWrite", "Direct Write", [] () -> void {
        std::string content;
        RDMARegion* sendingRegion = RDMAHandler::getInstance().selectRegion( true );
        std::cout << "Content: " << std::flush;
        std::getline(std::cin, content);
        
        if ( sendingRegion ) {
            sendingRegion->setSendData( content );
        } else {
            std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
        }
    }
    ) );

    registerTask( new Task( "commitToRegion", "Commit", [] () -> void {
        RDMARegion* sendingRegion = RDMAHandler::getInstance().selectRegion( true );
        if ( sendingRegion ) {
            std::cout << std::endl << "Server side commiting." << std::endl;
            sendingRegion->setCommitCode( rdma_data_ready );
        } else {
            std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
        }
    }
    ) );
   
    registerTask( new Task( "createRegion", "Create new Region", [] () -> void {
        std::cout << "Buffer size in byte?: " << std::flush;
        std::string sz;
        std::size_t bytes;
        bool success = false;
        while ( !success ) {
            std::getline(std::cin, sz);
            try {
                bytes = stol( sz );
                success = true;
                std::cout << "Creating buffer with " << bytes << " bytes size." << std::endl;
            } catch( ... ) {
                std::cout << "Invalid byte value, conversion failed." << std::endl;
                continue;
            }
        }
        RDMACommunicator::getInstance().setupNewRegion( RDMACommunicator::getInstance().globalConfig, bytes );
    }
    ) );

    registerTask( new Task( "printRegions", "Print Regions", [] () -> void {
        RDMAHandler::getInstance().printRegions();
    }
    ) );
    
    registerTask( new Task( "deleteRegion", "Delete Region", [] () -> void {
        RDMARegion* sendingRegion = RDMAHandler::getInstance().selectRegion( false );
        if ( sendingRegion ) {
            std::cout << std::endl << "Server side asking to delete region." << std::endl;
            sendingRegion->setCommitCode( rdma_delete_region );
            sendingRegion->receivePtr()[0] = rdma_delete_region;
        } else {
            std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
        }
    }
    ) );

    registerTask( new Task( "dummyToAll", "Send Dummy to all Regions", [] () -> void {
        auto regs = RDMAHandler::getInstance().getAllRegions();
        std::string dummy = "This is a dummy message.";
        for ( auto r : regs ) {
            r->setSendData( dummy );
            r->setCommitCode( rdma_data_ready );
        }        
    }
    ) );

    registerTask( new Task( "fetchRandomData", "Fetch data from a fixed source", [] () -> void {
        RDMARegion* requestRegion = RDMAHandler::getInstance().selectRegion( true );
        if ( requestRegion ) {
            requestRegion->setSendData( "Please give data from Random source." );
            requestRegion->setCommitCode( rdma_data_fetch );
        }
    }
    ) );

    registerTask( new Task( "ss_tput", "Single-sided throughput test", [] () -> void {
        for ( std::size_t bytes = 1ull << 10; bytes < 1ull << 32; bytes <<= 1 ) {
            RDMACommunicator::getInstance().setupNewRegion( RDMACommunicator::getInstance().globalConfig, bytes );
            while ( RDMACommunicator::getInstance().pendingRegionCreation() ) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for( 10ms );
            }
            std::size_t regionId = RDMACommunicator::getInstance().lastRegionId() - 1;
            std::cout << "[main] Created region with id " << regionId << " and size " << GetBytesReadable( bytes ) << std::endl;
            auto currentRegion = RDMAHandler::getInstance().getRegion( regionId );
            std::cout << std::endl << "Single-sided throughput test." << std::endl;
            currentRegion->receivePtr()[0] = rdma_tput_test;	
            currentRegion->busy = true;

            while ( currentRegion->busy ) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for( 10ms );
            }

            currentRegion->setCommitCode( rdma_delete_region );
            currentRegion->receivePtr()[0] = rdma_delete_region;
        }
    }
    ) );

    registerTask( new Task( "ds_tput", "Double-sided throughput test", [] () -> void {
        for ( std::size_t bytes = 1ull << 10; bytes < 1ull << 32; bytes <<= 1 ) {
            RDMACommunicator::getInstance().setupNewRegion( RDMACommunicator::getInstance().globalConfig, bytes );
            while ( RDMACommunicator::getInstance().pendingRegionCreation() ) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for( 10ms );
            }
            std::size_t regionId = RDMACommunicator::getInstance().lastRegionId() - 1;
            std::cout << "[main] Created region with id " << regionId << " and size " << GetBytesReadable( bytes ) << std::endl;
            auto currentRegion = RDMAHandler::getInstance().getRegion( regionId );
            std::cout << std::endl << "Double-sided throughput test." << std::endl;
            currentRegion->receivePtr()[0] = rdma_consume_test;	
            currentRegion->busy = true;

            while ( currentRegion->busy ) {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for( 10ms );
            }

            currentRegion->setCommitCode( rdma_delete_region );
            currentRegion->receivePtr()[0] = rdma_delete_region;
        }
    }
    ) );

    registerTask( new Task( "mt_ss_tput", "Multi-threaded single-sided throughput test", [] () -> void {
        std::size_t maxDataElements = 1ull << 32;
        DataProvider* d = new DataProvider();
        std::cout << "[memoRDMA server] Generating dummy data for MT Tput tests..." << std::endl;
        d->generateDummyData( maxDataElements >> 1 );
        std::ofstream out;
        auto in_time_t = std::chrono::system_clock::to_time_t( std::chrono::system_clock::now() );
        std::stringstream logName;
        logName << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "mt_ss_tput.log";
        out.open( logName.str(), std::ios_base::app );

        for ( std::size_t bytes = 1ull << 10; bytes < 1ull << 32; bytes <<= 1 ) {
            std::vector< RDMARegion* > regions{};

            const std::size_t regionCount = 16;
            for ( std::size_t rCnt = 0; rCnt < regionCount; ++rCnt ) {
                RDMACommunicator::getInstance().setupNewRegion( RDMACommunicator::getInstance().globalConfig, bytes );
                while ( RDMACommunicator::getInstance().pendingRegionCreation() ) {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for( 10ms );
                }
                std::size_t regionId = RDMACommunicator::getInstance().lastRegionId() - 1;
                std::cout << "[memoRDMA server] Created region with id " << regionId << " and size " << GetBytesReadable( bytes ) << std::endl;
                
                regions.emplace_back( RDMAHandler::getInstance().getRegion( regionId ) );
            }
            
            std::cout << std::endl << "Single-sided throughput test." << std::endl;

            for ( std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1 ) {
                std::cout << "[memoRDMA server] Processing " << elementCount << " elements..." << std::endl;
                for ( std::size_t iteration = 0; iteration < 10; ++iteration ) {
                    // Copy data provider pointer and current element count to all regions
                    for ( auto r : regions ) {
                        r->clearCompleteBuffer();
                        r->busy = true;
                        memcpy( r->receivePtr()+1, (char*)&elementCount, sizeof(std::size_t) );
                        memcpy( r->receivePtr()+9, (char*)&d, sizeof(DataProvider*) );
                        std::size_t stuff;
                        memcpy( &stuff, r->receivePtr()+9, sizeof(DataProvider*) );
                    }
                    
                    // Tell all regions to start
                    for ( auto r : regions ) {
                        r->receivePtr()[0] = rdma_mt_tput_test;
                    }
                    
                    auto s_ts = std::chrono::high_resolution_clock::now();
                    bool anyBusy = true;
                    while ( anyBusy ) {
                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for( 1ms );
                        anyBusy = false;
                        for ( auto r : regions ) {
                            anyBusy |= r->busy;
                        }
                    }
                    auto e_ts = std::chrono::high_resolution_clock::now();

                    typedef std::chrono::duration<double> d_sec;
                    d_sec secs = e_ts - s_ts;
                    
                    auto transfertime_ns = std::chrono::duration_cast< std::chrono::nanoseconds >( e_ts - s_ts ).count();
                    auto datasize = elementCount * sizeof(uint64_t);
                    std::cout << "Communicated " << datasize << " Bytes (" << BtoMB( datasize ) << " MB) in " << secs.count() << " s times " << regionCount << " regions -- " << BtoMB( datasize * regionCount ) / (secs.count()) << " MB/s " << std::endl;

                    auto readable_size = GetBytesReadable( datasize );
                    std::cout.setf(std::ios::fixed, std::ios::floatfield);
                    std::cout.setf(std::ios::showpoint);
                    out << regionCount << "\t" << bytes << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB( datasize ) / (secs.count()) << std::endl << std::flush;
                }
            }
            
            for ( auto r : regions ) {
                r->setCommitCode( rdma_delete_region );
                r->receivePtr()[0] = rdma_delete_region;
            }
        }
        out.close();
    }
    ) );

    registerTask( new Task( "mt_ds_tput", "Multi-threaded double-sided throughput test", [] () -> void {
        std::size_t maxDataElements = 1ull << 32;
        DataProvider* d = new DataProvider();
        std::cout << "[memoRDMA server] Generating dummy data for MT Consume tests..." << std::endl;
        d->generateDummyData( maxDataElements >> 1 );
        std::ofstream out;
        auto in_time_t = std::chrono::system_clock::to_time_t( std::chrono::system_clock::now() );
        std::stringstream logName;
        logName << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "mt_ds_tput.log";
        out.open( logName.str(), std::ios_base::app );

        for ( std::size_t bytes = 1ull << 10; bytes < 1ull << 32; bytes <<= 1 ) {
            std::vector< RDMARegion* > regions{};

            const std::size_t regionCount = 16;
            for ( std::size_t rCnt = 0; rCnt < regionCount; ++rCnt ) {
                RDMACommunicator::getInstance().setupNewRegion( RDMACommunicator::getInstance().globalConfig, bytes );
                while ( RDMACommunicator::getInstance().pendingRegionCreation() ) {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for( 10ms );
                }
                std::size_t regionId = RDMACommunicator::getInstance().lastRegionId() - 1;
                std::cout << "[memoRDMA server] Created region with id " << regionId << " and size " << GetBytesReadable( bytes ) << std::endl;
                
                regions.emplace_back( RDMAHandler::getInstance().getRegion( regionId ) );
            }
            
            std::cout << std::endl << "Double-sided Consume test." << std::endl;

            for ( std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1 ) {
                std::cout << "[memoRDMA server] Processing " << elementCount << " elements..." << std::endl;
                for ( std::size_t iteration = 0; iteration < 10; ++iteration ) {
                    // Copy data provider pointer and current element count to all regions
                    for ( auto r : regions ) {
                        r->clearCompleteBuffer();
                        r->busy = true;
                        memcpy( r->receivePtr()+1, (char*)&elementCount, sizeof(std::size_t) );
                        memcpy( r->receivePtr()+9, (char*)&d, sizeof(DataProvider*) );
                        std::size_t stuff;
                        memcpy( &stuff, r->receivePtr()+9, sizeof(DataProvider*) );
                    }
                    
                    // Tell all regions to start
                    for ( auto r : regions ) {
                        r->receivePtr()[0] = rdma_mt_consume_test;
                    }
                    
                    auto s_ts = std::chrono::high_resolution_clock::now();
                    bool anyBusy = true;
                    while ( anyBusy ) {
                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for( 1ms );
                        anyBusy = false;
                        for ( auto r : regions ) {
                            anyBusy |= r->busy;
                        }
                    }
                    auto e_ts = std::chrono::high_resolution_clock::now();

                    typedef std::chrono::duration<double> d_sec;
                    d_sec secs = e_ts - s_ts;
                    
                    auto transfertime_ns = std::chrono::duration_cast< std::chrono::nanoseconds >( e_ts - s_ts ).count();
                    auto datasize = elementCount * sizeof(uint64_t);
                    std::cout << "Communicated " << datasize << " Bytes (" << BtoMB( datasize ) << " MB) in " << secs.count() << " s times " << regionCount << " regions -- " << BtoMB( datasize * regionCount ) / (secs.count()) << " MB/s " << std::endl;

                    auto readable_size = GetBytesReadable( datasize );
                    std::cout.setf(std::ios::fixed, std::ios::floatfield);
                    std::cout.setf(std::ios::showpoint);
                    out << regionCount << "\t" << bytes << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB( datasize ) / (secs.count()) << std::endl << std::flush;
                }
            }
            
            for ( auto r : regions ) {
                r->setCommitCode( rdma_delete_region );
                r->receivePtr()[0] = rdma_delete_region;
            }
        }
        out.close();
    }
    ) );
}

void TaskManager::setGlobalAbortFunction( std::function<void()> fn ) {
    globalAbort = fn;
}