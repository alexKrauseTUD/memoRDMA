#include <chrono>

#include "DataProvider.h"
#include "RDMACommunicator.h"
#include "RDMAHandler.h"
#include "util.h"

RDMACommunicator::RDMACommunicator() :
    bufferSize( 1024*1024*2 ),
    globalAbort( false )
{
	RDMAHandler::getInstance().setupCommunicationBuffer( config );
	auto region = RDMAHandler::getInstance().communicationBuffer;
	region->clearCompleteBuffer();

	check_receive = [this]( RDMARegion* communicationRegion, config_t* config, bool* abort ) -> void {
		using namespace std::chrono_literals;
		std::ios_base::fmtflags f( std::cout.flags() );
		std::cout << "Starting monitoring thread for region " << communicationRegion << std::endl;
		while( !*abort ) {
			switch( communicationRegion->receivePtr()[0] ) {
				case rdma_create_region: {
					RDMARegion* newRegion = new RDMARegion();
					newRegion->resources_create(*config, false);
					RDMAHandler::getInstance().receiveRegionInfo( config, communicationRegion, newRegion );
					RDMAHandler::getInstance().sendRegionInfo( config, communicationRegion, newRegion, rdma_receive_region );
					RDMAHandler::getInstance().registerRegion( newRegion );
					regionThreads.emplace_back( new std::thread(check_receive, newRegion, config, abort) );
					std::cout << "Monitor created new region thread." << std::endl;
					communicationRegion->clearCompleteBuffer();
				}; break;
				case rdma_delete_region: {
					RDMAHandler::getInstance().removeRegion( communicationRegion );
					std::cout << "Monitoring thread for Region " << communicationRegion << " stopping." << std::endl;
					return;
				}; break;
				case rdma_data_ready: {
					std::cout << "Received data [" << communicationRegion << "]: " << communicationRegion->receivePtr()+1 << std::endl;
					communicationRegion->clearCompleteBuffer();
				}; break;
				case rdma_data_fetch: {
					communicationRegion->clearCompleteBuffer();
					/* provide data to remote */
					DataProvider d;
					uint64_t elementCount = 1000*1000;
					uint64_t remainingSize = elementCount * sizeof(uint64_t);
					uint64_t maxPaylodSize = communicationRegion->maxWriteSize() - 1 - sizeof(elementCount) - sizeof(remainingSize);
					uint64_t maxDataToWrite = (maxPaylodSize/sizeof(uint64_t)) * sizeof(uint64_t);
					std::cout << "Max Payload is: " << maxPaylodSize << " but we use " << maxDataToWrite << std::endl;
					std::cout << "Generating " << remainingSize << " Byte of data and send them over." << std::endl;
					d.generateDummyData( elementCount );
					uint64_t* copy = d.data;
					
					std::cout << "Starting to loop. " << std::endl;
					size_t iteration = 1;
					// Add 17 Byte to the size - 1 Byte commit code, 8 Byte elementCount, 8 byte maxDataToWrite.
					auto s_ts = std::chrono::high_resolution_clock::now();
					while ( remainingSize + 17 > communicationRegion->maxWriteSize() ) {  
						communicationRegion->clearCompleteBuffer();
						communicationRegion->setSendData( copy, elementCount, maxDataToWrite );
						communicationRegion->setCommitCode( rdma_data_receive );

						remainingSize -= maxDataToWrite;
						copy = (uint64_t*) (((char*)copy) + maxDataToWrite);

						// Wait for receiver to consume.
						while ( communicationRegion->receivePtr()[0] != rdma_data_next ) {
							continue; // Busy waiting to ensure fastest possible transfer?
						}
					}
					communicationRegion->setSendData( copy, elementCount, remainingSize );
					communicationRegion->setCommitCode( rdma_data_finished );
					auto e_ts = std::chrono::high_resolution_clock::now();
					auto transfertime_ms = std::chrono::duration_cast< std::chrono::milliseconds >( e_ts - s_ts ).count();
					auto datasize = elementCount * sizeof(uint64_t);
					std::cout << "Communicated " << datasize << " Bytes (" << BtoMB( datasize ) << " MB) in " << transfertime_ms << " ms -- " << BtoMB( datasize ) / (transfertime_ms / 1000) << " MB/s " << std::endl;
					communicationRegion->clearCompleteBuffer();
				} break;
				case rdma_data_finished: {
					uint64_t* localData;
					uint64_t elementCount;
					uint64_t size;
					
					memcpy( &elementCount, communicationRegion->receivePtr()+1, 8 );
					memcpy( &size, communicationRegion->receivePtr()+9, 8 );
					
					localData = (uint64_t*) malloc( elementCount * sizeof( uint64_t ) );
					std::cout << "Created memory region for " << (elementCount*sizeof(uint64_t)) << " bytes (" << elementCount << " uint64_t elements)." << std::endl;
					
					memcpy( localData, communicationRegion->receivePtr()+17, size );
					std::cout << "Finished receiving data. Here's an extract:" << std::endl;
					for ( size_t i = 0; i < 10; ++i ) {
						std::cout << localData[i] << " " << std::flush;
					}
					std::cout << std::endl;
					free( localData );
					communicationRegion->clearCompleteBuffer();
				}; break;
				case rdma_data_receive: {
					/* receive data from remote */
					bool initDone = false;
					uint64_t* localData;
					uint64_t* localWritePtr;
					uint64_t size;
					uint64_t elementCount;

					size_t i = 0;
					while ( communicationRegion->receivePtr()[0] != rdma_data_finished ) {
						communicationRegion->clearReadCode();
						memcpy( &size, communicationRegion->receivePtr()+9, 8 );
						if (!initDone) {
							initDone = true;
							memcpy( &elementCount, communicationRegion->receivePtr()+1, 8 );
							localData = (uint64_t*) malloc( elementCount * sizeof( uint64_t ) );
							localWritePtr = localData;
							std::cout << "Created memory region for " << (elementCount*sizeof(uint64_t)) << " bytes (" << elementCount << " uint64_t elements)." << std::endl;
						}
						// std::cout << "\r[" << i++ << "] Writing " << size << " Byte." << std::endl;
						memcpy( localWritePtr, communicationRegion->receivePtr()+17, size );
						localWritePtr = (uint64_t*) ((char*)localWritePtr + size);
						communicationRegion->setCommitCode( rdma_data_next );

						while( communicationRegion->receivePtr()[0] != rdma_data_finished && communicationRegion->receivePtr()[0] != rdma_data_receive ) {
							continue; // Busy waiting to ensure fastest possible transfer?
						}
					}
					memcpy( &size, communicationRegion->receivePtr()+9, 8 );
					memcpy( localWritePtr, communicationRegion->receivePtr()+17, size );
					communicationRegion->clearCompleteBuffer();

					DataProvider d;
					d.generateDummyData( elementCount );
					auto ret = memcmp( localData, d.data, elementCount * sizeof(uint64_t) );
					std::cout << "Ret is: " << ret << std::endl;

					free( localData );
				} break;
				default: {
					continue;
				}; break;
			}
			std::cout.flags( f );
			std::this_thread::sleep_for( 100ms );
		}
	};

    check_regions = [this]( bool* abort ) -> void {
		using namespace std::chrono_literals;
		while (!*abort) {
			for ( auto it = pool.begin(); it != pool.end(); ) {
				if ( *std::get<0>(it->second) ) {
					/* Memory region created, old thread can be let go */
					std::cout << "Joining thread " << it->first << std::endl;
					std::get<2>(it->second)->join();
					/* Spawning new thread to listen to it */
					auto newRegion = RDMAHandler::getInstance().getRegion( *std::get<1>(it->second) );
					regionThreads.emplace_back( new std::thread(check_receive, newRegion, &config, abort) );
					std::cout << "Spawned new listener thread for region: " << *std::get<1>(it->second) << std::endl;
					/* Cleanup & remove element from global map */
					free( std::get<2>( it->second ) ); // thread pointer
					free( std::get<1>( it->second ) ); // regionid pointer
					it = pool.erase( it );
				}
			}
			std::this_thread::sleep_for( 50ms );
		}
	};

    readWorker = new std::thread( check_receive, region, &config, &globalAbort );
	creationWorker = new std::thread( check_regions, &globalAbort );
}

RDMACommunicator::~RDMACommunicator() {
	std::cout << "Joining general workers..." << std::endl;
	readWorker->join();
	creationWorker->join();
	std::cout << "Joining region workers..." << std::endl;
	for ( auto t : regionThreads ) {
		t->join();
		delete t;
	}
	std::cout << "Cleaning pool..." << std::endl;
	for ( auto it = pool.begin(); it != pool.end(); ) {
		for ( auto it = pool.begin(); it != pool.end(); ) {
			if ( *std::get<0>(it->second) ) {
				std::get<2>(it->second)->join();
				delete std::get<2>( it->second );
				delete std::get<1>( it->second );
				it = pool.erase( it );
			}
		}		
	}
}

void RDMACommunicator::stop() {
	globalAbort = true;
}

void RDMACommunicator::setupNewRegion() {
	bool* b = new bool();
	*b = false;
	uint64_t* tid = new uint64_t();
	std::thread* t = new std::thread( &RDMAHandler::create_and_setup_region, &RDMAHandler::getInstance(), &config, tid, b );
	pool.insert( {global_id++, {b,tid,t}} );
}