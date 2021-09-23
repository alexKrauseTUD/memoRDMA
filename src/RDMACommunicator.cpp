#include <chrono>
#include <fstream>

#include "DataProvider.h"
#include "RDMACommunicator.h"
#include "RDMAHandler.h"
#include "util.h"
#include "package_manager.hpp"

RDMACommunicator::RDMACommunicator() :
    globalAbort( false )
{
	check_receive = [this]( RDMARegion* communicationRegion, config_t* config, bool* abort ) -> void {
		using namespace std::chrono_literals;
		std::ios_base::fmtflags f( std::cout.flags() );
		std::cout << "Starting monitoring thread for region " << communicationRegion << std::endl;
		while( !*abort ) {
			std::this_thread::sleep_for( 10ms );
			switch( communicationRegion->receivePtr()[0] ) {
				case rdma_create_region: {
					createRdmaRegion( config, communicationRegion );
				}; break;
				case rdma_delete_region: {
					deleteRdmaRegion( communicationRegion );
					return;
				}; break;
				case rdma_data_ready: {
					readCommittedData( communicationRegion );
				}; break;
				case rdma_data_fetch: {
					sendDataToRemote( communicationRegion );
				} break;
				case rdma_data_finished: {
					receiveDataFromRemote( communicationRegion, true );
				}; break;
				case rdma_data_receive: {
					receiveDataFromRemote( communicationRegion, false );
				} break;
				case rdma_tput_test: {
					throughputTest( communicationRegion );
				} break;
				case rdma_consume_test: {
					consumingTest( communicationRegion );
				} break;
				default: {
					continue;
				}; break;
			}
			std::cout.flags( f );
		}
	};

    check_regions = [this]( bool* abort ) -> void {
		using namespace std::chrono_literals;
		while (!*abort) {
			for ( auto it = pool.begin(); it != pool.end(); ) {
				if ( *std::get<0>(it->second) ) {
					const std::lock_guard< std::mutex > lock( poolMutex );
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
}

void RDMACommunicator::init( config_t& config ) {
	RDMAHandler::getInstance().setupCommunicationBuffer( config );
	auto region = RDMAHandler::getInstance().communicationBuffer;
	region->clearCompleteBuffer();
    readWorker = new std::thread( check_receive, region, &config, &globalAbort );
	creationWorker = new std::thread( check_regions, &globalAbort );
}

RDMACommunicator::~RDMACommunicator() {
	stop();
	std::cout << "Joining general workers..." << std::endl;
	if ( readWorker ) {
		readWorker->join();
	}
	if ( creationWorker ) {
		creationWorker->join();
	}
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

void RDMACommunicator::setupNewRegion( config_t& config, std::size_t bytes ) {
	bool* b = new bool();
	*b = false;
	uint64_t* tid = new uint64_t();
	std::cout << "[RDMACommunicator] Dispatching new thread: Create new buffer with " << bytes << " bytes" << std::endl;
	std::thread* t = new std::thread( &RDMAHandler::create_and_setup_region, &RDMAHandler::getInstance(), &config, bytes, tid, b );
	pool.insert( {global_id++, {b,tid,t}} );
}

void RDMACommunicator::createRdmaRegion( config_t* config, RDMARegion* communicationRegion ) {
	std::size_t remoteBufferSize = RDMAHandler::getInstance().receiveRegionSize( communicationRegion );
	std::cout << "[RDMACommunicator] Receiving region size: " << remoteBufferSize << std::endl;
	RDMARegion* newRegion = new RDMARegion( remoteBufferSize );
	newRegion->resources_create(*config, false);
	RDMAHandler::getInstance().receiveRegionInfo( config, communicationRegion, newRegion );
	RDMAHandler::getInstance().sendRegionInfo( config, remoteBufferSize, communicationRegion, newRegion, rdma_receive_region );
	RDMAHandler::getInstance().registerRegion( newRegion );
	regionThreads.emplace_back( new std::thread(check_receive, newRegion, config, &globalAbort) );
	std::cout << "Monitor created new region thread." << std::endl;
	communicationRegion->clearCompleteBuffer();
}

void RDMACommunicator::deleteRdmaRegion( RDMARegion* communicationRegion ) {
	RDMAHandler::getInstance().removeRegion( communicationRegion );
	std::cout << "Monitoring thread for Region " << communicationRegion << " stopping." << std::endl;
}

void RDMACommunicator::readCommittedData( RDMARegion* communicationRegion ) {
	std::cout << "Received data [" << communicationRegion << "]: " << communicationRegion->receivePtr()+1 << std::endl;
	communicationRegion->clearCompleteBuffer();
}

void RDMACommunicator::sendDataToRemote( RDMARegion* communicationRegion ) {
	communicationRegion->clearCompleteBuffer();
	/* provide data to remote */
	DataProvider d;
	uint64_t elementCount = 1000*1000*4;
	uint64_t remainingSize = elementCount * sizeof(uint64_t);
	uint64_t maxPayloadSize = communicationRegion->maxWriteSize() - 1 - package_t::metaDataSize();
	uint64_t maxDataToWrite = (maxPayloadSize/sizeof(uint64_t)) * sizeof(uint64_t);
	std::cout << "Max Payload is: " << maxPayloadSize << " but we use " << maxDataToWrite << std::endl;
	std::cout << "Generating " << remainingSize << " Byte of data and send them over." << std::endl;
	d.generateDummyData( elementCount );
	uint64_t* copy = d.data;

	package_t package( remainingSize, maxDataToWrite, copy );

	std::cout << "Starting to loop. " << std::endl;
	auto s_ts = std::chrono::high_resolution_clock::now();
	communicationRegion->setPackageHeader( &package );
	while ( remainingSize + package.metaDataSize() > communicationRegion->maxWriteSize() ) {  
		communicationRegion->clearReadCode();
		communicationRegion->sendPackage( &package, rdma_data_receive );

		remainingSize -= maxDataToWrite;
		package.advancePayloadPtr( maxDataToWrite );

		// Wait for receiver to consume.
		while ( communicationRegion->currentReadCode() != rdma_data_next ) {
			using namespace std::chrono_literals;
			std::this_thread::sleep_for( 100ns );
			continue; // Busy waiting to ensure fastest possible transfer?
		}
	}
	package.setCurrentPackageSize( remainingSize );
	communicationRegion->setPackageHeader( &package );
	communicationRegion->sendPackage( &package, rdma_data_finished );
	auto e_ts = std::chrono::high_resolution_clock::now();
	auto transfertime_ms = std::chrono::duration_cast< std::chrono::milliseconds >( e_ts - s_ts ).count();
	auto datasize = elementCount * sizeof(uint64_t);
	std::cout << "Communicated " << datasize << " Bytes (" << BtoMB( datasize ) << " MB) in " << transfertime_ms << " ms -- " << BtoMB( datasize ) / (((double)transfertime_ms) / 1000) << " MB/s " << std::endl;
	communicationRegion->clearCompleteBuffer();
}

void RDMACommunicator::receiveDataFromRemote( RDMARegion* communicationRegion, bool soloPackage ) {
	bool initDone = false;
	uint64_t* localData;
	uint64_t* localWritePtr;

	package_t::header_t* package_head = (package_t::header_t*)(communicationRegion->receivePtr()+1);
	if ( !soloPackage ) {
		while ( communicationRegion->currentReadCode() != rdma_data_finished ) {
			communicationRegion->clearReadCode();
			// memcpy( &package_head, communicationRegion->receivePtr()+1, sizeof(package_t::header_t) );
			if (!initDone) {
				initDone = true;
				localData = (uint64_t*) malloc( package_head->total_data_size );
				localWritePtr = localData;
				std::cout << "[RDMACommunicator] MultiPackage: Created memory region for " << package_head->total_data_size << " bytes (" << (package_head->total_data_size / sizeof(uint64_t)) << " uint64_t elements)." << std::endl;
			}
			memcpy( localWritePtr, communicationRegion->receivePtr()+1+package_t::metaDataSize(), package_head->current_payload_size ); // +1 for commit code
			localWritePtr = (uint64_t*) ((char*)localWritePtr + package_head->current_payload_size);
			communicationRegion->setCommitCode( rdma_data_next );

			while( communicationRegion->currentReadCode() != rdma_data_finished && communicationRegion->currentReadCode() != rdma_data_receive ) {
				continue; // Busy waiting to ensure fastest possible transfer?
			}
		}
		/* Handle remainder of rdma_data_finished package */
		memcpy( localWritePtr, communicationRegion->receivePtr()+1+package_t::metaDataSize(), package_head->current_payload_size );
	} else {
		localData = (uint64_t*) malloc( package_head->total_data_size );
		std::cout << "[RDMACommunicator] SoloPackage: Created memory region for " << package_head->total_data_size << " bytes (" << (package_head->total_data_size/sizeof(uint64_t)) << " uint64_t elements)." << std::endl;
		memcpy( localData, communicationRegion->receivePtr()+1+package_t::metaDataSize(), package_head->current_payload_size );
		communicationRegion->clearCompleteBuffer();
	}

	std::cout << "[Sanity] memcmp to check for sequential data correctness. Ret should be 0." << std::endl;
	DataProvider d;
	d.generateDummyData( package_head->total_data_size/sizeof(uint64_t) );
	auto ret = memcmp( localData, d.data, package_head->total_data_size );
	std::cout << "Ret is: " << ret << std::endl;
	communicationRegion->clearCompleteBuffer();
	communicationRegion->setCommitCode( rdma_next_test );
}

void RDMACommunicator::throughputTest( RDMARegion* communicationRegion ) {
	communicationRegion->clearCompleteBuffer();
	/* provide data to remote */
	std::size_t maxDataElements = 1ull << 32;
	DataProvider d;
	d.generateDummyData( maxDataElements >> 1 );
	std::ofstream out;
	auto in_time_t = std::chrono::system_clock::to_time_t( std::chrono::system_clock::now() );
	std::stringstream logName;
	logName << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "tput_log.log";
	out.open( logName.str(), std::ios_base::app );

	std::ios_base::fmtflags f( std::cout.flags() );
	for ( std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1 ) {
		for ( std::size_t iteration = 0; iteration < 10; ++iteration ) {
			uint64_t remainingSize = elementCount * sizeof(uint64_t);
			uint64_t maxPayloadSize = communicationRegion->maxWriteSize() - 1 - package_t::metaDataSize();
			uint64_t maxDataToWrite = (maxPayloadSize/sizeof(uint64_t)) * sizeof(uint64_t);
			std::cout << "Max Payload is: " << maxPayloadSize << " but we use " << maxDataToWrite << std::endl;
			std::cout << "Generating " << remainingSize << " Byte of data and send them over." << std::endl;
			uint64_t* copy = d.data;

			package_t package( remainingSize, maxDataToWrite, copy );
			auto s_ts = std::chrono::high_resolution_clock::now();
			communicationRegion->setPackageHeader( &package );
			while ( remainingSize + package.metaDataSize() > communicationRegion->maxWriteSize() ) {  
				communicationRegion->clearReadCode();
				communicationRegion->sendPackage( &package, rdma_no_op );

				remainingSize -= maxDataToWrite;
				package.advancePayloadPtr( maxDataToWrite );
				// No waiting, just plain copying and see how fast we can go.
			}
			package.setCurrentPackageSize( remainingSize );
			communicationRegion->setPackageHeader( &package );
			communicationRegion->sendPackage( &package, rdma_no_op );
			auto e_ts = std::chrono::high_resolution_clock::now();
			auto transfertime_ns = std::chrono::duration_cast< std::chrono::nanoseconds >( e_ts - s_ts ).count();
			auto datasize = elementCount * sizeof(uint64_t);
			
			typedef std::chrono::duration<double> d_sec;
			d_sec secs = e_ts - s_ts;

			std::cout << "Communicated " << datasize << " Bytes (" << BtoMB( datasize ) << " MB) in " << secs.count() << " s -- " << BtoMB( datasize ) / (secs.count()) << " MB/s " << std::endl;

			auto readable_size = GetBytesReadable( datasize );

			std::cout.setf(std::ios::fixed, std::ios::floatfield);
			std::cout.setf(std::ios::showpoint);
			out << communicationRegion->bufferSize << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB( datasize ) / (secs.count()) << std::endl << std::flush;
			std::cout.flags( f );
		}
	}
	std::cout << "[ThroughputTest] Finished." << std::endl;
	out.close();
	communicationRegion->busy = false;
}

void RDMACommunicator::consumingTest( RDMARegion* communicationRegion ) {
	communicationRegion->clearCompleteBuffer();
	/* provide data to remote */
	std::size_t maxDataElements = 1ull << 32;
	DataProvider d;
	d.generateDummyData( maxDataElements >> 1 );
	std::ofstream out;
	auto in_time_t = std::chrono::system_clock::to_time_t( std::chrono::system_clock::now() );
	std::stringstream logName;
	logName << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "consume_log.log";
	out.open( logName.str(), std::ios_base::app );

	std::ios_base::fmtflags f( std::cout.flags() );
	for ( std::size_t elementCount = 1; elementCount < maxDataElements; elementCount <<= 1 ) {
		for ( std::size_t iteration = 0; iteration < 10; ++iteration ) {
			uint64_t remainingSize = elementCount * sizeof(uint64_t);
			uint64_t maxPayloadSize = communicationRegion->maxWriteSize() - 1 - package_t::metaDataSize();
			uint64_t maxDataToWrite = (maxPayloadSize/sizeof(uint64_t)) * sizeof(uint64_t);
			std::cout << "[ConsumeTest] Max Payload is: " << maxPayloadSize << " but we use " << maxDataToWrite << std::endl;
			std::cout << "[ConsumeTest] Generating " << remainingSize << " Byte of data and send them over." << std::endl;
			uint64_t* copy = d.data;

			package_t package( remainingSize, maxDataToWrite, copy );
			auto s_ts = std::chrono::high_resolution_clock::now();
			communicationRegion->setPackageHeader( &package );
			while ( remainingSize + package.metaDataSize() > communicationRegion->maxWriteSize() ) {  
				communicationRegion->clearReadCode();
				communicationRegion->sendPackage( &package, rdma_data_receive );

				remainingSize -= maxDataToWrite;
				package.advancePayloadPtr( maxDataToWrite );
				// Wait for receiver to consume.
				while ( communicationRegion->currentReadCode() != rdma_data_next ) {
					// using namespace std::chrono_literals;
					// std::this_thread::sleep_for( 100ns );
					continue; // Busy waiting to ensure fastest possible transfer?
				}
			}
			package.setCurrentPackageSize( remainingSize );
			communicationRegion->setPackageHeader( &package );
			communicationRegion->sendPackage( &package, rdma_data_finished );
			auto e_ts = std::chrono::high_resolution_clock::now();
			auto transfertime_ns = std::chrono::duration_cast< std::chrono::nanoseconds >( e_ts - s_ts ).count();
			auto datasize = elementCount * sizeof(uint64_t);
			
			typedef std::chrono::duration<double> d_sec;
			d_sec secs = e_ts - s_ts;

			std::cout << "[ConsumeTest] Communicated " << datasize << " Bytes (" << BtoMB( datasize ) << " MB) in " << secs.count() << " s -- " << BtoMB( datasize ) / (secs.count()) << " MB/s " << std::endl;

			auto readable_size = GetBytesReadable( datasize );

			std::cout.setf(std::ios::fixed, std::ios::floatfield);
			std::cout.setf(std::ios::showpoint);
			out << communicationRegion->bufferSize << "\t" << elementCount << "\t" << datasize << "\t" << transfertime_ns << "\t" << BtoMB( datasize ) / (secs.count()) << std::endl << std::flush;
			std::cout.flags( f );
			std::cout << "[ConsumeTest] Waiting for remote sanity check to finish." << std::endl;
			while ( communicationRegion->currentReadCode() != rdma_next_test ) {
				using namespace std::chrono_literals;
				std::this_thread::sleep_for( 100ns );
				continue; // Busy waiting to ensure fastest possible transfer?
			}
			communicationRegion->clearCompleteBuffer();
		}
	}
	std::cout << "[ThroughputTest] Finished." << std::endl;
	out.close();
	communicationRegion->busy = false;
}

std::size_t RDMACommunicator::lastRegionId() const {
	return global_id;
}

bool RDMACommunicator::pendingRegionCreation() {
	const std::lock_guard< std::mutex > lock( poolMutex );
	return !pool.empty();
}