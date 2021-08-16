#include <chrono>
#include <iostream>
#include <stdio.h>
#include <string>
#include <map>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include "common.h"
#include "util.h"
#include "RDMARegion.h"
#include "RDMAHandler.h"
#include "DataProvider.h"

double BtoMB( uint32_t byte ) {
	return static_cast<double>(byte) / 1024 / 1024;
}

int main(int argc, char *argv[]) {
	config_t config = {.dev_name = NULL,
                          .server_name = NULL,
                          .tcp_port = 20000,
                          .ib_port = 1,
                          .gid_idx = -1
                        };

	// \begin parse command line parameters
	while (1) {
		int c;

		static struct option long_options[] = {
			{"port", required_argument, 0, 'p'},
			{"ib-dev", required_argument, 0, 'd'},
			{"ib-port", required_argument, 0, 'i'},
			{"gid-idx", required_argument, 0, 'g'},
			{"help", no_argument, 0, 'h'},
			{NULL, 0, 0, 0}};

		c = getopt_long(argc, argv, "p:d:i:g:h", long_options, NULL);
		if (c == -1)
			break;

		switch (c) {
		case 'p':
			config.tcp_port = strtoul(optarg, NULL, 0);
			break;
		case 'd':
			config.dev_name = strdup(optarg);
			break;
		case 'i':
			config.ib_port = strtoul(optarg, NULL, 0);
			if (config.ib_port < 0) {
				print_usage(argv[0]);
				exit(EXIT_FAILURE);
			}
			break;
		case 'g':
			config.gid_idx = strtoul(optarg, NULL, 0);
			if (config.gid_idx < 0) {
				print_usage(argv[0]);
				exit(EXIT_FAILURE);
			}
			break;
		case 'h':
		default:
			print_usage(argv[0]);
			exit(EXIT_FAILURE);
		}
	}

	// parse the last parameter (if exists) as the server name
	if (optind == argc - 1) {
		config.server_name = argv[optind];
	} else if (optind < argc) {
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	print_config(config);
	RDMAHandler::getInstance().setupCommunicationBuffer( config );
	auto region = RDMAHandler::getInstance().communicationBuffer;
	region->clearCompleteBuffer();

	std::string content;
	std::string op;
	bool abort = false;

	std::map< uint32_t, std::tuple< bool*, uint64_t*, std::thread* > > pool;
	std::vector< std::thread* > regionThreads;
	std::atomic< size_t > global_id = {0};

	std::function<void (RDMARegion*, config_t*, bool*)> check_receive;
	
	check_receive = [&regionThreads,&check_receive]( RDMARegion* communicationRegion, config_t* config, bool* abort ) -> void {
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
					communicationRegion->clearReadCode();
					/* provide data to remote */
					DataProvider d;
					uint64_t elementCount = 1000*1000*4;
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
					while ( remainingSize + 17 > communicationRegion->maxWriteSize() ) {  
						std::cout << "Remaining size: " << remainingSize << std::endl;
						communicationRegion->clearReadCode();
						std::cout << "Setting data to send with " << copy << " " << elementCount << " " << maxDataToWrite << std::endl;
						std::cout << "Data Pointer is accessible at start: " << *copy << std::flush;
						std::cout << " Data Pointer is accessible at end: " << *((uint64_t*) ((char*)copy+maxDataToWrite)) << std::flush;
						
						communicationRegion->setSendData( copy, elementCount, maxDataToWrite );
						std::cout << " Commitinng round [" << iteration << "]..." << std::endl;
						communicationRegion->setCommitCode( rdma_data_receive );

						remainingSize -= maxDataToWrite;
						copy = (uint64_t*) (((char*)copy) + maxDataToWrite);

						// Wait for receiver to consume.
						while ( communicationRegion->receivePtr()[0] != rdma_data_next ) {
							continue; // Busy waiting to ensure fastest possible transfer?
						}
					}
					std::cout << "Sending remainder." << std::endl;
					communicationRegion->setSendData( copy, elementCount, remainingSize );
					communicationRegion->setCommitCode( rdma_data_finished );
					std::cout << "Remainder finished." << std::endl;
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
					uint32_t size;

					size_t i = 0;
					while ( communicationRegion->receivePtr()[0] != rdma_data_finished ) {
						communicationRegion->clearReadCode();
						memcpy( &size, communicationRegion->receivePtr()+5, 4 );
						if (!initDone) {
							initDone = true;
							uint32_t elementCount;
							memcpy( &elementCount, communicationRegion->receivePtr()+1, 4 );
							localData = (uint64_t*) malloc( elementCount * sizeof( uint64_t ) );
							localWritePtr = localData;
							std::cout << "Created memory region for " << (elementCount*sizeof(uint64_t)) << " bytes (" << elementCount << " uint64_t elements)." << std::endl;
						}
						memcpy( localWritePtr, communicationRegion->receivePtr()+9, size );
						localWritePtr = (uint64_t*) ((char*)localWritePtr + size);
						std::cout << "\r[" << i++ << "] Written " << size << " Byte." << std::flush;
						communicationRegion->setCommitCode( rdma_data_next );

						while( communicationRegion->receivePtr()[0] != rdma_data_finished && communicationRegion->receivePtr()[0] != rdma_data_receive ) {
							continue; // Busy waiting to ensure fastest possible transfer?
						}
					}
					std::cout << std::endl;
					memcpy( &size, communicationRegion->receivePtr()+5, 4 );
					memcpy( &localWritePtr, communicationRegion->receivePtr()+9, size );
					communicationRegion->clearCompleteBuffer();
					std::cout << "\r[" << i++ << "] Written " << size << " Byte." << std::flush;

					std::cout << "Finished receiving data. Here's an extract:" << std::endl;
					for ( size_t i = 0; i < 10; ++i ) {
						std::cout << localData[i] << " " << std::flush;
					}
					std::cout << std::endl;
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

	auto check_regions = [&pool,&regionThreads,&check_receive,&config]( bool* abort ) -> void {
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
					delete std::get<2>( it->second ); // thread pointer
					delete std::get<1>( it->second ); // regionid pointer
					it = pool.erase( it );
				}
			}
			std::this_thread::sleep_for( 50ms );
		}
	};

	auto selectRegion = []( bool withDefault ) -> RDMARegion* {
		RDMAHandler::getInstance().printRegions();
		if ( withDefault ) {
			std::cout << "Which region (\"d\" for default buffer)?" << std::endl;
		} else {
			std::cout << "Which region?" << std::endl;
		}
		std::string content;
		std::getline(std::cin, content);
		uint64_t rid;
		if (content != "d") {
			try {
				char* pEnd;
				rid = strtoull(content.c_str(), &pEnd, 10);

			} catch (...) {
				std::cout << "[Error] Couldn't convert number." << std::endl;
				return nullptr;
			}
			return RDMAHandler::getInstance().getRegion( rid );
		} else {
			if ( !withDefault ) {
				return nullptr;
			}
			return RDMAHandler::getInstance().communicationBuffer;
		}
	};

	std::thread readWorker(check_receive, region, &config, &abort);
	std::thread creationWorker(check_regions, &abort);

	std::cout << "Entering Server side event loop." << std::endl;
	while ( !abort ) {
		std::cout << "Choose an opcode:\n[1] Direct write ";
		std::cout << "[2] Commit ";
		std::cout << "[3] Create new region" << std::endl;
		std::cout << "[4] Print Regions ";
		std::cout << "[5] Delete Region ";
		std::cout << "[6] Send dummy to all regions." << std::endl;
		std::cout << "[7] Fetch data from a fixed source. ";
		std::cout << "[8] Exit" << std::endl;
  		std::cin >> op;
		std::cout << "Chosen:" << op << std::endl;
		std::getline(std::cin, content);

		if ( op == "1" ) {
			RDMARegion* sendingRegion = selectRegion( true );
			std::cout << "Content: " << std::flush;
			std::getline(std::cin, content);
			
			if ( sendingRegion ) {
				sendingRegion->setSendData( content );
			} else {
				std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
			}
		} else if ( op == "2" ) {
			RDMARegion* sendingRegion = selectRegion( true );
			if ( sendingRegion ) {
				std::cout << std::endl << "Server side commiting." << std::endl;
				sendingRegion->setCommitCode( rdma_data_ready );
			} else {
				std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
			}
		} else if ( op == "3" ) {
			bool* b = new bool();
			*b = false;
			uint64_t* tid = new uint64_t();
			std::thread* t = new std::thread( &RDMAHandler::create_and_setup_region, &RDMAHandler::getInstance(), &config, tid, b );
			pool.insert( {global_id++, {b,tid,t}} );
		} else if ( op == "4" ) {
			RDMAHandler::getInstance().printRegions();
		} else if ( op == "5" ) {
			RDMARegion* sendingRegion = selectRegion( false );
			if ( sendingRegion ) {
				std::cout << std::endl << "Server side asking to delete region." << std::endl;
				sendingRegion->setCommitCode( rdma_delete_region );
				sendingRegion->receivePtr()[0] = rdma_delete_region;
			} else {
				std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
			}
		} else if ( op == "6" ) {
			auto regs = RDMAHandler::getInstance().getAllRegions();
			std::string dummy = "This is a dummy message.";
			for ( auto r : regs ) {
				r->setSendData( dummy );
				r->setCommitCode( rdma_data_ready );
			}
		} else if ( op == "7" ) {
			RDMARegion* requestRegion = selectRegion( false );
			if ( requestRegion ) {
				requestRegion->setSendData( "Please give data from Random source." );
				requestRegion->setCommitCode( rdma_data_fetch );
			}
		} else if ( op == "8" ) {
			abort = true;
		}
	}
	std::cout << "Joining general workers..." << std::endl;
	readWorker.join();
	creationWorker.join();
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

	return 0;
}
