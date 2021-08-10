#include <chrono>
#include <iostream>
#include <stdio.h>
#include <string>
#include "util.h"
#include "RDMARegion.h"
#include "RDMAHandler.h"
#include <map>
#include <thread>
#include <vector>
#include <atomic>

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

	auto check_receive = []( RDMARegion* communicationRegion, config_t* config, bool* abort ) {
		using namespace std::chrono_literals;
		std::ios_base::fmtflags f( std::cout.flags() );
		std::cout << "Starting Client-side monitoring thread" << std::endl;
		while( !*abort ) {
			switch( communicationRegion->receivePtr()[0] ) {
				case rdma_create_region: {
					RDMARegion* newRegion = new RDMARegion();
					newRegion->resources_create(*config, false);
					RDMAHandler::getInstance().receiveRegionInfo( config, communicationRegion, newRegion );
					RDMAHandler::getInstance().sendRegionInfo( config, communicationRegion, newRegion, rdma_receive_region );
					RDMAHandler::getInstance().registerRegion( newRegion );
					communicationRegion->clearCompleteBuffer();
				}; break;
				case rdma_delete_region: {}; break;
				case rdma_data_ready: {
					std::cout << "Received data: " << communicationRegion->receivePtr()+1 << std::endl;
					communicationRegion->clearCompleteBuffer();
				}; break;
				default: {
					continue;
				}; break;
			}
			std::cout.flags( f );
			std::this_thread::sleep_for( 100ms );
		}
	};

	auto check_regions = [&pool,&regionThreads,&global_id,&check_receive,&config]( bool* abort ) {
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
					/* Remove element from global map */
					it = pool.erase( it );
				}
			}
			std::this_thread::sleep_for( 1ms );
		}
	};

	std::thread readWorker(check_receive, region, &config, &abort);
	std::thread creationWorker(check_regions, &abort);

	std::cout << "Entering Server side event loop." << std::endl;
	while ( !abort ) {
		std::cout << "Choose an opcode: [1] Direct write ";
		std::cout << "[2] Commit ";
		std::cout << "[3] Create new region ";
		std::cout << "[4] Print Regions ";
		std::cout << "[5] Exit" << std::endl;
  		std::cin >> op;
		std::cout << "Chosen:" << op << std::endl;
		std::getline(std::cin, content);

		if ( op == "1" ) {
			std::getline(std::cin, content);
			std::cout << std::endl << "Server side sending: " << content << std::endl;
			strcpy( region->writePtr()+1, content.c_str() );
			post_send(&region->res, content.size(), IBV_WR_RDMA_WRITE, BUFF_SIZE/2) ;
			poll_completion(&region->res);
		} else if ( op == "2" ) {
			std::cout << std::endl << "Server side commiting." << std::endl;
			region->writePtr()[0] = rdma_data_ready;
			post_send(&region->res, sizeof(char), IBV_WR_RDMA_WRITE, BUFF_SIZE/2 );
			poll_completion(&region->res);
			region->clearCompleteBuffer();
		} else if ( op == "3" ) {
			bool* b = new bool();
			*b = false;
			uint64_t* tid = new uint64_t();
			std::thread* t = new std::thread( &RDMAHandler::create_and_setup_region, &RDMAHandler::getInstance(), &config, tid, b );
			pool.insert( {global_id++, {b,tid,t}} );
		} else if ( op == "4" ) {
			RDMAHandler::getInstance().printRegions();
		} else if ( op == "5" ) {
			abort = true;
		}
	}
	readWorker.join();
	creationWorker.join();

	return 0;
}
