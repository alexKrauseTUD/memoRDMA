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
#include "RDMACommunicator.h"

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
	RDMACommunicator::getInstance().init( config );

	std::string content;
	std::string op;

	bool abort = false;
	std::cout << "Entering Server side event loop." << std::endl;
	while ( !abort ) {
		std::cout << "Choose an opcode:\n[1] Direct write ";
		std::cout << "[2] Commit ";
		std::cout << "[3] Create new region." << std::endl;
		std::cout << "[4] Print Regions. ";
		std::cout << "[5] Delete Region. ";
		std::cout << "[6] Send dummy to all regions." << std	::endl;
		std::cout << "[7] Fetch data from a fixed source. ";
		std::cout << "[8] Single-sided throughput test.";
		std::cout << "[9] Exit" << std::endl;
  		std::cin >> op;
		std::cout << "Chosen:" << op << std::endl;
		std::getline(std::cin, content);

		if ( op == "1" ) { /* Direct write to Region */
			RDMARegion* sendingRegion = RDMAHandler::getInstance().selectRegion( true );
			std::cout << "Content: " << std::flush;
			std::getline(std::cin, content);
			
			if ( sendingRegion ) {
				sendingRegion->setSendData( content );
			} else {
				std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
			}
		} else if ( op == "2" ) { /* Commit string data to region */
			RDMARegion* sendingRegion = RDMAHandler::getInstance().selectRegion( true );
			if ( sendingRegion ) {
				std::cout << std::endl << "Server side commiting." << std::endl;
				sendingRegion->setCommitCode( rdma_data_ready );
			} else {
				std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
			}
		} else if ( op == "3" ) { /* Create new Region */
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
			RDMACommunicator::getInstance().setupNewRegion( config, bytes );
		} else if ( op == "4" ) { /* Print Regions */
			RDMAHandler::getInstance().printRegions();
		} else if ( op == "5" ) { /* Delete */
			RDMARegion* sendingRegion = RDMAHandler::getInstance().selectRegion( false );
			if ( sendingRegion ) {
				std::cout << std::endl << "Server side asking to delete region." << std::endl;
				sendingRegion->setCommitCode( rdma_delete_region );
				sendingRegion->receivePtr()[0] = rdma_delete_region;
			} else {
				std::cout << "[Error] Invalid Region ID. Nothing done." << std::endl;
			}
		} else if ( op == "6" ) { /* Send dummy data to all regions except default */
			auto regs = RDMAHandler::getInstance().getAllRegions();
			std::string dummy = "This is a dummy message.";
			for ( auto r : regs ) {
				r->setSendData( dummy );
				r->setCommitCode( rdma_data_ready );
			}
		} else if ( op == "7" ) { /* Fetch data benchmark */
			RDMARegion* requestRegion = RDMAHandler::getInstance().selectRegion( true );
			if ( requestRegion ) {
				requestRegion->setSendData( "Please give data from Random source." );
				requestRegion->setCommitCode( rdma_data_fetch );
			}
		} else if ( op == "8" ) { /* Single-sided TPut test */
			for ( std::size_t bytes = 1ull << 10; bytes < 1ull << 32; bytes <<= 1 ) {
				RDMACommunicator::getInstance().setupNewRegion( config, bytes );
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
			std::cout << std::endl << "[memoRDMA server] Test-suite finished. You can now shut down the remote process." << std::endl;
			abort = true;
		} else if ( op == "9" ) { /* End me */
			RDMACommunicator::getInstance().stop();
			abort = true;
		}
	}

	return 0;
}
