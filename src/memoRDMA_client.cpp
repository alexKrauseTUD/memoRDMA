#include <util.h>
#include <chrono>
#include <iostream>
#include <stdio.h>
#include <string>
#include <thread>
#include <chrono>
#include "RDMAHandler.h"

double BtoMB( uint32_t byte ) {
	return static_cast<double>(byte) / 1024 / 1024;
}

void check_receive( RDMARegion* region, bool* abort ) {
	std::cout << "Starting Client-side monitoring thread" << std::endl;
	using namespace std::chrono_literals;
	while( !*abort ) {
		if ( region->res.buf[0] != rdma_data_ready ) {
			std::cout << "Current postbox byte: " << std::hex << region->res.buf[0] << std::endl;
			std::this_thread::sleep_for( 1000ms );
		} else {
			std::cout << "Current postbox byte: " << std::hex << region->res.buf[0] << std::endl;
			std::cout << "Client side received: " << region->res.buf+1 << std::endl;
			region->clearBuffer();
			std::this_thread::sleep_for( 1000ms );
		}
	}
}

int main(int argc, char *argv[]) {
	config_t config = {.dev_name = NULL,
                          .server_name = NULL,
                          .tcp_port = 20000,
                          .ib_port = 1,
                          .gid_idx = -1
                        };
	std::cout << "Running stuff" << std::endl;
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
	// \end parse command line parameters

	print_config(config);
	
	RDMAHandler::getInstance().setupCommunicationBuffer( config );
	auto region = RDMAHandler::getInstance().communicationBuffer;

	std::string op;
	bool abort = false;

	std::thread t(check_receive, region, &abort);
	while ( !abort ) {
		std::cout << "Choose an opcode: [1] Read from region [2] Exit.";
  		std::cin >> op;
		std::cout << "Chosen:" << op << std::endl;

		if ( op == "1" ) {
			// std::cout << "Client side received: " << region->res.buf << std::endl << std::endl;
		} else if ( op == "2" ) {
			abort = true;
		}
	}
	std::cout << "Waiting for receiver thread to finish..." << std::endl;
	t.join();
	return 0;

	// // @Client
	// std::cout << "Entering Client side event loop." << std::endl;
	// while( true ) {
	// 	poll_completion(&region->res);
	// 	std::cout << "Client side received: " << region->res.buf << std::endl << std::endl;
	// 	post_receive(&region->res);
	// }
}
