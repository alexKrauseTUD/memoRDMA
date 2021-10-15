#include <chrono>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <map>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <algorithm>
#include "common.h"
#include "util.h"
#include "RDMARegion.h"
#include "RDMAHandler.h"
#include "DataProvider.h"
#include "RDMACommunicator.h"
#include "TaskManager.h"

int main(int argc, char *argv[]) {
	config_t config = {.dev_name = NULL,
                          .server_name = NULL,
                          .tcp_port = 20000,
                          .client_mode = false,
                          .ib_port = 1,
                          .gid_idx = -1
                        };

	// \begin parse command line parameters
	while (1) {
		int c;
		static struct option long_options[] = {
			{"port", required_argument, 0, 'p'},
			{"client", no_argument, 0, 'c'},
			{"ib-dev", required_argument, 0, 'd'},
			{"ib-port", required_argument, 0, 'i'},
			{"gid-idx", required_argument, 0, 'g'},
			{"help", no_argument, 0, 'h'},
			{NULL, 0, 0, 0}};

		c = getopt_long(argc, argv, "p:cd:i:g:h", long_options, NULL);
		if (c == -1)
			break;

		switch (c) {
		case 'p':
			config.tcp_port = strtoul(optarg, NULL, 0);
			break;
		case 'c':
			config.client_mode = true;
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
	{
		using namespace std::chrono_literals;
		std::this_thread::sleep_for( 100ms );
	}

	bool abort = false;
	auto globalExit = [&]() -> void {
		{
			using namespace std::chrono_literals;
			std::this_thread::sleep_for( 500ms );
		}
		RDMAHandler::getInstance().communicationBuffer->setCommitCode( rdma_shutdown );
		RDMACommunicator::getInstance().stop();
		abort = true;
	};

	TaskManager tm;
	tm.setGlobalAbortFunction( globalExit );

	if (!config.client_mode) {
		std::string content;
		std::string op;

		std::cout << "Entering Server side event loop." << std::endl;
		while ( !abort ) {
			op = "-1";
			tm.printAll();
			std::cout << "Type \"exit\" to terminate." << std::endl;
			// std::cin >> op;
			std::getline( std::cin, op, '\n' );
			if ( op == "-1" ) {
				globalExit();
				continue;
			}

			std::cout << "Chosen:" << op << std::endl;
			std::transform(op.begin(), op.end(), op.begin(), [](unsigned char c){ return std::tolower(c); });

			if ( op == "exit" ) {
				globalExit();
			} else {
				std::size_t id;
				bool converted = false;
				try {
					id = stol( op );
					converted = true;
				} catch( ... ) {
					std::cout << "[Error] No number given." << std::endl;
					continue;
				}
				if ( converted ) {
					tm.executeById( id );
				}
			}
		}
	} else {
		/* Client */
		std::cout << "Entering CLIENT side event loop." << std::endl;
		while ( !RDMACommunicator::getInstance().abortSignaled() ) {
			using namespace std::chrono_literals;
			std::this_thread::sleep_for( 100ms );
		}
	}

	return 0;
}
