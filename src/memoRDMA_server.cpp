#include <chrono>
#include <iostream>
#include <stdio.h>
#include <string>
#include "RDMARegion.h"
#include "util.h"

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
	//struct resources res;
	RDMARegion region;

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

	// init all the resources, so cleanup will be easy
	region.resources_init();

	// create resources before using them
	region.resources_create(config);

	// connect the QPs
	connect_qp(config, region);
	
	using hrc = std::chrono::high_resolution_clock;
	using usecs = std::chrono::microseconds;
	typedef std::chrono::duration<float> secs;
	// @Server
	std::string content;
	std::cout << "Entering Server side event loop." << std::endl;
	while( true ) {
		std::getline(std::cin, content);
		std::cout << std::endl << "Server side sending: " << content << std::endl;
		strcpy( region.res.buf, content.c_str() );
		auto t_start = hrc::now();
		post_send(&region.res, content.size(), IBV_WR_SEND);
		poll_completion(&region.res);
		auto t_end = hrc::now();
		secs dur = t_end - t_start;
		std::cout << "Transmission of " << content.size() << " Bytes (" << BtoMB(content.size()) << " MB) took " << dur.count() << " us (" << BtoMB(content.size()) / dur.count() << " MB/s)" << std::endl;
	}
}
