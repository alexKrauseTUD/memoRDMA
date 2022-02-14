#include <stdio.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <thread>

#include "ConnectionManager.h"
#include "TaskManager.h"
#include "common.h"
#include "util.h"

int main(int argc, char *argv[]) {
    config_t config = {.dev_name = NULL,
                       .server_name = NULL,
                       .tcp_port = 20000,
                       .client_mode = false,
                       .ib_port = 1,
                       .gid_idx = -1};

    // \begin parse command line parameters
    while (1) {
        int c;
        static struct option long_options[] = {
            {"client", no_argument, 0, 'c'},
            {"help", no_argument, 0, 'h'},
            {NULL, 0, 0, 0}};

        c = getopt_long(argc, argv, "ch", long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
            case 'c':
                config.client_mode = true;
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
    {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ms);
    }

    bool abort = false;
    auto globalExit = [&]() -> void {
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(500ms);
        }
        ConnectionManager::getInstance().stop();
        abort = true;
    };

    TaskManager tm;
    tm.setGlobalAbortFunction(globalExit);

    if (!config.client_mode) {
        std::string content;
        std::string op;

        std::cout << "Entering Server side event loop." << std::endl;
        while (!abort) {
            op = "-1";
            tm.printAll();
            std::cout << "Type \"exit\" to terminate." << std::endl;
            // std::cin >> op;
            std::getline(std::cin, op, '\n');
            if (op == "-1") {
                globalExit();
                continue;
            }

            std::cout << "Chosen:" << op << std::endl;
            std::transform(op.begin(), op.end(), op.begin(), [](unsigned char c) { return std::tolower(c); });

            if (op == "exit") {
                globalExit();
            } else {
                std::size_t id;
                bool converted = false;
                try {
                    id = stol(op);
                    converted = true;
                } catch (...) {
                    std::cout << "[Error] No number given." << std::endl;
                    continue;
                }
                if (converted) {
                    tm.executeById(id);
                }
            }
        }
    } else {
        /* Client */
        std::cout << "Entering CLIENT side event loop." << std::endl;
        while (!ConnectionManager::getInstance().abortSignaled()) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(100ms);
        }
    }

    return 0;
}
