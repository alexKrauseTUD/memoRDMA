#include "TaskManager.h"

#include <algorithm>
#include <fstream>
#include <iostream>

#include "Buffer.h"
#include "Connection.h"
#include "ConnectionManager.h"
#include "DataProvider.h"
#include "FunctionalTests.hpp"

TaskManager::TaskManager() : globalId{1} {
    size_t init_flags = 0;
    init_flags |= connection_handling;
    // init_flags |= buffer_handling;
    init_flags |= dummy_tests;
    init_flags |= performance_benchmarks;
    init_flags |= functional_tests;

    setup(init_flags);

    registerTask(std::make_shared<Task>("executeMulti", "Execute multiple by ID with shutdown", [&]() -> void {
        std::vector<std::size_t> taskList;
        std::cout << "Space-separated list of tests to run: " << std::endl
                  << "> " << std::flush;
        std::string content = "";
        const char delimiter = ' ';
        std::getline(std::cin, content);
        try {
            size_t last = 0;
            size_t next = 0;
            while ((next = content.find(delimiter, last)) != std::string::npos) {
                taskList.emplace_back(stol(content.substr(last, next - last)));
                last = next + 1;
            }
            taskList.emplace_back(stol(content.substr(last)));
        } catch (...) {
            std::cout << "[Error] Invalid number(s) detected, nothing done." << std::endl;
            return;
        }

        for (auto v : taskList) {
            std::cout << "[Taskmanager] Executing Task [" << v << "]" << std::endl;
            executeById(v);

            using namespace std::chrono_literals;
            std::this_thread::sleep_for(500ms);
        }

        globalAbort();
    }));

    globalAbort = []() -> void { std::cout << "[TaskManager] No global Abort function set." << std::endl; };
}

TaskManager::~TaskManager() {
    tasks.clear();
}

void TaskManager::registerTask(std::shared_ptr<Task> task) {
    tasks.insert({globalId++, task});
}

void TaskManager::unregisterTask(std::string ident) {
    for (auto task : tasks) {
        if (task.second->ident.compare(ident) == 0) {
            tasks.erase(task.first);
            std::cout << "[TaskManager] Removed Task " << ident << std::endl;
        }
    }
}

bool TaskManager::hasTask(std::string ident) const {
    for (auto task : tasks) {
        if (task.second->ident.compare(ident) == 0) {
            return true;
        }
    }
    return false;
}

void TaskManager::printAll() {
    for (auto it = tasks.begin(); it != tasks.end(); ++it) {
        std::cout << "[" << it->first << "] " << it->second->name << std::endl;
    }
}

void TaskManager::executeById(std::size_t id) {
    auto it = tasks.find(id);
    if (it != tasks.end()) {
        it->second->run();
    }
}

void TaskManager::setup(size_t init_flags) {
    if (init_flags & connection_handling) {
        registerTask(std::make_shared<Task>("openConnection", "Open Connection", []() -> void {
            bool clientMode = false;

            bool correct;
            bool useDefaultConfig;
            std::string input;

            do {
                std::cin.clear();
                std::cin.sync();
                std::cout << "Do you want to use the defaul configuration ('y' / 'yes') or an own one ('n' / 'no')?" << std::endl;
                std::getline(std::cin, input);

                if (input.compare("y") == 0 || input.compare("yes") == 0) {
                    correct = true;
                    useDefaultConfig = true;
                } else if (input.compare("n") == 0 || input.compare("no") == 0) {
                    correct = true;
                    useDefaultConfig = false;
                } else {
                    std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('y' / 'yes' / 'n' / 'no')!" << std::endl;
                    correct = false;
                }

            } while (!correct);

            std::string devName = "mlx5_0";
            std::string serverName = clientMode ? "141.76.47.8" : "141.76.47.9";
            uint32_t tcpPort = 20000;
            int ibPort = 1;
            int gidIndex = 0;

            uint8_t numOwnReceive = 2;
            uint32_t sizeOwnReceive = 1024 * 512;
            uint8_t numRemoteReceive = 2;
            uint32_t sizeRemoteReceive = 1024 * 512;
            uint64_t sizeOwnSend = 1024 * 512;
            uint64_t sizeRemoteSend = 1024 * 512;

            std::size_t largerNum = numOwnReceive < numRemoteReceive ? numRemoteReceive : numOwnReceive;

            std::size_t minMetaInfoSize = 2 * (1 + largerNum);
            uint8_t metaInfoSize = minMetaInfoSize > 16 ? minMetaInfoSize : 16;

            if (!useDefaultConfig) {
                std::cout << "Please enter the Server-IP!" << std::endl;
                std::cin >> serverName;

                std::string devName = "0";

                std::cout << "Please enter the IB-Device-Name! (Default: 0)" << std::endl;
                std::cin >> devName;

                std::cout << "Please enter the TCP-Port that you want to use (if already used, another one is selected automatically)!" << std::endl;
                std::cin >> tcpPort;

                int ibPort = 0;

                do {
                    std::cout << "Please enter the IB-Port that you want to use! (Default: 0)" << std::endl;
                    std::cin >> ibPort;

                    if (ibPort < 0)
                        std::cout << "[Error] The provided IB-Port was incorrect! Please enter the correct number!" << std::endl;

                } while (ibPort < 0);

                int gidIndex = 0;

                do {
                    std::cout << "Please enter the GID-Index that you want to use! (Default: 0)" << std::endl;
                    std::cin >> gidIndex;

                    if (gidIndex < 0)
                        std::cout << "[Error] The provided GID-Index was incorrect! Please enter the correct number!" << std::endl;

                } while (gidIndex < 0);

                bool correctInput2 = false;
                bool confBuffer = false;
                std::string inp2;

                do {
                    std::cin.clear();
                    std::cin.sync();
                    std::cout << "Do you want to configure the buffer quantity and size ('y' / 'yes') or use the default configuration ('n' / 'no')?" << std::endl;
                    std::getline(std::cin, inp2);

                    if (inp2.compare("y") == 0 || inp2.compare("yes") == 0) {
                        correctInput2 = true;
                        confBuffer = true;
                    } else if (inp2.compare("n") == 0 || inp2.compare("no") == 0) {
                        correctInput2 = true;
                        confBuffer = false;
                    } else {
                        std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('y' / 'yes' / 'n' / 'no')!" << std::endl;
                        correctInput2 = false;
                    }

                } while (!correctInput2);

                if (confBuffer) {
                    do {
                        std::cout << "Please enter the number of own Receive-Buffers that you want to create!" << std::endl;
                        std::cin >> numOwnReceive;

                        if (numOwnReceive < 1)
                            std::cout << "[Error] The provided number of own Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

                    } while (numOwnReceive < 1);

                    do {
                        std::cout << "Please enter the size of own Receive-Buffers that you want to create!" << std::endl;
                        std::cin >> sizeOwnReceive;

                        if (sizeOwnReceive < 1)
                            std::cout << "[Error] The provided size of own Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

                    } while (sizeOwnReceive < 1);

                    do {
                        std::cout << "Please enter the number of remote Receive-Buffers that you want to create!" << std::endl;
                        std::cin >> numRemoteReceive;

                        if (numRemoteReceive < 1)
                            std::cout << "[Error] The provided number of remote Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

                    } while (numRemoteReceive < 1);

                    do {
                        std::cout << "Please enter the size of remote Receive-Buffers that you want to create!" << std::endl;
                        std::cin >> sizeRemoteReceive;

                        if (sizeRemoteReceive < 1)
                            std::cout << "[Error] The provided size of remote Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

                    } while (sizeRemoteReceive < 1);

                    do {
                        std::cout << "Please enter the size of own Send-Buffer that you want to create!" << std::endl;
                        std::cin >> sizeOwnSend;

                        if (sizeOwnSend < 1)
                            std::cout << "[Error] The provided size of own Send-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

                    } while (sizeOwnSend < 1);

                    do {
                        std::cout << "Please enter the size of remote Send-Buffer that you want to create!" << std::endl;
                        std::cin >> sizeRemoteSend;

                        if (sizeRemoteSend < 1)
                            std::cout << "[Error] The provided size of remote Send-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

                    } while (sizeRemoteSend < 1);

                    largerNum = numOwnReceive < numRemoteReceive ? numRemoteReceive : numOwnReceive;

                    minMetaInfoSize = 2 * (1 + largerNum);

                    do {
                        std::cout << "Please enter the size for the MetaInfo-Buffer that you want to create! (number of entries)" << std::endl;
                        std::cin >> metaInfoSize;

                        if (metaInfoSize < minMetaInfoSize)
                            std::cout << "[Error] The provided size for the MetaInfo-Buffer was to small! Please enter a number of at least " << minMetaInfoSize << "!" << std::endl;

                    } while (metaInfoSize < minMetaInfoSize);
                }
            }

            config_t config = {.dev_name = devName,
                               .server_name = serverName,
                               .tcp_port = tcpPort ? tcpPort : 20000,
                               .client_mode = clientMode,
                               .ib_port = ibPort,
                               .gid_idx = gidIndex};

            // TODO: configurability hardcoded
            buffer_config_t bufferConfig = {.num_own_send_threads = 2,
                                            .num_own_receive_threads = 2,
                                            .num_remote_send_threads = 2,
                                            .num_remote_receive_threads = 2,
                                            .num_own_receive = numOwnReceive,
                                            .size_own_receive = sizeOwnReceive,
                                            .num_remote_receive = numRemoteReceive,
                                            .size_remote_receive = sizeRemoteReceive,
                                            .num_own_send = numRemoteReceive,
                                            .size_own_send = sizeRemoteReceive,
                                            .num_remote_send = numOwnReceive,
                                            .size_remote_send = sizeOwnReceive,
                                            .meta_info_size = 16};

            std::size_t connectionId = ConnectionManager::getInstance().registerConnection(config, bufferConfig);

            if (connectionId != 0) {
                std::cout << "[Success] Connection " << connectionId << " opened for config: " << std::endl;
            } else {
                std::cout << "[Error] Something went wrong! The connection could not be opened for config: " << std::endl;
            }
            print_config(config);
            std::cout << std::endl;
            std::cout << std::endl;
        }));

        registerTask(std::make_shared<Task>("listenConnection", "Listen for Connection", []() -> void {
            bool clientMode = true;

            bool correct;
            bool useDefaultConfig;
            std::string input;

            do {
                std::cin.clear();
                std::cin.sync();
                std::cout << "Do you want to use the defaul configuration ('y' / 'yes') or an own one ('n' / 'no')?" << std::endl;
                std::getline(std::cin, input);

                if (input.compare("y") == 0 || input.compare("yes") == 0) {
                    correct = true;
                    useDefaultConfig = true;
                } else if (input.compare("n") == 0 || input.compare("no") == 0) {
                    correct = true;
                    useDefaultConfig = false;
                } else {
                    std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('y' / 'yes' / 'n' / 'no')!" << std::endl;
                    correct = false;
                }

            } while (!correct);

            uint32_t tcpPort = 20000;

            if (!useDefaultConfig) {
                std::cout << "Please enter the TCP-Port that you want to use (if already used, another one is selected automatically)!" << std::endl;
                std::cin >> tcpPort;
            }

            config_t config = {.dev_name = "mlx5_0",
                               .server_name = clientMode ? "141.76.47.8" : "141.76.47.9",
                               .tcp_port = tcpPort ? tcpPort : 20000,
                               .client_mode = clientMode,
                               .ib_port = 1,
                               .gid_idx = 0};

            buffer_config_t bufferConfig;

            std::size_t connectionId = ConnectionManager::getInstance().registerConnection(config, bufferConfig);

            if (connectionId != 0) {
                std::cout << "[Success] Connection " << connectionId << " opened for config: " << std::endl;
            } else {
                std::cout << "[Error] Something went wrong! The connection could not be opened for config: " << std::endl;
            }
            print_config(config);
            std::cout << std::endl;
            std::cout << std::endl;
        }));

        registerTask(std::make_shared<Task>("printConnections", "Print Connections", []() -> void {
            ConnectionManager::getInstance().printConnections();
        }));

        registerTask(std::make_shared<Task>("closeConnection", "Close Connection", []() -> void {
            std::size_t connectionId;

            std::cout << "Please enter the name of the connection you want to close!" << std::endl;
            // TODO: check whether this works
            std::cin >> connectionId;

            ConnectionManager::getInstance().closeConnection(connectionId);
        }));

        registerTask(std::make_shared<Task>("closeAllConnections", "Close All Connections", []() -> void {
            ConnectionManager::getInstance().closeAllConnections();
        }));
    }

    if (init_flags & buffer_handling) {
        registerTask(std::make_shared<Task>("addReceiveBuffer", "Add Receive Buffer", []() -> void {
            std::size_t connectionId;
            std::size_t quantity;
            bool own;
            bool correct;
            std::string input;

            std::cout << "Please enter the name of the connection you want to change!" << std::endl;
            std::cin >> connectionId;

            std::cout << "How many Receive-Buffer do you want to add?" << std::endl;
            std::cin >> quantity;

            do {
                std::cin.clear();
                std::cin.sync();
                std::cout << "Do you want to add on the own ('o') or on the remote ('r') site?" << std::endl;
                std::getline(std::cin, input);

                if (input.compare("o") == 0 || input.compare("own") == 0) {
                    correct = true;
                    own = true;
                } else if (input.compare("r") == 0 || input.compare("remote") == 0) {
                    correct = true;
                    own = false;
                } else {
                    std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
                    correct = false;
                }

            } while (!correct);

            ConnectionManager::getInstance().addReceiveBuffer(connectionId, quantity, own);
        }));

        registerTask(std::make_shared<Task>("removeReceiveBuffer", "Remove Receive Buffer", []() -> void {
            std::size_t connectionId;
            std::size_t quantity;
            bool own;
            bool correct;
            std::string input;

            std::cout << "Please enter the name of the connection you want to change!" << std::endl;
            std::cin >> connectionId;

            std::cout << "How many Receive-Buffer do you want to remove? (At least 1 Receive-Buffer will be kept.)" << std::endl;
            std::cin >> quantity;

            do {
                std::cin.clear();
                std::cin.sync();
                std::cout << "Do you want to remove on the own ('o') or on the remote ('r') site?" << std::endl;
                std::getline(std::cin, input);

                if (input.compare("o") == 0 || input.compare("own") == 0) {
                    correct = true;
                    own = true;
                } else if (input.compare("r") == 0 || input.compare("remote") == 0) {
                    correct = true;
                    own = false;
                } else {
                    std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
                    correct = false;
                }

            } while (!correct);

            ConnectionManager::getInstance().removeReceiveBuffer(connectionId, quantity, own);
        }));

        registerTask(std::make_shared<Task>("resizeReceiveBuffer", "Resize Receive Buffer", []() -> void {
            std::size_t connectionId;
            std::size_t newSize;
            bool own;
            bool correct;
            std::string input;

            std::cout << "Please enter the name of the connection you want to change!" << std::endl;
            std::cin >> connectionId;

            std::cout << "Please enter the new size for all existing Receive-Buffer." << std::endl;
            std::cin >> newSize;

            do {
                std::cin.clear();
                std::cin.sync();
                std::cout << "Do you want to resize on the own ('o') or on the remote ('r') site?" << std::endl;
                std::getline(std::cin, input);

                if (input.compare("o") == 0 || input.compare("own") == 0) {
                    correct = true;
                    own = true;
                } else if (input.compare("r") == 0 || input.compare("remote") == 0) {
                    correct = true;
                    own = false;
                } else {
                    std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
                    correct = false;
                }

            } while (!correct);

            ConnectionManager::getInstance().resizeReceiveBuffer(connectionId, newSize, own);
        }));

        registerTask(std::make_shared<Task>("resizeSendBuffer", "Resize Send Buffer", []() -> void {
            std::size_t connectionId;
            std::size_t newSize;
            bool own;
            bool correct;
            std::string input;

            std::cout << "Please enter the name of the connection you want to change!" << std::endl;
            std::cin >> connectionId;

            std::cout << "Please enter the new size for the Send-Buffer." << std::endl;
            std::cin >> newSize;

            do {
                std::cin.clear();
                std::cin.sync();
                std::cout << "Do you want to resize on the own ('o') or on the remote ('r') site?" << std::endl;
                std::getline(std::cin, input);

                if (input.compare("o") == 0 || input.compare("own") == 0) {
                    correct = true;
                    own = true;
                } else if (input.compare("r") == 0 || input.compare("remote") == 0) {
                    correct = true;
                    own = false;
                } else {
                    std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
                    correct = false;
                }

            } while (!correct);

            ConnectionManager::getInstance().resizeSendBuffer(connectionId, newSize, own);
        }));
    }

    if (init_flags & dummy_tests) {
        // registerTask(std::make_shared<Task>("dummyToAll", "Send Dummy to all Connections", []() -> void {
        //     std::string dummy = "This is a dummy message.";
        //     ConnectionManager::getInstance().sendDataToAllConnections(dummy);
        // }));

        registerTask(std::make_shared<Task>("customOpcode", "Send Custom opcode to all Connections", []() -> void {
            uint8_t val;
            uint64_t input;
            std::cout << "Opcode? [0,255]" << std::endl;
            std::cin >> input;

            val = (uint8_t)std::clamp(input, (uint64_t)0, (uint64_t)UINT8_MAX);

            ConnectionManager::getInstance().sendCustomOpcodeToAllConnections(val);
            std::cout << "Custom opcode sent." << std::endl;
        }));
    }

    if (init_flags & performance_benchmarks) {
        registerTask(std::make_shared<Task>("ss_tput", "Single-sided throughput benchmark", [this]() -> void {
            genericBenchFunc("ss_tput", "Single-sided throughput benchmark", ss_tput, 1, Strategies::push);
        }));

        registerTask(std::make_shared<Task>("ds_tput", "Double-sided throughput benchmark", [this]() -> void {
            genericBenchFunc("ds_tput", "Double-sided throughput benchmark", ds_tput, 1, Strategies::push);
        }));

        registerTask(std::make_shared<Task>("ss_tput_pull", "Single-sided throughput benchmark PULL", [this]() -> void {
            genericBenchFunc("ss_tput_pull", "Single-sided throughput benchmark PULL", ss_tput, 1, Strategies::pull);
        }));

        registerTask(std::make_shared<Task>("ds_tput_pull", "Double-sided throughput benchmark PULL", [this]() -> void {
            genericBenchFunc("ds_tput_pull", "Double-sided throughput benchmark PULL", ds_tput, 1, Strategies::pull);
        }));
    }

    if (init_flags & functional_tests) {
        registerTask(std::make_shared<Task>("all_func_tests", "Execute all functional tests", [this]() -> void {
            FunctionalTests::getInstance().executeAllTests(false);
        }));

        registerTask(std::make_shared<Task>("all_func_tests_lite", "Execute all functional tests lite", [this]() -> void {
            FunctionalTests::getInstance().executeAllTests(true);
        }));
    }
}

void TaskManager::setGlobalAbortFunction(std::function<void()> fn) {
    globalAbort = fn;
}

void TaskManager::genericBenchFunc(std::string shortName, std::string name, bench_code tc, std::size_t connectionId, Strategies strat) {
    using namespace std::chrono_literals;

    for (uint8_t num_rb = 1; num_rb <= 8; ++num_rb) {
        for (uint8_t num_sb = 1; num_sb <= num_rb; ++num_sb) {
            for (uint8_t thrds = 1; thrds <= num_sb; ++thrds) {
                auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                std::stringstream logNameStream;
                logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << shortName << "_" << +num_sb << "_" << +num_rb << "_" << +thrds << ".log";
                std::string logName = logNameStream.str();
                std::cout << "[Task] Set name: " << logName << std::endl;

                for (uint64_t size_rb = 1ull << 15; size_rb < 1ull << 28; size_rb <<= 1) {
                    buffer_config_t bufferConfig = {.num_own_send_threads = thrds,
                                                    .num_own_receive_threads = 1,
                                                    .num_remote_send_threads = 1,
                                                    .num_remote_receive_threads = thrds,
                                                    .num_own_receive = 1,
                                                    .size_own_receive = 640,
                                                    .num_remote_receive = num_rb,
                                                    .size_remote_receive = size_rb + package_t::metaDataSize(),
                                                    .num_own_send = num_sb,
                                                    .size_own_send = size_rb + package_t::metaDataSize(),
                                                    .num_remote_send = 1,
                                                    .size_remote_send = 640,
                                                    .meta_info_size = 16};

                    CHECK(ConnectionManager::getInstance().reconfigureBuffer(connectionId, bufferConfig));

                    std::cout << "[main] Used connection with id '" << connectionId << "' and " << +num_rb << " remote receive buffer (size for one remote receive: " << GetBytesReadable(size_rb) << ")" << std::endl;
                    std::cout << std::endl;
                    std::cout << name << std::endl;

                    switch (tc) {
                        case ss_tput:
                            CHECK(ConnectionManager::getInstance().throughputTest(connectionId, logName, strat));
                            break;
                        case ds_tput:
                            CHECK(ConnectionManager::getInstance().consumingTest(connectionId, logName, strat));
                            break;
                        default:
                            std::cout << "A non-valid test_code was provided!";
                            return;
                    }

                    std::cout << std::endl;
                    std::cout << name << " ended." << std::endl;
                }
            }
        }
    }
}