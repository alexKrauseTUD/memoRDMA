#include "TaskManager.h"

#include <algorithm>
#include <fstream>
#include <iostream>

#include "Buffer.h"
#include "Connection.h"
#include "ConnectionManager.h"
#include "DataProvider.h"
#include "FunctionalTests.hpp"
#include "Logger.h"
#include "Utility.h"

using namespace memordma;

static void print_config(struct config_t& config) {
    Logger::getInstance() << LogLevel::INFO << "\tDevice name:\t\t" << config.dev_name << std::endl;
    Logger::getInstance() << LogLevel::INFO << "\tIB port:\t\t" << config.ib_port << std::endl;

    if (!config.server_name.empty()) {
        Logger::getInstance() << LogLevel::INFO << "\tIP:\t\t\t" << config.server_name << std::endl;
    }

    Logger::getInstance() << LogLevel::INFO << "\tTCP port:\t\t" << config.tcp_port << std::endl;

    if (config.gid_idx >= 0) {
        Logger::getInstance() << LogLevel::INFO << "\tGID index:\t\t" << config.gid_idx << std::endl;
    }
}

static void print_buff_cfg(struct config_t& config, struct buffer_config_t& bufferConfig) {
    std::cout << "Remote IP:\t\t\t" << config.server_name << "\n"
              << "\tOwn SB Number:\t\t" << +bufferConfig.num_own_send << "\n"
              << "\tOwn SB Size:\t\t" << bufferConfig.size_own_send << "\n"
              << "\tOwn RB Number:\t\t" << +bufferConfig.num_own_receive << "\n"
              << "\tOwn RB Size:\t\t" << bufferConfig.size_own_receive << "\n"
              << "\tRemote SB Number:\t" << +bufferConfig.num_remote_send << "\n"
              << "\tRemote SB Size:\t\t" << bufferConfig.size_remote_send << "\n"
              << "\tRemote RB Number:\t" << +bufferConfig.num_remote_receive << "\n"
              << "\tRemote RB Size:\t\t" << bufferConfig.size_remote_receive << "\n"
              << "\tOwn S Threads:\t\t" << +bufferConfig.num_own_send_threads << "\n"
              << "\tOwn R Threads:\t\t" << +bufferConfig.num_own_receive_threads << "\n"
              << "\tRemote S Threads:\t" << +bufferConfig.num_remote_send_threads << "\n"
              << "\tRemote R Threads:\t" << +bufferConfig.num_remote_receive_threads << "\n"
              << std::endl;
}

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
            Logger::getInstance() << LogLevel::ERROR << "Invalid number(s) detected, nothing done." << std::endl;
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

void TaskManager::executeByIdent(std::string name) {
    for ( auto t : tasks ) {
        if ( t.second->ident == name ) {
            t.second->run();
            return;
        }
    }
}

void TaskManager::setup(size_t init_flags) {
    if (init_flags & connection_handling) {
        registerTask(std::make_shared<Task>("openConnection", "Open Connection", []() -> void {
            uint8_t numOwnReceive = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_OWN_RECEIVE_BUFFER_COUNT);
            uint32_t sizeOwnReceive = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMO_DEFAULT_OWN_RECEIVE_BUFFER_SIZE);
            uint8_t numRemoteReceive = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_REMOTE_RECEIVE_BUFFER_COUNT);
            uint32_t sizeRemoteReceive = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMO_DEFAULT_REMOTE_RECEIVE_BUFFER_SIZE);
            uint64_t sizeOwnSend = ConnectionManager::getInstance().configuration->get<uint64_t>(MEMO_DEFAULT_OWN_SEND_BUFFER_SIZE);
            uint64_t sizeRemoteSend = ConnectionManager::getInstance().configuration->get<uint64_t>(MEMO_DEFAULT_REMOTE_SEND_BUFFER_SIZE);

            /* This should be used to adapt the meta info struct. However, we currently only allow 8 buffer per side, hard coded.
                std::size_t largerNum = numOwnReceive < numRemoteReceive ? numRemoteReceive : numOwnReceive;
                std::size_t minMetaInfoSize = 2 * (1 + largerNum);
                const uint8_t defaultMetaSize = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_META_INFO_SIZE);
                uint8_t metaInfoSize = minMetaInfoSize > defaultMetaSize ? minMetaInfoSize : defaultMetaSize;
            */
            config_t config = {.dev_name = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_IB_DEVICE_NAME),
                               .server_name = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_CONNECTION_AUTO_INITIATE_IP),
                               .tcp_port = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMO_DEFAULT_TCP_PORT),
                               .client_mode = false,
                               .ib_port = ConnectionManager::getInstance().configuration->get<int32_t>(MEMO_DEFAULT_IB_PORT),
                               .gid_idx = ConnectionManager::getInstance().configuration->get<int32_t>(MEMO_DEFAULT_IB_GLOBAL_INDEX)};

            buffer_config_t bufferConfig = {.num_own_send_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_OWN_SEND_THREADS),
                                            .num_own_receive_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_OWN_RECEIVE_THREADS),
                                            .num_remote_send_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_REMOTE_SEND_THREADS),
                                            .num_remote_receive_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_REMOTE_RECEIVE_THREADS),
                                            .num_own_receive = numOwnReceive,
                                            .size_own_receive = sizeOwnReceive,
                                            .num_remote_receive = numRemoteReceive,
                                            .size_remote_receive = sizeRemoteReceive,
                                            .num_own_send = numRemoteReceive,
                                            .size_own_send = sizeOwnSend,
                                            .num_remote_send = numOwnReceive,
                                            .size_remote_send = sizeRemoteSend,
                                            .meta_info_size = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMO_DEFAULT_META_INFO_SIZE)};

            print_config(config);
            print_buff_cfg(config, bufferConfig);
            std::size_t connectionId = ConnectionManager::getInstance().registerConnection(config, bufferConfig);

            if (connectionId != 0) {
                Logger::getInstance() << LogLevel::SUCCESS << "Connection " << connectionId << " opened for config: " << std::endl;
                ConnectionManager::getInstance().getConnectionById(connectionId)->printConnectionInfo();
            } else {
                Logger::getInstance() << LogLevel::ERROR << "Something went wrong! The connection could not be opened for config: " << std::endl;
                print_config(config);
            }
            std::cout << std::endl;
            std::cout << std::endl;
        }));

        registerTask(std::make_shared<Task>("listenConnection", "Listen for Connection", []() -> void {
            config_t config = {.dev_name = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_IB_DEVICE_NAME),
                               .server_name = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_CONNECTION_AUTO_LISTEN_IP),
                               .tcp_port = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMO_DEFAULT_TCP_PORT),
                               .client_mode = true,
                               .ib_port = ConnectionManager::getInstance().configuration->get<int32_t>(MEMO_DEFAULT_IB_PORT),
                               .gid_idx = ConnectionManager::getInstance().configuration->get<int32_t>(MEMO_DEFAULT_IB_GLOBAL_INDEX)};

            buffer_config_t bufferConfig;

            std::size_t connectionId = ConnectionManager::getInstance().registerConnection(config, bufferConfig);

            if (connectionId != 0) {
                Logger::getInstance() << LogLevel::SUCCESS << "Connection " << connectionId << " opened for config: " << std::endl;
            } else {
                Logger::getInstance() << LogLevel::ERROR << "Something went wrong! The connection could not be opened for config: " << std::endl;
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
            ConnectionManager::getInstance().closeAllConnections(true);
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
                    Logger::getInstance() << LogLevel::ERROR << "Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
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
                    Logger::getInstance() << LogLevel::ERROR << "Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
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
                    Logger::getInstance() << LogLevel::ERROR << "Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
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
                    Logger::getInstance() << LogLevel::ERROR << "Your input was not interpretable! Please enter one of the given possibilities ('o' / 'own' / 'r' / 'remote')!" << std::endl;
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

                    Utility::check_or_die(ConnectionManager::getInstance().reconfigureBuffer(connectionId, bufferConfig));

                    std::cout << "[main] Used connection with id '" << connectionId << "' and " << +num_rb << " remote receive buffer (size for one remote receive: " << Utility::GetBytesReadable(size_rb) << ")" << std::endl;
                    std::cout << std::endl;
                    std::cout << name << std::endl;

                    switch (tc) {
                        case ss_tput:
                            Utility::check_or_die(ConnectionManager::getInstance().throughputBenchmark(connectionId, logName, strat));
                            break;
                        case ds_tput:
                            Utility::check_or_die(ConnectionManager::getInstance().consumingBenchmark(connectionId, logName, strat));
                            break;
                        default:
                            std::cout << "A non-valid bench_code was provided!";
                            return;
                    }

                    std::cout << std::endl;
                    std::cout << name << " ended." << std::endl;
                }
            }
        }
    }
}
