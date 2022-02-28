#include "TaskManager.h"

#include <fstream>
#include <iostream>

#include "Buffer.h"
#include "Connection.h"
#include "ConnectionManager.h"
#include "DataProvider.h"

TaskManager::TaskManager() : globalId{1} {
    setup();

    registerTask(new Task("executeMulti", "Execute multiple by ID with shutdown", [&]() -> void {
        std::vector< std::size_t > taskList;
        std::cout << "Space-separated list of tests to run: " << std::endl << "> " << std::flush;
        std::string content = "";
        const char delimiter = ' ';
        std::getline( std::cin, content );
        try {
            size_t last = 0; 
            size_t next = 0; 
            while ( ( next = content.find( delimiter, last ) ) != std::string::npos ) {
                taskList.emplace_back( stol( content.substr( last, next - last ) ) );
                last = next + 1; 
            } 
            taskList.emplace_back( stol( content.substr( last ) ) );
        } catch( ... ) {
            std::cout << "[Error] Invalid number(s) detected, nothing done." << std::endl;
            return;
        }
        
        for ( auto v : taskList ) {
            std::cout << "[Taskmanager] Executing Task [" << v << "]" << std::endl;
            executeById( v );
			
            using namespace std::chrono_literals;
			std::this_thread::sleep_for( 500ms );
        }

        globalAbort(); }));

    globalAbort = []() -> void { std::cout << "[TaskManager] No global Abort function set." << std::endl; };
}

TaskManager::~TaskManager() {
    for (auto it = tasks.begin(); it != tasks.end(); ++it) {
        delete it->second;
    }
}

void TaskManager::registerTask(Task *task) {
    tasks.insert({globalId++, task});
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

// void TaskManager::executeByIdent(std::string name) {
// }

void TaskManager::setup() {
    registerTask(new Task("openConnection", "Open Connection", []() -> void {
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

        unsigned int numOwnReceive = 2;
        std::size_t sizeOwnReceive = 1024 * 1024 * 2 + 32;
        unsigned int numRemoteReceive = 4;
        std::size_t sizeRemoteReceive = 1024 * 1024 * 2 + 32;
        std::size_t sizeOwnSend = 1024 * 1024 * 8 + 4 * 32;
        std::size_t sizeRemoteSend = 1024 * 1024 * 4 + 4 * 32;

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
            }
        }

        config_t config = {.dev_name = devName,
                           .server_name = serverName,
                           .tcp_port = tcpPort ? tcpPort : 20000,
                           .client_mode = clientMode,
                           .ib_port = ibPort,
                           .gid_idx = gidIndex};

        buffer_config_t bufferConfig = {.num_own_receive = numOwnReceive,
                                        .size_own_receive = sizeOwnReceive,
                                        .num_remote_receive = numRemoteReceive,
                                        .size_remote_receive = sizeRemoteReceive,
                                        .size_own_send = sizeOwnSend,
                                        .size_remote_send = sizeRemoteSend};

        std::string connectionName;

        std::cout << "Please enter a name for the connection that you want to create! (Numerical suggested)" << std::endl;
        std::getline(std::cin, connectionName);

        if (ConnectionManager::getInstance().openConnection(connectionName, config, bufferConfig)) {
            std::cout << "[Success] Connection opened for config: " << std::endl;
        } else {
            std::cout << "[Error] Something went wrong! The connection could not be opened for config: " << std::endl;
        }
        print_config(config);
        std::cout << std::endl;
        std::cout << std::endl;
    }));

    registerTask(new Task("listenConnection", "Listen for Connection", []() -> void {
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

        print_config(config);

        std::string connectionName;

        std::cout << "Please enter a name for the connection that you want to create! (Numerical suggested)" << std::endl;
        std::getline(std::cin, connectionName);

        if (ConnectionManager::getInstance().receiveConnection(connectionName, config)) {
            std::cout << "[Success] Connection opened for config: " << std::endl;
        } else {
            std::cout << "[Error] Something went wrong! The connection could not be opened for config: " << std::endl;
        }
        print_config(config);
        std::cout << std::endl;
        std::cout << std::endl;
    }));

    registerTask(new Task("printConnections", "Print Connections", []() -> void { ConnectionManager::getInstance().printConnections(); }));

    registerTask(new Task("closeConnection", "Close Connection", []() -> void {
        std::string name;

        std::cout << "Please enter the name of the connection you want to close!" << std::endl;
        std::getline(std::cin, name);

        ConnectionManager::getInstance().closeConnection(name); }));

    registerTask(new Task("closeAllConnections", "Close All Connections", []() -> void { ConnectionManager::getInstance().closeAllConnections(); }));

    registerTask(new Task("addReceiveBuffer", "Add Receive Buffer", []() -> void {
        std::string name;
        unsigned int quantity;

        std::cout << "Please enter the name of the connection you want to change!" << std::endl;
        std::getline(std::cin, name);

        std::cout << "How many Receive-Buffer do you want to add?" << std::endl;
        std::cin >> quantity;

        ConnectionManager::getInstance().addReceiveBuffer(name, quantity); }));

    registerTask(new Task("removeReceiveBuffer", "Remove Receive Buffer", []() -> void {
        std::string name;
        unsigned int quantity;

        std::cout << "Please enter the name of the connection you want to change!" << std::endl;
        std::getline(std::cin, name);

        std::cout << "How many Receive-Buffer do you want to remove? (At least 1 Receive-Buffer will be kept.)" << std::endl;
        std::cin >> quantity;

        ConnectionManager::getInstance().removeReceiveBuffer(name, quantity); }));

    registerTask(new Task("resizeReceiveBuffer", "Resize Receive Buffer", []() -> void {
        std::string name;
        std::size_t newSize;

        std::cout << "Please enter the name of the connection you want to change!" << std::endl;
        std::getline(std::cin, name);

        std::cout << "Please enter the new size for all existing Receive-Buffer." << std::endl;
        std::cin >> newSize;

        ConnectionManager::getInstance().resizeReceiveBuffer(name, newSize); }));

    registerTask(new Task("resizeSendBuffer", "Resize Send Buffer", []() -> void {
        std::string name;
        std::size_t newSize;

        std::cout << "Please enter the name of the connection you want to change!" << std::endl;
        std::getline(std::cin, name);

        std::cout << "Please enter the new size for the Send-Buffer." << std::endl;
        std::cin >> newSize;

        ConnectionManager::getInstance().resizeSendBuffer(name, newSize); }));

    registerTask(new Task("dummyToAll", "Send Dummy to all Connections", []() -> void {
        std::string dummy = "This is a dummy message.";
        ConnectionManager::getInstance().sendDataToAllConnections(dummy); }));

    registerTask(new Task("ss_tput", "Single-sided throughput test", []() -> void {
        config_t config = {.dev_name = "mlx5_0",
                           .server_name = "141.76.47.9",
                           .tcp_port = 20000,
                           .client_mode = false,
                           .ib_port = 1,
                           .gid_idx = 0};

        for (size_t num_rb = 2; num_rb < 5; ++num_rb) {
            auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::stringstream logNameStream;
            logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "ss_tput.log";
            std::string logName = logNameStream.str();
            std::cout << "[Task] Set name: " << logName << std::endl;

            for (std::size_t bytes = 1ull << 10; bytes < 1ull << 32; bytes <<= 1) {
                buffer_config_t bufferConfig = {.num_own_receive = 0,
                                                .size_own_receive = 0,
                                                .num_remote_receive = num_rb,
                                                .size_remote_receive = bytes,
                                                .size_own_send = bytes * num_rb,
                                                .size_remote_send = 0};

                CHECK(ConnectionManager::getInstance().openConnection("ss_tput", config, bufferConfig));

                std::cout << "[main] Opened connection with id 'ss_tput' and size for one receive: " << GetBytesReadable(bytes) << std::endl;
                std::cout << std::endl
                          << "Single-sided throughput test." << std::endl;

                CHECK(ConnectionManager::getInstance().throughputTest("ss_tput", logName));

                std::cout << std::endl
                          << "Single-sided throughput test ended." << std::endl;

                CHECK(ConnectionManager::getInstance().closeConnection("ss_tput"));
            }
        }
    }));
}

void TaskManager::setGlobalAbortFunction(std::function<void()> fn) {
    globalAbort = fn;
}