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

void TaskManager::executeByIdent(std::string name) {
}

void TaskManager::setup() {
    registerTask(new Task("openConnection", "Open Connection", []() -> void {
        std::string devName = NULL;

        std::cout << "Please enter the IB-Device-Name!" << std::endl;
        std::cin >> devName;

        std::string serverName = NULL;

        uint32_t tcpPort = 20000;

        std::cout << "Please enter the TCP-Port that you want to use (if already used, another one is selected automatically)!" << std::endl;
        std::cin >> tcpPort;

        bool correctInput;
        bool clientMode;
        std::string inp;

        do
        {
            std::cout << "Is this the client ('y' / 'yes') or the server ('n' / 'no')?" << std::endl;
            std::getline(std::cin, inp);

            if (inp.compare("y") || inp.compare("yes")) {
                correctInput = true;
                clientMode = true;
            } else if (inp.compare("n") || inp.compare("no")) {
                correctInput = true;
                clientMode = false;
            } else {
                std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('y' / 'yes' / 'n' / 'no')!" << std::endl;
                correctInput = false;
            }

        } while (!correctInput);

        if (clientMode) {
            std::cout << "Please enter the Server-Name!" << std::endl;
            std::cin >> serverName;
        }
        
        int ibPort = 1;

        do
        {
            std::cout << "Please enter the IB-Port that you want to use!" << std::endl;
            std::cin >> ibPort;

            if (ibPort < 0) 
                std::cout << "[Error] The provided IB-Port was incorrect! Please enter the correct number!" << std::endl;

        } while (ibPort < 0);
        

        int gidIndex = -1;

        do
        {
            std::cout << "Please enter the GID-Index that you want to use!" << std::endl;
            std::cin >> gidIndex;

            if (gidIndex < 0) 
                std::cout << "[Error] The provided GID-Index was incorrect! Please enter the correct number!" << std::endl;

        } while (gidIndex < 0);

        config_t config = {.dev_name = devName,
                          .server_name = serverName,
                          .tcp_port = tcpPort ? tcpPort : 20000,
                          .client_mode = clientMode,
                          .ib_port = ibPort,
                          .gid_idx = gidIndex
                        };

        bool correctInput2;
        bool confBuffer;
        std::string inp2;

        do
        {
            std::cout << "Do you want to configure the buffer quantity and size ('y' / 'yes') or use the default configuration ('n' / 'no')?" << std::endl;
            std::getline(std::cin, inp2);

            if (inp2.compare("y") || inp2.compare("yes")) {
                correctInput2 = true;
                confBuffer = true;
            } else if (inp2.compare("n") || inp2.compare("no")) {
                correctInput2 = true;
                confBuffer = false;
            } else {
                std::cout << "[Error] Your input was not interpretable! Please enter one of the given possibilities ('y' / 'yes' / 'n' / 'no')!" << std::endl;
                correctInput = false;
            }

        } while (!correctInput2);

        unsigned int numOwnReceive = 1;
        std::size_t sizeOwnReceive = 1024 * 1024 * 2 + 128;
        unsigned int numRemoteReceive = 1;
        std::size_t sizeRemoteReceive = 1024 * 1024 * 2 + 128;
        std::size_t sizeOwnSend = 1024 * 1024 * 8 + 4 * 128;
        std::size_t sizeRemoteSend = 1024 * 1024 * 8 + 4 * 128;

        if (confBuffer) {
            do
            {
                std::cout << "Please enter the number of own Receive-Buffers that you want to create!" << std::endl;
                std::cin >> numOwnReceive;

                if (numOwnReceive < 1) 
                    std::cout << "[Error] The provided number of own Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

            } while (numOwnReceive < 1);

            do
            {
                std::cout << "Please enter the size of own Receive-Buffers that you want to create!" << std::endl;
                std::cin >> sizeOwnReceive;

                if (sizeOwnReceive < 1) 
                    std::cout << "[Error] The provided size of own Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

            } while (sizeOwnReceive < 1);

            do
            {
                std::cout << "Please enter the number of remote Receive-Buffers that you want to create!" << std::endl;
                std::cin >> numRemoteReceive;

                if (numRemoteReceive < 1) 
                    std::cout << "[Error] The provided number of remote Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

            } while (numRemoteReceive < 1);

            do
            {
                std::cout << "Please enter the size of remote Receive-Buffers that you want to create!" << std::endl;
                std::cin >> sizeRemoteReceive;

                if (sizeRemoteReceive < 1) 
                    std::cout << "[Error] The provided size of remote Receive-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

            } while (sizeRemoteReceive < 1);

            do
            {
                std::cout << "Please enter the size of own Send-Buffer that you want to create!" << std::endl;
                std::cin >> sizeOwnSend;

                if (sizeOwnSend < 1) 
                    std::cout << "[Error] The provided size of own Send-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

            } while (sizeOwnSend < 1);

            do
            {
                std::cout << "Please enter the size of remote Send-Buffer that you want to create!" << std::endl;
                std::cin >> sizeRemoteSend;

                if (sizeRemoteSend < 1) 
                    std::cout << "[Error] The provided size of remote Send-Buffers was incorrect! Please enter the correct number (>0)!" << std::endl;

            } while (sizeRemoteSend < 1);

        }

        buffer_config_t bufferConfig = {.num_own_receive = numOwnReceive,
                          .size_own_receive = sizeOwnReceive,
                          .num_remote_receive = numRemoteReceive,
                          .size_remote_receive = sizeRemoteReceive,
                          .size_own_send = sizeOwnSend,
                          .size_remote_send = sizeRemoteSend
                        };
        
        std::string connectionName;

        std::cout << "Please enter a name for the connection that you want to create! (Numerical suggested)" << std::endl;
        std::getline(std::cin, connectionName);


        if (ConnectionManager::getInstance().openConnection(connectionName, config, bufferConfig)) {
            std::cout << "[Success] Connection opened for config: " << std::endl;
            print_config(config);
        } else {
            std::cout << "[Error] Something went wrong! The connection could not be opened for config: " << std::endl;
        }
        print_config(config); }));

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
}

void TaskManager::setGlobalAbortFunction(std::function<void()> fn) {
    globalAbort = fn;
}
