#include "ConnectionManager.h"

#include "Connection.h"

ConnectionManager::ConnectionManager() : globalConnectionId{0} {
    monitor_connection = [this](bool *abort) -> void {
        using namespace std::chrono_literals;

        std::cout << "Starting monitoring thread for connections!" << std::flush;

        while (!*abort) {
            std::this_thread::sleep_for(100ms);
            for (auto const &[name, con] : connections) {
                switch (con->conStat) {
                    case ConnectionStatus::closing: {
                        closeConnection(name, false);
                    } break;
                    case ConnectionStatus::multi_thread: {
                        con->workMultiThread();
                    } break;
                    default:
                        break;
                }
            }
        }
        std::cout << "[monitor_connection] Ending through global abort." << std::endl;
    };

    monitorWorker = new std::thread(monitor_connection, &globalAbort);
}

ConnectionManager::~ConnectionManager() {
    stop();
}

int ConnectionManager::registerConnection(config_t &config, buffer_config_t &bufferConfig) {
    do {
        ++globalConnectionId;
    } while (connections.contains(globalConnectionId));

    connections.insert(std::make_pair(globalConnectionId, new Connection(config, bufferConfig)));

    return globalConnectionId;
}

void ConnectionManager::printConnections() {
    for (auto const &[name, con] : connections) {
        std::cout << "Connection ID:\t\t" << name << std::endl;
        con->printConnectionInfo();
        std::cout << "\n"
                  << std::endl;
    }
}

int ConnectionManager::closeConnection(std::size_t connectionId, bool sendRemote) {
    if (connections.contains(connectionId)) {
        auto con = connections[connectionId];
        connections.erase(connectionId);
        return con->closeConnection(sendRemote);
    } else {
        std::cout << "The Connection you wanted to close was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::closeAllConnections() {
    std::size_t success = 0;
    std::size_t allSuccess = 0;

    for (auto &[name, con] : connections) {
        success = con->closeConnection();
        if (success == 0)
            std::cout << "Connection '" << name << "' was successfully closed." << std::endl;
        else
            std::cout << "Something went wrong while closing connection '" << name << "'!" << std::endl;
        allSuccess += success;
    }

    if (allSuccess == 0) {
        std::cout << "All Connections were successfully closed!" << std::endl;
        connections.clear();
    } else {
        return 1;
    }

    return allSuccess;
}

// TODO: How about a pointer to the data;; Generic datatype?
int ConnectionManager::sendData(std::size_t connectionId, std::string &data) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->sendData(data);
    } else {
        std::cout << "The Connection you wanted to use was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

// TODO: How about a pointer to the data;; Generic datatype?
int ConnectionManager::sendDataToAllConnections(std::string &data) {
    int success = 0;

    for (auto const &[name, con] : connections) {
        success += sendData(name, data);
    }

    if (success == 0) {
        std::cout << "The data was successfully broadcasted to all connections!" << std::endl;
    } else {
        std::cout << "Something went wrong when trying to broadcast the data to all connections!" << std::endl;
        success = 1;
    }

    return success;
}

int ConnectionManager::reconfigureBuffer(std::size_t connectionId, buffer_config_t &bufferConfig) {
    // TODO: sanity check
    if (connections.contains(connectionId)) {
        return connections[connectionId]->sendReconfigureBuffer(bufferConfig);
    } else {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::addReceiveBuffer(std::size_t connectionId, std::size_t quantity = 1, bool own = true) {
    // TODO: sanity check
    if (connections.contains(connectionId)) {
        return connections[connectionId]->addReceiveBuffer(quantity, own);
    } else {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::removeReceiveBuffer(std::size_t connectionId, std::size_t quantity = 1, bool own = true) {
    // TODO: sanity check
    if (connections.contains(connectionId)) {
        return connections[connectionId]->removeReceiveBuffer(quantity, own);
    } else {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::resizeReceiveBuffer(std::size_t connectionId, std::size_t newSize, bool own = true) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->resizeReceiveBuffer(newSize, own);
    } else {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::resizeSendBuffer(std::size_t connectionId, std::size_t newSize, bool own = true) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->resizeSendBuffer(newSize, own);
    } else {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::pendingBufferCreation(std::size_t connectionId) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->pendingBufferCreation();
    } else {
        std::cout << "The Connection was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::throughputTest(std::size_t connectionId, std::string logName, Strategies strat) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->throughputTest(logName, strat);
    } else {
        std::cout << "The Connection was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::consumingTest(std::size_t connectionId, std::string logName, Strategies strat) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->consumingTest(logName, strat);
    } else {
        std::cout << "The Connection was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::throughputTestMultiThread(std::size_t connectionId, std::string logName, Strategies strat) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->throughputTestMultiThread(logName, strat);
    } else {
        std::cout << "The Connection was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::consumingTestMultiThread(std::size_t connectionId, std::string logName, Strategies strat) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->consumingTestMultiThread(logName, strat);
    } else {
        std::cout << "The Connection was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

void ConnectionManager::stop() {
    closeAllConnections();
    globalAbort = true;
    monitorWorker->join();
}

bool ConnectionManager::abortSignaled() const {
    return globalAbort;
}