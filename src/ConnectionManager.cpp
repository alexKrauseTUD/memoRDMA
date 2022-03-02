#include "ConnectionManager.h"

#include "Connection.h"

ConnectionManager::ConnectionManager() {
    monitor_connection = [this](bool *abort) -> void {
        using namespace std::chrono_literals;

        std::cout << "Starting monitoring thread for connections!" << std::flush;

        while (!*abort) {
            std::this_thread::sleep_for(100ms);
            for (auto const &[name, con] : connections) {
                switch (con->conStat) {
                    case closing:
                        closeConnection(name, false);
                        break;
                    case reinitialize:
                        con->init(true);
                        break;
                    case mt_consume:
                        con->consumeMultiThread(true);
                        break;
                    case next_mt_consume:
                        con->init(true);
                        con->consumeMultiThread();
                        break;
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

int ConnectionManager::openConnection(std::string connectionName, config_t &config, buffer_config_t &bufferConfig) {
    if (!connections.contains(connectionName)) {
        connections.insert(std::make_pair(connectionName, new Connection(config, bufferConfig)));

        return 0;
    } else {
        std::cout << "There is already a connection with the name '" << connectionName << "'! Please use another one!" << std::endl;
    }

    return 1;
}

int ConnectionManager::receiveConnection(std::string connectionName, config_t &config) {
    if (!connections.contains(connectionName)) {
        buffer_config_t bufferConfig;
        connections.insert(std::make_pair(connectionName, new Connection(config, bufferConfig)));

        return 0;
    } else {
        std::cout << "There is already a connection with the name '" << connectionName << "'! Please use another one!" << std::endl;
    }

    return 1;
}

void ConnectionManager::printConnections() {
    for (auto const &[name, con] : connections) {
        std::cout << name << ':' << con->res.sock << std::endl;
    }
}

int ConnectionManager::closeConnection(std::string connectionName, bool sendRemote) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection you wanted to close was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        auto con = connections[connectionName];
        connections.erase(connectionName);
        return con->closeConnection(sendRemote);
    }

    return 1;
}

int ConnectionManager::closeAllConnections() {
    int success = 0;
    for (auto &[name, con] : connections) {
        success += con->closeConnection();
        if (success == 0)
            std::cout << "Connection '" << name << "' was successfully closed." << std::endl;
    }

    connections.clear();

    return success;
}

// TODO: How about a pointer to the data;; Generic datatype?
int ConnectionManager::sendData(std::string connectionName, std::string &data) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection you wanted to use was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->sendData(data);
    }

    return 1;
}

// TODO: How about a pointer to the data;; Generic datatype?
int ConnectionManager::sendDataToAllConnections(std::string &data) {
    int success = 0;

    for (auto const &[name, con] : connections) {
        success += sendData(name, data);
    }

    if (success == 0)
        std::cout << "The data was successfully broadcasted to all connections!" << std::endl;
    else
        std::cout << "Something went wrong when trying to broadcast the data to all connections!" << std::endl;

    return success;
}

int ConnectionManager::addReceiveBuffer(std::string connectionName, uint8_t quantity = 1) {
    // TODO: sanity check
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->addReceiveBuffer(quantity);
    }

    return 1;
}

int ConnectionManager::removeReceiveBuffer(std::string connectionName, uint8_t quantity = 1) {
    // TODO: sanity check
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->removeReceiveBuffer(quantity);
    }

    return 1;
}

int ConnectionManager::resizeReceiveBuffer(std::string connectionName, std::size_t newSize) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->resizeReceiveBuffer(newSize);
    }

    return 1;
}

int ConnectionManager::resizeSendBuffer(std::string connectionName, std::size_t newSize) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->resizeSendBuffer(newSize);
    }

    return 1;
}

int ConnectionManager::pendingBufferCreation(std::string connectionName) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->pendingBufferCreation();
    }

    return 0;
}

void ConnectionManager::stop() {
    closeAllConnections();
    globalAbort = true;
    monitorWorker->join();
}

bool ConnectionManager::abortSignaled() const {
    return globalAbort;
}

int ConnectionManager::throughputTest(std::string connectionName, std::string logName) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->throughputTest(logName);
    }

    return 0;
}

int ConnectionManager::consumingTest(std::string connectionName, std::string logName) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->consumingTest(logName);
    }

    return 0;
}

int ConnectionManager::throughputTestMultiThread(std::string connectionName, std::string logName) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->throughputTestMultiThread(logName);
    }

    return 0;
}

int ConnectionManager::consumingTestMultiThread(std::string connectionName, std::string logName) {
    if (!connections.contains(connectionName)) {
        std::cout << "The Connection was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName]->consumingTestMultiThread(logName);
    }

    return 0;
}
