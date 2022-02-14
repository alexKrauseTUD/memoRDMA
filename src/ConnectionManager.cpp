#include "ConnectionManager.h"

#include "Connection.h"

ConnectionManager::ConnectionManager() {}

ConnectionManager::~ConnectionManager() {
    stop();
    closeAllConnections();
}

bool ConnectionManager::openConnection(std::string connectionName, config_t &config, buffer_config_t &bufferConfig) {
    if (connections.find(connectionName) == connections.end()) {
        Connection con(config, bufferConfig);

        connections.insert(std::pair<std::string, Connection>(connectionName, con));

        return true;
    } else {
        std::cout << "There is already a connection with the name '" << connectionName << "'! Please use another one!" << std::endl;
    }

    return false;
}

void ConnectionManager::printConnections() {
    for (auto const &[name, con] : connections) {
        std::cout << name << ':' << con.res.sock << std::endl;
    }
}

bool ConnectionManager::closeConnection(std::string connectionName) {
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection you wanted to close was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].close();
    }

    return false;
}

bool ConnectionManager::closeAllConnections() {
    bool success = true;
    for (auto &[name, con] : connections) {
        success = success && con.close();
        if (success)
            std::cout << "Connection '" << name << "' was successfully closed." << std::endl;
    }

    return success;
}

// TODO: How about a pointer to the data;; Generic datatype?
bool ConnectionManager::sendData(std::string connectionName, std::string &data) {
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection you wanted to use was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].sendData(data);
    }

    return false;
}

// TODO: How about a pointer to the data;; Generic datatype?
bool ConnectionManager::sendDataToAllConnections(std::string &data) {
    bool success = true;

    for (auto const &[name, con] : connections) {
        success = success && sendData(name, data);
    }

    if (success)
        std::cout << "The data was successfully broadcasted to all connections!" << std::endl;
    else
        std::cout << "Something went wrong when trying to broadcast the data to all connections!" << std::endl;

    return success;
}

bool ConnectionManager::addReceiveBuffer(std::string connectionName, uint8_t quantity = 1) {
    // TODO: sanity check
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].addReceiveBuffer(quantity);
    }

    return false;
}

bool ConnectionManager::removeReceiveBuffer(std::string connectionName, uint8_t quantity = 1) {
    // TODO: sanity check// TODO: sanity check
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].removeReceiveBuffer(quantity);
    }

    return false;
}

bool ConnectionManager::resizeReceiveBuffer(std::string connectionName, std::size_t newSize) {
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].resizeReceiveBuffer(newSize);
    }

    return false;
}

bool ConnectionManager::resizeSendBuffer(std::string connectionName, std::size_t newSize) {
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection you wanted to change was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].resizeSendBuffer(newSize);
    }

    return false;
}

bool ConnectionManager::pendingBufferCreation(std::string connectionName) {
    if (connections.find(connectionName) == connections.end()) {
        std::cout << "The Connection was not found. Please be sure to use the correct name!" << std::endl;
    } else {
        return connections[connectionName].pendingBufferCreation();
    }

    return true;
}

void ConnectionManager::stop() {
    globalAbort = true;
}

bool ConnectionManager::abortSignaled() const {
    return globalAbort;
}
