#include "ConnectionManager.h"

#include <iostream>

#include "Connection.hpp"

ConnectionManager::ConnectionManager() : globalConnectionId{0} {
}

ConnectionManager::~ConnectionManager() {
    stop(false);
}

int ConnectionManager::registerConnection(config_t &config, buffer_config_t &bufferConfig) {
    do {
        ++globalConnectionId;
    } while (connections.contains(globalConnectionId));

    if (configuration->get<uint8_t>(MEMO_DEFAULT_CONNECTION_TYPE) == (uint8_t)ConnectionType::PushConnection) {
        connections.insert(std::make_pair(globalConnectionId, std::make_shared<ConnectionPush>(config, bufferConfig, globalConnectionId)));
    } else if (configuration->get<uint8_t>(MEMO_DEFAULT_CONNECTION_TYPE) == (uint8_t)ConnectionType::PullConnection) {
        connections.insert(std::make_pair(globalConnectionId, std::make_shared<ConnectionPull>(config, bufferConfig, globalConnectionId)));
    } else {
        LOG_ERROR("There was a wrong connection type provided! (" << +configuration->get<uint8_t>(MEMO_DEFAULT_CONNECTION_TYPE) << ")" << std::endl);
        return 0;
    }

    return globalConnectionId;
}

std::shared_ptr<Connection> ConnectionManager::getConnectionById(size_t id) {
    if (connections.contains(id)) {
        return connections[id];
    }
    return nullptr;
}

bool ConnectionManager::registerCallback(uint8_t code, CallbackFunction cb) {
    if (callbacks.contains(code)) {
        return false;
    }
    callbacks.insert({code, cb});

    return true;
}

bool ConnectionManager::hasCallback(uint8_t code) const {
    return callbacks.contains(code);
}

CallbackFunction ConnectionManager::getCallback(uint8_t code) const {
    if (callbacks.contains(code)) {
        return callbacks.at(code);
    } else {
        std::cout << "There was no callback found for " << +code << std::endl;
        return CallbackFunction();
    }
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

int ConnectionManager::closeAllConnections(bool remoteShutdown) {
    std::size_t success = 0;
    std::size_t allSuccess = 0;

    for (auto &[name, con] : connections) {
        success = con->closeConnection(remoteShutdown);
        if (success == 0)
            std::cout << "Connection '" << name << "' was successfully closed." << std::endl;
        else
            std::cout << "Something went wrong while closing connection '" << name << "'!" << std::endl;
        allSuccess += success;
    }

    if (connections.size() && allSuccess == 0) {
        std::cout << "All Connections were successfully closed!" << std::endl;
        connections.clear();
    } else {
        return 1;
    }

    return allSuccess;
}

int ConnectionManager::sendData(std::size_t connectionId, char *data, std::size_t dataSize, char *customMetaData, std::size_t customMetaDataSize, uint8_t opcode) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->sendData(data, dataSize, customMetaData, customMetaDataSize, opcode);
    } else {
        std::cout << "The Connection you wanted to use was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::sendOpCode(std::size_t connectionId, uint8_t opcode, bool sendToRemote) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->sendOpcode(opcode, sendToRemote);
    } else {
        std::cout << "The Connection you wanted to use was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

int ConnectionManager::sendCustomOpcodeToAllConnections(uint8_t code) {
    for (auto const &[name, con] : connections) {
        con->sendOpcode(code, true);
    }

    std::cout << "Sent opcode " << (uint64_t)code << " to all connections." << std::endl;

    return 0;
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

int ConnectionManager::benchmark(std::size_t connectionId, std::string shortName, std::string name, BenchmarkType benchType) {
    if (connections.contains(connectionId)) {
        return connections[connectionId]->benchmark(shortName, name, benchType);
    } else {
        std::cout << "The Connection was not found. Please be sure to use the correct ID!" << std::endl;
    }

    return 1;
}

void ConnectionManager::stop(bool remoteShutdown) {
    if (!stopped) {
        closeAllConnections(remoteShutdown);
    }
}

bool ConnectionManager::abortSignaled() const {
    return globalAbort;
}