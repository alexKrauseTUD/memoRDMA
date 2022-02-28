#ifndef MEMORDMA_RDMA_CONNECTION_MANAGER
#define MEMORDMA_RDMA_CONNECTION_MANAGER

#include <map>

#include "Connection.h"

class ConnectionManager {
   public:
    static ConnectionManager &getInstance() {
        static ConnectionManager instance;
        return instance;
    }
    ~ConnectionManager();

    ConnectionManager(ConnectionManager const &) = delete;
    void operator=(ConnectionManager const &) = delete;

    bool openConnection(std::string connectionName, config_t &config, buffer_config_t &bufferConfig);
    void printConnections();
    bool closeConnection(std::string connectionName);
    bool closeAllConnections();
    bool sendData(std::string connectionName, std::string &data);
    bool sendDataToAllConnections(std::string &data);
    bool addReceiveBuffer(std::string connectionName, uint8_t quantity);
    bool removeReceiveBuffer(std::string connectionName, uint8_t quantity);
    bool resizeReceiveBuffer(std::string connectionName, std::size_t newSize);
    bool resizeSendBuffer(std::string connectionName, std::size_t newSize);
    bool receiveConnection(std::string connectionName, config_t &config);

    bool throughputTest(std::string connectionName, std::string logName);

    std::map<std::string, Connection*> connections;

    bool pendingBufferCreation(std::string connectionName);

    void stop();
    bool abortSignaled() const;

   private:
    /* Singleton-required */
    ConnectionManager();

    bool globalAbort;
};

#endif  // MEMORDMA_RDMA_CONNECTION_MANAGER