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

    int openConnection(std::string connectionName, config_t &config, buffer_config_t &bufferConfig);
    void printConnections();
    int closeConnection(std::string connectionName);
    int closeAllConnections();
    int sendData(std::string connectionName, std::string &data);
    int sendDataToAllConnections(std::string &data);
    int addReceiveBuffer(std::string connectionName, uint8_t quantity);
    int removeReceiveBuffer(std::string connectionName, uint8_t quantity);
    int resizeReceiveBuffer(std::string connectionName, std::size_t newSize);
    int resizeSendBuffer(std::string connectionName, std::size_t newSize);
    int receiveConnection(std::string connectionName, config_t &config);

    int throughputTest(std::string connectionName, std::string logName);
    int consumingTest(std::string connectionName, std::string logName);

    std::map<std::string, Connection*> connections;

    int pendingBufferCreation(std::string connectionName);

    void stop();
    bool abortSignaled() const;

   private:
    /* Singleton-required */
    ConnectionManager();

    bool globalAbort;
};

#endif  // MEMORDMA_RDMA_CONNECTION_MANAGER