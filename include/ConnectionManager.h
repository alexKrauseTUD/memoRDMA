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

    int registerConnection(config_t &config, buffer_config_t &bufferConfig);
    void printConnections();
    int closeConnection(std::size_t connectionId, bool sendRemote = true);
    int closeAllConnections();
    int sendData(std::size_t connectionId, std::string &data);
    int sendDataToAllConnections(std::string &data);
    int addReceiveBuffer(std::size_t connectionId, uint8_t quantity);
    int removeReceiveBuffer(std::size_t connectionId, uint8_t quantity);
    int resizeReceiveBuffer(std::size_t connectionId, std::size_t newSize);
    int resizeSendBuffer(std::size_t connectionId, std::size_t newSize);

    int reconfigureBuffer(std::size_t connectionId, buffer_config_t &bufferConfig);

    int throughputTest(std::size_t connectionId, std::string logName);
    int consumingTest(std::size_t connectionId, std::string logName);
    int throughputTestMultiThread(std::size_t connectionId, std::string logName);
    int consumingTestMultiThread(std::size_t connectionId, std::string logName);

    std::map<std::size_t, Connection *> connections;

    int pendingBufferCreation(std::size_t connectionId);

    void stop();
    bool abortSignaled() const;

   private:
    /* Singleton-required */
    ConnectionManager();

    bool globalAbort;

    std::function<void(bool *)> monitor_connection;

    std::thread *monitorWorker;

    std::size_t globalConnectionId;
};

#endif  // MEMORDMA_RDMA_CONNECTION_MANAGER