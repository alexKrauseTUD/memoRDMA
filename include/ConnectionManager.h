#ifndef MEMORDMA_RDMA_CONNECTION_MANAGER
#define MEMORDMA_RDMA_CONNECTION_MANAGER

#include <map>

#include "Configuration.h"
#include "Connection.hpp"

typedef std::function<void(const size_t, const ReceiveBuffer *, const std::_Bind<ResetFunction(uint64_t)>)> CallbackFunction;

class ConnectionManager {
   public:
    static ConnectionManager &getInstance() {
        static ConnectionManager instance;
        if (!instance.configuration) {
            instance.configuration = std::make_unique<Configuration>();
        }
        return instance;
    }
    ~ConnectionManager();

    ConnectionManager(ConnectionManager const &) = delete;
    void operator=(ConnectionManager const &) = delete;

    int registerConnection(config_t &config, buffer_config_t &bufferConfig);
    std::shared_ptr<ConnectionPush> getConnectionById(size_t id);
    bool registerCallback(uint8_t code, CallbackFunction cb);
    bool hasCallback(uint8_t code) const;
    CallbackFunction getCallback(uint8_t code) const;
    void printConnections();
    int closeConnection(std::size_t connectionId, bool sendRemote = true);
    int closeAllConnections(bool remoteShutdown);
    int sendData(std::size_t connectionId, char *data, std::size_t dataSize, char *customMetaData, std::size_t customMetaDataSize, uint8_t opcode);
    int sendOpCode(std::size_t connectionId, uint8_t opcode, bool sendToRemote);
    int sendCustomOpcodeToAllConnections(uint8_t code);

    int reconfigureBuffer(std::size_t connectionId, buffer_config_t &bufferConfig);

    int benchmark(std::size_t connectionId, std::string shortName, std::string name, BenchmarkType benchType);

    void stop(bool remoteShutdown);
    bool abortSignaled() const;

    std::unique_ptr<Configuration> configuration;

   private:
    /* Singleton-required */
    ConnectionManager();

    std::map<std::size_t, std::shared_ptr<ConnectionPush>> connections;

    bool globalAbort;
    bool stopped = false;

    std::size_t globalConnectionId;

    std::map<uint8_t, CallbackFunction> callbacks;
};

#endif  // MEMORDMA_RDMA_CONNECTION_MANAGER