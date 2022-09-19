#ifndef MEMORDMA_RDMA_CONNECTION_MANAGER
#define MEMORDMA_RDMA_CONNECTION_MANAGER

#include <map>

#include "Connection.h"
#include "Configuration.h"

typedef std::function<void(const size_t, const ReceiveBuffer*, const std::_Bind<ResetFunction (uint64_t)>)> CallbackFunction;

class ConnectionManager {
   public:
    static ConnectionManager &getInstance() {
        static ConnectionManager instance;
        if ( !instance.configuration ) {
            instance.configuration = std::make_unique<Configuration>();
        }
        return instance;
    }
    ~ConnectionManager();

    ConnectionManager(ConnectionManager const &) = delete;
    void operator=(ConnectionManager const &) = delete;

    int registerConnection(config_t &config, buffer_config_t &bufferConfig);
    std::shared_ptr<Connection> getConnectionById( size_t id );
    bool registerCallback( uint8_t code, CallbackFunction cb );
    bool hasCallback( uint8_t code ) const;
    CallbackFunction getCallback( uint8_t code ) const;
    void printConnections();
    int closeConnection(std::size_t connectionId, bool sendRemote = true);
    int closeAllConnections();
    int sendData(std::size_t connectionId, char* data, std::size_t dataSize, char* customMetaData, std::size_t customMetaDataSize, uint8_t opcode, Strategies strat);
    int sendOpCode(std::size_t connectionId, uint8_t opcode);
    int sendCustomOpcodeToAllConnections( uint8_t code );

    int addReceiveBuffer(std::size_t connectionId, std::size_t quantity, bool own);
    int removeReceiveBuffer(std::size_t connectionId, std::size_t quantity, bool own);
    int resizeReceiveBuffer(std::size_t connectionId, std::size_t newSize, bool own);
    int resizeSendBuffer(std::size_t connectionId, std::size_t newSize, bool own);

    int reconfigureBuffer(std::size_t connectionId, buffer_config_t &bufferConfig);

    int throughputBenchmark(std::size_t connectionId, std::string logName, Strategies strat);
    int consumingBenchmark(std::size_t connectionId, std::string logName, Strategies strat);

    void stop();
    bool abortSignaled() const;

    std::unique_ptr<Configuration> configuration;

   private:
    /* Singleton-required */
    ConnectionManager();

    std::map<std::size_t, std::shared_ptr<Connection>> connections;

    bool globalAbort;
    bool stopped = false;

    std::size_t globalConnectionId;

    std::map< uint8_t, CallbackFunction > callbacks;
};

#endif  // MEMORDMA_RDMA_CONNECTION_MANAGER