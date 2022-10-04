/*
 * Configuration.h
 *
 *  Created on: Jan 30, 2014
 *      Author: kiefer
 */

#ifndef MEMORDMA_CONFIGURATION_H_
#define MEMORDMA_CONFIGURATION_H_

#include <map>
#include <string>

// Configuration Options
#define MEMO_CONFIG_FILE "memo.conf"
#define MEMO_CONFIG_EXAMPLE_FILE "memo.conf.default"

#define MEMO_DEFAULT_LOGGER_LEVEL "logger.level"
#define MEMO_DEFAULT_LOGGER_FILENAME "logger.logfile"
#define MEMO_DEFAULT_LOGGER_LOG_TO_FILE "logger.tofile"
#define MEMO_DEFAULT_LOGGER_TIMEFORMAT "logger.timeformat"
#define MEMO_DEFAULT_LOGGER_COLOR_ENABLE "logger.color.enable"
#define MEMO_DEFAULT_LOGGER_COLOR_FATAL "logger.color.fatal"
#define MEMO_DEFAULT_LOGGER_COLOR_ERROR "logger.color.error"
#define MEMO_DEFAULT_LOGGER_COLOR_CONSOLE "logger.color.console"
#define MEMO_DEFAULT_LOGGER_COLOR_WARNING "logger.color.warning"
#define MEMO_DEFAULT_LOGGER_COLOR_INFO "logger.color.info"
#define MEMO_DEFAULT_LOGGER_COLOR_SUCCESS "logger.color.success"
#define MEMO_DEFAULT_LOGGER_COLOR_DEBUG1 "logger.color.debug1"
#define MEMO_DEFAULT_LOGGER_COLOR_DEBUG2 "logger.color.debug2"

#define MEMO_DEFAULT_OWN_SEND_THREADS "memo.default.own.sender"
#define MEMO_DEFAULT_OWN_RECEIVE_THREADS "memo.default.own.receiver"

#define MEMO_DEFAULT_REMOTE_SEND_THREADS "memo.default.remote.sender"
#define MEMO_DEFAULT_REMOTE_RECEIVE_THREADS "memo.default.remote.receiver"

#define MEMO_DEFAULT_OWN_SEND_BUFFER_COUNT "memo.default.own.sb.count"
#define MEMO_DEFAULT_OWN_RECEIVE_BUFFER_COUNT "memo.default.own.rb.count"

#define MEMO_DEFAULT_REMOTE_SEND_BUFFER_COUNT "memo.default.remote.sb.count"
#define MEMO_DEFAULT_REMOTE_RECEIVE_BUFFER_COUNT "memo.default.remote.rb.count"

#define MEMO_DEFAULT_OWN_SEND_BUFFER_SIZE "memo.default.own.sb.size"
#define MEMO_DEFAULT_OWN_RECEIVE_BUFFER_SIZE "memo.default.own.rb.size"

#define MEMO_DEFAULT_REMOTE_SEND_BUFFER_SIZE "memo.default.remote.sb.size"
#define MEMO_DEFAULT_REMOTE_RECEIVE_BUFFER_SIZE "memo.default.remote.rb.size"

#define MEMO_DEFAULT_META_INFO_SIZE "memo.default.metainfo.size"

#define MEMO_DEFAULT_OPERATION_MODE "memo.default.operation.mode"

#define MEMO_DEFAULT_TCP_PORT "memo.default.tcp.port"
#define MEMO_DEFAULT_IB_DEVICE_NAME "memo.default.ibv.dev"
#define MEMO_DEFAULT_IB_PORT "memo.default.ibv.port"
#define MEMO_DEFAULT_IB_GLOBAL_INDEX "memo.default.ibv.gidx"

#define MEMO_DEFAULT_CONNECTION_AUTO_LISTEN "memo.default.autoconnect.listen"
#define MEMO_DEFAULT_CONNECTION_AUTO_INITIATE "memo.default.autoconnect.initiate"
#define MEMO_DEFAULT_CONNECTION_AUTO_LISTEN_IP "memo.default.autoconnect.listen.ip"
#define MEMO_DEFAULT_CONNECTION_AUTO_INITIATE_IP "memo.default.autoconnect.initiate.ip"

#include <cstdint>
#include <sstream>

class Configuration {
   public:
    Configuration();
    virtual ~Configuration();
    void add(int argc, char* argv[]);
    void addDefault(std::string key, std::string value, std::string description = "");
    void setDescription(std::string key, std::string description);
    bool writeConfiguration(std::string filename, bool writeDescriptions = true, bool writeDefaults = false, bool writeChangedOnly = false);

    bool exists(std::string key);

    template <class T>
    T get(std::string key, bool* exists = nullptr) {
        if (exists != nullptr) {
            *exists = this->exists(key);
        }
        T result;
        if (this->exists(key)) {
            std::stringstream(conf[key]) >> result;
        } else if (defaults.count(key) > 0) {
            std::stringstream(defaults[key]) >> result;
        }
        return result;
    }

    bool isSet(std::string key, bool* exists = nullptr);

    std::string getAsString(std::string key, bool* exists = nullptr, bool lowercase = false);
    std::string get(std::string key, bool* exists = nullptr);
    // void print(QueryAnswerPrinter* channel, bool all = false);

    std::map<std::string, std::string> conf;
    std::map<std::string, std::string> defaults;
    std::map<std::string, std::string> descriptions;

    bool readFromFile(std::string filename);
    void insert(std::string key, std::string value);
};

#endif /* MEMORDMA_CONFIGURATION_H_ */
