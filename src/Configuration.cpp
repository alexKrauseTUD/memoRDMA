/*
 * Configuration.cpp
 *
 *  Created on: Jan 30, 2014
 *      Author: kiefer
 */

#include "Configuration.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "Logger.h"
#include "Utility.h"

using namespace memordma;

template <>
uint8_t Configuration::get(std::string key, bool* exists) {
    if (exists != nullptr) {
        *exists = this->exists(key);
    }
    uint8_t result;
    std::map<std::string, std::string> container;
    if (this->exists(key)) {
        container = conf;
    } else if (defaults.count(key) > 0) {
        container = defaults;
    }

    uint16_t temp;
    std::stringstream(container[key]) >> temp;
    constexpr auto max = std::numeric_limits<uint8_t>::max();
    result = std::min(temp, (uint16_t)max);

    return result;
}

Configuration::Configuration() {
    addDefault(MEMO_DEFAULT_LOGGER_LEVEL, "DEBUG2", "Control log level\n[FATAL, ERROR, CONSOLE, WARNING, INFO, SUCCESS, DEBUG1, DEBUG2]");
    addDefault(MEMO_DEFAULT_LOGGER_FILENAME, "default.log", "The filename, where every valid logger output is mirrored. New data is always appended to an existing file.");
    addDefault(MEMO_DEFAULT_LOGGER_LOG_TO_FILE, "0", "Mirror the output of the logger to a file.");
    addDefault(MEMO_DEFAULT_LOGGER_TIMEFORMAT, "%m/%d %H:%M:%S", "Time format for logger timestamp. see strftime().");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_ENABLE, "0", "Enable color support for Logger");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_FATAL, "red", "Default color for fatal logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_ERROR, "red", "Default color for error logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_CONSOLE, "yellow", "Default color for console logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_WARNING, "brown", "Default color for warning logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_INFO, "light_green", "Default color for info logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_SUCCESS, "green", "Default color for success logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_DEBUG1, "blue", "Default color for debug1 logs");
    addDefault(MEMO_DEFAULT_LOGGER_COLOR_DEBUG2, "blue", "Default color for debug2 logs");

    addDefault(MEMO_DEFAULT_OWN_SEND_THREADS, "2", "How many threads are used to copy from local sendbuffers to remote receive buffers.");
    addDefault(MEMO_DEFAULT_OWN_RECEIVE_THREADS, "2", "How many threads are used to observe local receive buffers.");

    addDefault(MEMO_DEFAULT_REMOTE_SEND_THREADS, "2", "Used for Reconfiguring the buffer config on the remote side: How many threads are used to copy from remote sendbuffers to local receive buffers.");
    addDefault(MEMO_DEFAULT_REMOTE_RECEIVE_THREADS, "2", "Used for Reconfiguring the buffer config on the remote side: How many threads are used to observe local receive buffers.");

    addDefault(MEMO_DEFAULT_OWN_SEND_BUFFER_COUNT, "2", "How many send buffers are created locally.");
    addDefault(MEMO_DEFAULT_OWN_RECEIVE_BUFFER_COUNT, "2", "How many receive buffers are created locally.");

    addDefault(MEMO_DEFAULT_REMOTE_SEND_BUFFER_COUNT, "2", "Used for Reconfiguring the buffer config on the remote side: How many send buffers are created remotely.");
    addDefault(MEMO_DEFAULT_REMOTE_RECEIVE_BUFFER_COUNT, "2", "Used for Reconfiguring the buffer config on the remote side: How many receive buffers are created remotely.");

    addDefault(MEMO_DEFAULT_OWN_SEND_BUFFER_SIZE, std::to_string(1024 * 512), "Size of local send buffers in Byte.");
    addDefault(MEMO_DEFAULT_OWN_RECEIVE_BUFFER_SIZE, std::to_string(1024 * 512), "Size of local receive buffers in Byte.");

    addDefault(MEMO_DEFAULT_REMOTE_SEND_BUFFER_SIZE, std::to_string(1024 * 512), "Used for Reconfiguring the buffer config on the remote side: Size of remote send buffers in Byte.");
    addDefault(MEMO_DEFAULT_REMOTE_RECEIVE_BUFFER_SIZE, std::to_string(1024 * 512), "Used for Reconfiguring the buffer config on the remote side: Size of remote receive buffers in Byte.");

    addDefault(MEMO_DEFAULT_META_INFO_SIZE, "16", "[Currently unchangeable] Size of the meta info array. Allows for maximum 8 receive buffers per server.");

    addDefault(MEMO_DEFAULT_TCP_PORT, "20000", "TCP Port to perform initial handshake and exchange RDMA Infos.");
    addDefault(MEMO_DEFAULT_IB_DEVICE_NAME, "mlx5_0", "The device name of the RDMA NIC.");
    addDefault(MEMO_DEFAULT_IB_PORT, "1", "The port on the RDMA NIC where the to-be-used infiniband cable is attached.");
    addDefault(MEMO_DEFAULT_IB_GLOBAL_INDEX, "0", "Global Index ID of the RDMA NIC to be used.");

    addDefault(MEMO_DEFAULT_CONNECTION_AUTO_LISTEN, "0", "Upon startup, immediately use the default TCP Port and listen to the default listen ip.");
    addDefault(MEMO_DEFAULT_CONNECTION_AUTO_INITIATE, "0", "Upon startup, immediately use the default TCP Port and initiate a connection to the default initiate ip.");
    addDefault(MEMO_DEFAULT_CONNECTION_AUTO_LISTEN_IP, "141.76.47.8", "haecBoehm");
    addDefault(MEMO_DEFAULT_CONNECTION_AUTO_INITIATE_IP, "141.76.47.9", "haecBoehmTheSecond");

    if (!readFromFile(MEMO_CONFIG_FILE)) {
        std::cout << "[Logger] Could not open standard configuration file ('" << MEMO_CONFIG_FILE << "') -- using default values." << std::endl;
    }
}

void Configuration::add(int argc, char* argv[]) {
    for (int i = 1; i < argc; i++) {  // skip argv[0], the executable
        std::string argument = argv[i];
        size_t pos = argument.find_first_of("=");
        if (argument[0] != '-') {  // assume config file and read params from there
            if (!readFromFile(argument)) {
                WARNING("Could not open configuration file ('" << argument << "')." << std::endl);
            }
        } else if (pos == std::string::npos) {  // assume boolean switch
            argument.erase(0, 1);
            insert(argument, "true");
        } else {  // assume key=value pair
            argument.erase(0, 1);
            std::string key = argument.substr(0, pos - 1);
            std::string value = argument.substr(pos, std::string::npos);
            insert(key, value);
        }
    }
}

void Configuration::addDefault(std::string key, std::string value, std::string description) {
    defaults[key] = value;
    if (description.size() > 0) {
        setDescription(key, description);
    }
}

void Configuration::setDescription(std::string key, std::string description) {
    if (description.size() < 1) {
        descriptions.erase(key);
        return;
    }
    descriptions[key] = description;
}

bool Configuration::writeConfiguration(std::string filename, bool writeDescriptions, bool writeDefaults, bool writeChangedOnly) {
    std::ofstream outfile;
    outfile.open(filename, std::ios::out | std::ios::trunc);
    if (!outfile.is_open()) {
        return false;
    }
    std::stringstream ss;
    ss << std::endl
       << "// ";

    std::vector<std::string> allkeys;
    for (auto def : defaults) {
        allkeys.push_back(def.first);
    }
    for (auto con : conf) {
        if (std::find(allkeys.begin(), allkeys.end(), con.first) != allkeys.end()) {
            continue;
        }
        allkeys.push_back(con.first);
    }
    for (auto des : descriptions) {
        if (std::find(allkeys.begin(), allkeys.end(), des.first) != allkeys.end()) {
            continue;
        }
        allkeys.push_back(des.first);
    }

    for (std::string key : allkeys) {
        bool keyDiffers = exists(key) && conf[key] != defaults[key];
        if (writeChangedOnly && !keyDiffers) {
            continue;
        }
        if (writeDescriptions) {
            std::string convstr = descriptions[key];

            // Get the first occurrence
            size_t pos = convstr.find('\n');
            // Repeat till end is reached
            while (pos != std::string::npos) {
                // Replace this occurrence of Sub String
                convstr.replace(pos, 1, "");
                // Get the next occurrence from the current position
                pos = convstr.find('\n', pos + 1);
            }

            outfile << std::endl;
            outfile << "// " << key << " [" << (defaults.count(key) ? defaults[key] : "<not set>") << "]" << std::endl;
            outfile << "// " << (descriptions.count(key) ? convstr : "<no description>") << std::endl;
        }

        if (!keyDiffers) {
            outfile << "#";
        }
        outfile << key << " = " << (writeDefaults || !conf.count(key) ? defaults[key] : conf[key]) << std::endl;
    }
    outfile.close();

    return true;
}

bool Configuration::exists(std::string key) {
    return conf.count(key);
}

bool Configuration::isSet(std::string key, bool* exists) {
    if (exists != nullptr) {
        *exists = this->exists(key);
    }
    if (this->exists(key)) {
        return conf[key] == "true";
    }
    return false;
}

std::string Configuration::getAsString(std::string key, bool* exists, bool lowercase) {
    if (exists != nullptr) {
        *exists = this->exists(key);
    }
    if (this->exists(key)) {
        if (lowercase) {
            std::string data(conf[key]);
            std::transform(data.begin(), data.end(), data.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            return data;
        }
        return conf[key];
    } else if (defaults.count(key) > 0) {
        if (lowercase) {
            std::string data(defaults[key]);
            std::transform(data.begin(), data.end(), data.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            return data;
        }
        return defaults[key];
    }
    return "";
}

std::string Configuration::get(std::string key, bool* exists) {
    return getAsString(key, exists);
}

void Configuration::insert(std::string key, std::string value) {
    // make lowercase, trim whitespaces and remove quotes
    std::transform(key.begin(), key.end(), key.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    Utility::trim(key);
    Utility::trim(value);
    Utility::trim_any_of(key, "\"\'");
    Utility::trim_any_of(value, "\"\'");

    if (key.length() < 1) {
        return;
    }

    conf[key] = value;

    //	DEBUG1("Adding to configuration(" << key << "=" << value << ")" <<endl );
}

bool Configuration::readFromFile(std::string filename) {
    std::ifstream file(filename);
    if (file.is_open()) {
        std::string line;
        while (getline(file, line)) {
            if ((line.size() > 0 && line[0] == '#') || (line.size() > 1 && line[0] == '/' && line[1] == '/')) {  // ignore comment
                continue;
            }
            size_t pos = line.find_first_of("=");
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1, std::string::npos);
            insert(key, value);
        }
        file.close();
    } else {
        return false;
    }
    return true;
}

// void Configuration::print(QueryAnswerPrinter* channel, bool all) {
// for (auto c : defaults) {
// std::stringstream ss;
// ss << c.first << ": ";
// if (conf.count(c.first) < 1) {
// if (!all) {
// continue;
// }
// ss << "<not set>";
// } else {
// ss << conf[c.first];
// }
// ss << " (" << c.second << ")";
// channel->printInfoLine(ss.str());
// }
// }

Configuration::~Configuration() {
}
