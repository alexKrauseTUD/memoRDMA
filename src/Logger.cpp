/*
 * Logger.cpp
 *
 *  Created on: 29.01.2014
 *      Author: tom
 */

#include "Logger.h"

#include <thread>
#include <iostream>

#include "Configuration.h"
#include "ConnectionManager.h"

namespace memordma {

thread_local std::shared_ptr<Logger> Logger::instance;

LogLevel Logger::logLevel = LogLevel::LOG_INFO;
std::string Logger::timeFormat = "%m/%d %H:%M:%S";
std::string Logger::logFileName = "";
std::ofstream Logger::logfile{};
bool Logger::colorEnabled = false;
bool Logger::logToFile = false;

thread_local std::unordered_map<LogColor, ColorCode, LogColorHash> Logger::colorCodes_ = {
    {LogColor::BLACK, {"black", "\033[30m"}},
    {LogColor::GREY, {"grey", "\033[1;30m"}},
    {LogColor::RED, {"red", "\033[31m"}},
    {LogColor::GREEN, {"green", "\033[32m"}},
    {LogColor::YELLOW, {"yellow", "\033[33m"}},
    {LogColor::BLUE, {"blue", "\033[34m"}},
    {LogColor::MAGENTA, {"magenta", "\033[35m"}},
    {LogColor::CYAN, {"cyan", "\033[36m"}},
    {LogColor::LIGHT_GRAY, {"light_gray", "\033[37m"}},
    {LogColor::DARK_GRAY, {"dark_gray", "\033[90m"}},
    {LogColor::LIGHT_RED, {"light_red", "\033[91m"}},
    {LogColor::LIGHT_GREEN, {"light_green", "\033[92m"}},
    {LogColor::LIGHT_YELLOW, {"light_yellow", "\033[93m"}},
    {LogColor::LIGHT_BLUE, {"light_blue", "\033[94m"}},
    {LogColor::LIGHT_MAGENTA, {"light_magenta", "\033[95m"}},
    {LogColor::LIGHT_CYAN, {"light_cyan", "\033[1;36m"}},
    {LogColor::WHITE, {"white", "\033[97m"}},
    {LogColor::NOCOLOR, {"noc", "\033[0;39m"}}};

thread_local std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash> Logger::formatMap = {
    {LogLevel::LOG_NOFORMAT, std::make_shared<LogFormat>("", getColorCode(LogColor::NOCOLOR))},
    {LogLevel::LOG_FATAL, std::make_shared<LogFormat>("FATAL", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_FATAL)))},
    {LogLevel::LOG_ERROR, std::make_shared<LogFormat>("ERROR", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_ERROR)))},
    {LogLevel::LOG_CONSOLE, std::make_shared<LogFormat>("CONSOLE", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_CONSOLE)))},
    {LogLevel::LOG_WARNING, std::make_shared<LogFormat>("WARNING", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_WARNING)))},
    {LogLevel::LOG_INFO,    std::make_shared<LogFormat>("INFO", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_INFO)))},
    {LogLevel::LOG_SUCCESS, std::make_shared<LogFormat>("SUCCESS", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_SUCCESS)))},
    {LogLevel::LOG_DEBUG1,  std::make_shared<LogFormat>("DEBUG 1", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_DEBUG1)))},
    {LogLevel::LOG_DEBUG2,  std::make_shared<LogFormat>("DEBUG 2", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_DEBUG2)))}
    };

thread_local std::atomic<bool> Logger::initializedAtomic = {false};

std::string Logger::getColorCode(std::string color) {
    if (Logger::colorEnabled) {
        for (auto it = Logger::colorCodes_.begin(); it != Logger::colorCodes_.end(); it++) {
            if (it->second.name == color) {
                return it->second.value;
            }
        }
    }
    return "";
}

std::string Logger::getColorCode(LogColor color) {
    if (Logger::colorEnabled) {
        return Logger::colorCodes_.at(color).value;
    }
    return "";
}

Logger::Logger() {
}

Logger::Logger(LogLevel logLevel) : Logger() {
    Logger::logLevel = logLevel;
}

Logger::Logger(LogLevel logLevel, std::string timeFormat) : Logger(logLevel) {
    Logger::timeFormat = timeFormat;
}

Logger& Logger::flush() {
    if (currentLevel <= logLevel) {
        std::time_t t = std::time(0);
        char ft[64];
        std::strftime(ft, 64, Logger::timeFormat.c_str(), std::localtime(&t));
        std::stringstream s;

        // reset color
        if (colorEnabled) {
            s << Logger::colorCodes_.at(LogColor::NOCOLOR).value;
        }
        if (currentLevel != LogLevel::LOG_NOFORMAT) {
            // time stamp
            s << "[" << ft << "]";
            // log level
            s << "[" << formatMap.at(currentLevel)->color << std::setfill(' ') << std::setw(7) << formatMap.at(currentLevel)->level << (colorEnabled ? colorCodes_.at(LogColor::NOCOLOR).value : "") << "] ";
            // log message
        }
        s << str() << std::endl;

        // logMessage(currentLevel, s.str());
        std::cout << s.str();
        if (logToFile) {
            if (currentLevel != LogLevel::LOG_NOFORMAT) {
                logfile << "[" << ft << "][" << std::setfill(' ') << std::setw(7) << formatMap.at(currentLevel)->level << "] " << str() << std::endl;
            } else {
                logfile << "[" << ft << "] " << str() << std::endl;
            }
            logfile.flush();
        }
    }
    str("");
    currentLevel = LogLevel::LOG_CONSOLE;
    return *this;
}

Logger& Logger::getInstance() {
    if (!initializedAtomic.exchange(true, std::memory_order_acquire)) {
        //		cout << std::this_thread::get_id() << " Instance is " << instance << endl;
        instance = std::make_shared<Logger>();
    }
    while (!instance) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    };
    return *instance;
}

Logger& Logger::operator<<(const LogLevel& level) {
    currentLevel = level;
    return *this;
}

Logger& Logger::operator<<(const LogColor& color) {
    if (Logger::colorEnabled) {
        (*this) << Logger::colorCodes_.at(color).value;
    }
    return *this;
}

Logger::~Logger() {
    instance = nullptr;
}

void Logger::LoadConfiguration() {
    std::string slevel = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_LOGGER_LEVEL, nullptr, true);
    if (slevel == "fatal")
        logLevel = LogLevel::LOG_FATAL;
    else if (slevel == "error")
        logLevel = LogLevel::LOG_ERROR;
    else if (slevel == "console")
        logLevel = LogLevel::LOG_CONSOLE;
    else if (slevel == "warning")
        logLevel = LogLevel::LOG_WARNING;
    else if (slevel == "info")
        logLevel = LogLevel::LOG_INFO;
    else if (slevel == "success")
        logLevel = LogLevel::LOG_SUCCESS;
    else if (slevel == "debug1")
        logLevel = LogLevel::LOG_DEBUG1;
    else if (slevel == "debug2")
        logLevel = LogLevel::LOG_DEBUG2;

    timeFormat = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_LOGGER_TIMEFORMAT);
    colorEnabled = ConnectionManager::getInstance().configuration->get<int>(MEMO_DEFAULT_LOGGER_COLOR_ENABLE);
    logToFile = ConnectionManager::getInstance().configuration->get<bool>(MEMO_DEFAULT_LOGGER_LOG_TO_FILE);
    std::string fname = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_LOGGER_FILENAME);
    if (fname != logFileName && logToFile) {
        logFileName = fname;
        if (logfile.is_open()) {
            logfile.close();
        }
        logfile.open(fname, std::ios_base::app);
    } else {
        if (!logToFile && logfile.is_open()) {
            logfile.close();
        } else if (logToFile) {
            logfile.open(fname, std::ios_base::app);
        }
    }

    formatMap[LogLevel::LOG_CONSOLE] = std::make_shared<LogFormat>("CONSOLE", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_CONSOLE)));
}

}  // namespace memordma