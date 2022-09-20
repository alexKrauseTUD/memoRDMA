/*
 * Logger.cpp
 *
 *  Created on: 29.01.2014
 *      Author: tom
 */

#include "Logger.h"

#include <thread>

#include "Configuration.h"
#include "ConnectionManager.h"

namespace memordma {

thread_local std::shared_ptr<Logger> Logger::instance;

LogLevel Logger::logLevel = LogLevel::INFO;
std::string Logger::timeFormat = "%m/%d %H:%M:%S";
std::string Logger::logFileName = "";
std::ofstream Logger::logfile{};
bool Logger::colorEnabled = false;
bool Logger::logToFile = false;

thread_local std::unordered_map<LogColor, ColorCode, LogColorHash> Logger::colorCodes_ = {
    {LogColor::BLACK, {"black", "\e[30m"}},
    {LogColor::RED, {"red", "\e[31m"}},
    {LogColor::GREEN, {"green", "\e[32m"}},
    {LogColor::YELLOW, {"yellow", "\e[33m"}},
    {LogColor::BLUE, {"blue", "\e[34m"}},
    {LogColor::MAGENTA, {"magenta", "\e[35m"}},
    {LogColor::CYAN, {"cyan", "\e[36m"}},
    {LogColor::LIGHT_GRAY, {"light_gray", "\e[37m"}},
    {LogColor::DARK_GRAY, {"dark_gray", "\e[90m"}},
    {LogColor::LIGHT_RED, {"light_red", "\e[91m"}},
    {LogColor::LIGHT_GREEN, {"light_green", "\e[92m"}},
    {LogColor::LIGHT_YELLOW, {"light_yellow", "\e[93m"}},
    {LogColor::LIGHT_BLUE, {"light_blue", "\e[94m"}},
    {LogColor::LIGHT_MAGENTA, {"light_magenta", "\e[95m"}},
    {LogColor::LIGHT_CYAN, {"light_cyan", "\e[96m"}},
    {LogColor::WHITE, {"white", "\e[97m"}},
    {LogColor::NOCOLOR, {"noc", "\e[0;39m"}}};

thread_local std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash> Logger::formatMap = {
    {LogLevel::NOFORMAT, std::make_shared<LogFormat>("", getColorCode(LogColor::NOCOLOR))},
    {LogLevel::FATAL, std::make_shared<LogFormat>("FATAL", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_FATAL)))},
    {LogLevel::ERROR, std::make_shared<LogFormat>("ERROR", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_ERROR)))},
    {LogLevel::CONSOLE, std::make_shared<LogFormat>("CONSOLE", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_CONSOLE)))},
    {LogLevel::WARNING, std::make_shared<LogFormat>("WARNING", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_WARNING)))},
    {LogLevel::INFO, std::make_shared<LogFormat>("INFO", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_INFO)))},
    {LogLevel::SUCCESS, std::make_shared<LogFormat>("SUCCESS", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_SUCCESS)))},
    {LogLevel::DEBUG1, std::make_shared<LogFormat>("DEBUG 1", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_DEBUG1)))},
    {LogLevel::DEBUG2, std::make_shared<LogFormat>("DEBUG 2", getColorCode(ConnectionManager::getInstance().configuration->get(MEMO_DEFAULT_LOGGER_COLOR_DEBUG2)))}};

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
        // ThreadManager::setGlobalMemoryAllocator();
        std::strftime(ft, 64, Logger::timeFormat.c_str(), std::localtime(&t));
        // ThreadManager::restoreMemoryAllocator();
        std::stringstream s;

        // reset color
        if (colorEnabled) {
            s << Logger::colorCodes_.at(LogColor::NOCOLOR).value;
        }
        if (currentLevel != LogLevel::NOFORMAT) {
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
            if (currentLevel != LogLevel::NOFORMAT) {
                logfile << "[" << ft << "][" << std::setfill(' ') << std::setw(7) << formatMap.at(currentLevel)->level << "] " << str() << std::endl;
            } else {
                logfile << "[" << ft << "] " << str() << std::endl;
            }
            logfile.flush();
        }
    }
    str("");
    currentLevel = LogLevel::CONSOLE;
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
    //	LoggerFence _;
    if (Logger::colorEnabled) {
        //		(*(std::ostringstream*)this) << Logger::colorCodes_.at(color).value;
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
        logLevel = LogLevel::FATAL;
    else if (slevel == "error")
        logLevel = LogLevel::ERROR;
    else if (slevel == "console")
        logLevel = LogLevel::CONSOLE;
    else if (slevel == "warning")
        logLevel = LogLevel::WARNING;
    else if (slevel == "info")
        logLevel = LogLevel::INFO;
    else if (slevel == "debug1")
        logLevel = LogLevel::DEBUG1;
    else if (slevel == "debug2")
        logLevel = LogLevel::DEBUG2;

    timeFormat = ConnectionManager::getInstance().configuration->getAsString(MEMO_DEFAULT_LOGGER_TIMEFORMAT);
    colorEnabled = ConnectionManager::getInstance().configuration->get<int>(MEMO_DEFAULT_LOGGER_COLOR_ENABLE);
    logToFile = ConnectionManager::getInstance().configuration->get<int>("logger.logtofile");
    std::string fname = ConnectionManager::getInstance().configuration->getAsString("logger.file");
    if (fname != logFileName && logToFile) {
        logFileName = fname;
        if (logfile.is_open()) {
            std::cout << "Closing old logfile." << std::endl;
            logfile.close();
        }
        std::cout << "Opening new logfile " << fname << std::endl;
        logfile.open(fname, std::ios_base::app);
    } else {
        if (!logToFile && logfile.is_open()) {
            std::cout << "Closing logfile" << std::endl;
            logfile.close();
        } else if (logToFile) {
            logfile.open(fname, std::ios_base::app);
        }
    }
}

}  // namespace memordma