/*
 * Logger.h
 *
 *  Created on: 29.01.2014
 *      Author: tom
 */

#ifndef LOGGER_H_
#define LOGGER_H_

#include <atomic>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

namespace memordma {

    typedef std::ostream &(*manip1)(std::ostream &);
    typedef std::basic_ios<std::ostream::char_type, std::ostream::traits_type> ios_type;
    typedef ios_type &(*manip2)(ios_type &);
    typedef std::ios_base &(*manip3)(std::ios_base &);

enum class LogLevel {
    LOG_NOFORMAT,
    LOG_FATAL,
    LOG_ERROR,
    LOG_CONSOLE,
    LOG_WARNING,
    LOG_INFO,
    LOG_SUCCESS,
    LOG_DEBUG1,
    LOG_DEBUG2
};

// log message colorization
enum class LogColor {
    NOCOLOR,
    BLACK,
    GREY,
    RED,
    GREEN,
    YELLOW,
    BLUE,
    MAGENTA,
    CYAN,
    LIGHT_GRAY,
    DARK_GRAY,
    LIGHT_RED,
    LIGHT_GREEN,
    LIGHT_YELLOW,
    LIGHT_BLUE,
    LIGHT_MAGENTA,
    LIGHT_CYAN,
    WHITE
};

struct LogLevelHash {
    std::size_t operator()(const LogLevel& arg) const {
        std::hash<int> hashfn;
        return hashfn((int)arg);
    }
};

struct LogColorHash {
    std::size_t operator()(const LogColor& arg) const {
        std::hash<int> hashfn;
        return hashfn((int)arg);
    }
};

struct LogFormat {
    std::string level;
    std::string color;

    LogFormat(std::string _level, std::string _color) : level{_level}, color{_color} {};
};

struct ColorCode {
    std::string name;
    std::string value;
};

std::ostream& operator<<(std::ostream& os, LogColor c);
std::stringstream& operator<<(std::stringstream& ss, LogColor c);

struct LogMessage {
    LogMessage(LogLevel level, std::string message) : level(level), message(message) {}

    LogLevel level;
    std::string message;
};

#ifdef MEMO_NOLOGGING
    #define LOG_NOFORMAT(x)
    #define LOG_FATAL(x)
    #define LOG_ERROR(x)
    #define LOG_CONSOLE(x)
    #define LOG_WARNING(x)
    #define LOG_INFO(x)
    #define LOG_SUCCESS(x)
    #define LOG_DEBUG1(x)
    #define LOG_DEBUG2(x)

#else
    #define LOG_NOFORMAT(x) Logger::getInstance() << LogLevel::LOG_NOFORMAT << x
    #define LOG_FATAL(x) Logger::getInstance() << LogLevel::LOG_FATAL << x
    #define LOG_ERROR(x) Logger::getInstance() << LogLevel::LOG_ERROR << x
    #define LOG_CONSOLE(x) Logger::getInstance() << LogLevel::LOG_CONSOLE << x

    #ifndef MEMO_NOLOG_INFOLOGGING
        #define LOG_WARNING(x) Logger::getInstance() << LogLevel::LOG_WARNING << x
        #define LOG_INFO(x) Logger::getInstance() << LogLevel::LOG_INFO << x
        #define LOG_SUCCESS(x) Logger::getInstance() << LogLevel::LOG_SUCCESS << x

    #else
        #define LOG_WARNING(x)
        #define LOG_INFO(x)
        #define LOG_SUCCESS(x)
    #endif

    #ifndef MEMO_NODEBUGLOGGING
        #define LOG_DEBUG1(x) Logger::getInstance() << LogLevel::LOG_DEBUG1 << x
        #define LOG_DEBUG2(x) Logger::getInstance() << LogLevel::LOG_DEBUG2 << x
    #else
        #define LOG_DEBUG1(x)
        #define LOG_DEBUG2(x)
    #endif

#endif

/**
 * @ingroup common
 * @ingroup logger
 * @brief
 *
 */
class Logger : public std::ostringstream {
   public:
    Logger();
    Logger(LogLevel logLevel);
    Logger(LogLevel logLevel, std::string timeFormat);
    static void LoadConfiguration();
    Logger& flush();
    Logger& operator<<(const LogLevel& level);
    Logger& operator<<(const LogColor& color);

    template <typename T>
    Logger& operator<<(const T& t) {
        (*(std::ostringstream*)this) << t;
        return *this;
    }
    Logger& operator<<(manip1 fp) {
        if (fp == (std::basic_ostream<char, std::char_traits<char> > & (*)(std::basic_ostream<char, std::char_traits<char> >&)) std::endl) {
            flush();
            return *this;
        }
        (*(std::ostringstream*)this) << fp;
        return *this;
    }
    Logger& operator<<(manip2 fp) {
        (*(std::ostringstream*)this) << fp;
        return *this;
    }
    Logger& operator<<(manip3 fp) {
        (*(std::ostringstream*)this) << fp;
        return *this;
    }

    //	Logger& operator<<(LogColor c){
    //		LoggerFence _fence;
    //		if(this->colorEnabled){
    ////			os << Logger::getInstance().colorCodes_->at(c).value;
    ////			(*(std::ostringstream*)this) << this->colorCodes_->at(c).value;
    //		}
    //		return *this;
    //	}

    virtual ~Logger();

    static Logger& getInstance();

    static bool colorEnabled;
    static bool logToFile;
    static std::string logFileName;
    static std::ofstream logfile;

    /// Get the colorcode from a string
    static std::string getColorCode(std::string color);
    /// Get the colorcode from a LogColor
    static std::string getColorCode(LogColor color);
    /// Holds color names and colorcodes
    static thread_local std::unordered_map<LogColor, ColorCode, LogColorHash> colorCodes_;
    /// Holds information about each log level
    static thread_local std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash> formatMap;

   protected:
    /// Defines the minimal display log level
    static LogLevel logLevel;
    /// Defines the time formatting
    static std::string timeFormat;
    /// Defines the temporary log level
    LogLevel currentLevel = LogLevel::LOG_CONSOLE;
    static thread_local std::atomic<bool> initializedAtomic;

   public:
    static thread_local std::shared_ptr<Logger> instance;
};

}  // namespace memordma

#endif /* LOGGER_H_ */
