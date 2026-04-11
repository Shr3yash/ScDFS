#pragma once

#include <iostream>
#include <sstream>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <string>

namespace scdfs {

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

class Logger {
public:
    static Logger& instance() {
        static Logger logger;
        return logger;
    }

    void set_level(LogLevel level) { level_ = level; }

    template <typename... Args>
    void log(LogLevel level, const char* file, int line, Args&&... args) {
        if (level < level_) return;

        std::ostringstream oss;
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) % 1000;

        oss << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S")
            << '.' << std::setfill('0') << std::setw(3) << ms.count()
            << " [" << level_str(level) << "] "
            << extract_filename(file) << ":" << line << " - ";

        ((oss << std::forward<Args>(args)), ...);
        oss << '\n';

        std::lock_guard<std::mutex> lock(mutex_);
        std::cerr << oss.str();
    }

private:
    Logger() = default;
    LogLevel level_ = LogLevel::INFO;
    std::mutex mutex_;

    static const char* level_str(LogLevel l) {
        switch (l) {
            case LogLevel::DEBUG: return "DEBUG";
            case LogLevel::INFO:  return "INFO ";
            case LogLevel::WARN:  return "WARN ";
            case LogLevel::ERROR: return "ERROR";
            default:              return "?????";
        }
    }

    static const char* extract_filename(const char* path) {
        const char* f = path;
        for (const char* p = path; *p; ++p) {
            if (*p == '/' || *p == '\\') f = p + 1;
        }
        return f;
    }
};

#define LOG_DEBUG(...) scdfs::Logger::instance().log(scdfs::LogLevel::DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  scdfs::Logger::instance().log(scdfs::LogLevel::INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  scdfs::Logger::instance().log(scdfs::LogLevel::WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) scdfs::Logger::instance().log(scdfs::LogLevel::ERROR, __FILE__, __LINE__, __VA_ARGS__)

} // namespace scdfs
