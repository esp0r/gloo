#include <chrono>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <fstream>
#include <condition_variable>
#include <cstdlib>

// Log entry structure
struct LogEntry {
  std::string event;
  std::chrono::high_resolution_clock::time_point timestamp;
};

// Logger class to handle logging
class Logger {
public:
  Logger(int rank) : done_(false), rank_(rank) {
    // Initialize start time
    start_time_ = std::chrono::system_clock::now();

    // Read environment variable
    const char* log_dir = std::getenv("GLOO_LOG_DIR");
    if (log_dir == nullptr) {
      log_dir_ = "./";
    } else {
      log_dir_ = std::string(log_dir);
      if (log_dir_.back() != '/') {
        log_dir_ += "/";
      }
    }

    // Set log file path for this rank
    log_file_path_ = log_dir_ + "gloo_log_rank_" + std::to_string(rank_) + ".txt";

    log_thread_ = std::thread(&Logger::writeLogsToFile, this);
  }

  ~Logger() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      done_ = true;
    }
    cv_.notify_all();
    log_thread_.join();
  }

  void logEvent(const std::string& event) {
    auto now = std::chrono::high_resolution_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    logs_.emplace_back(LogEntry{event, now});
    cv_.notify_all();
  }

private:
  void writeLogsToFile() {
    std::ofstream log_file(log_file_path_, std::ios::out | std::ios::app);
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return !logs_.empty() || done_; });

      if (done_ && logs_.empty()) {
        break;
      }

      for (const auto& log : logs_) {
        auto time_since_start = std::chrono::duration_cast<std::chrono::microseconds>(
          log.timestamp.time_since_epoch()).count();
        log_file << "Event: " << log.event << ", Timestamp: " << time_since_start << " us\n";
      }
      logs_.clear();
    }
  }

  std::chrono::system_clock::time_point start_time_;
  std::vector<LogEntry> logs_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread log_thread_;
  bool done_;
  int rank_;
  std::string log_dir_;
  std::string log_file_path_;
};
