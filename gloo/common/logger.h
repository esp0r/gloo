#include <chrono>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <fstream>
#include <condition_variable>
#include <cstdlib>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <sys/file.h> // for flock

// struct gloo::transport::tcp::Op

struct Header {
  size_t nbytes;
  size_t opcode;
  size_t slot;
  size_t offset;
  size_t length;
  size_t roffset;

  Header(size_t nbytes, size_t opcode, size_t slot, size_t offset, size_t length, size_t roffset)
      : nbytes(nbytes), opcode(opcode), slot(slot), offset(offset), length(length), roffset(roffset) {}
};

struct LogEntry {
  std::string event;
  Header header;
  int nread;
  int nwritten;
  std::chrono::high_resolution_clock::time_point timestamp;

  LogEntry(const std::string& event, const Header& header, int nread, int nwritten, const std::chrono::high_resolution_clock::time_point& timestamp)
      : event(event), header(header), nread(nread), nwritten(nwritten), timestamp(timestamp) {}
};

class Logger {
public:
  Logger(int from_rank, int to_rank) : done_(false), from_rank_(from_rank), to_rank_(to_rank), buffer_max_depth_(10) {
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
    log_file_path_ = log_dir_ + "gloo_log_rank_" + std::to_string(from_rank_) + ".txt";

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

  void logEvent(const std::string& event, size_t nbytes, size_t opcode, size_t slot, size_t offset, size_t length, size_t roffset, int nread, int nwritten) {
    auto now = std::chrono::high_resolution_clock::now();
    Header header(nbytes, opcode, slot, offset, length, roffset);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      current_buffer_.emplace_back(LogEntry{event, header, nread, nwritten, now});
      if (current_buffer_.size() >= buffer_max_depth_) {
        cv_.notify_all();
      }
    }
  }

private:
  void writeLogsToFile() {
    std::ofstream log_file(log_file_path_, std::ios::out | std::ios::app);
    int fd = open(log_file_path_.c_str(), O_WRONLY | O_APPEND);
    if (fd == -1) {
      throw std::runtime_error("Failed to open log file");
    }

    std::vector<LogEntry> buffer_to_write;

    while (true) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !current_buffer_.empty() || done_; });

        if (done_ && current_buffer_.empty()) {
          break;
        }

        // Swap buffers
        std::swap(current_buffer_, buffer_to_write);
      }

      // Lock the file
      if (flock(fd, LOCK_EX) == -1) {
        throw std::runtime_error("Failed to lock log file");
      }

      // Write logs from buffer_to_write to file
      for (const auto& log : buffer_to_write) {
          auto time_since_start = std::chrono::duration_cast<std::chrono::microseconds>(
              log.timestamp.time_since_epoch()).count();

          // Create a JSON object for the log entry
          nlohmann::json log_entry;
          log_entry["from_rank"] = from_rank_;
          log_entry["to_rank"] = to_rank_;
          log_entry["event"] = log.event;
          log_entry["timestamp"] = time_since_start;
          // express in 0x format
          log_entry["nread"] = log.nread;
          log_entry["nwritten"] = log.nwritten;
          log_entry["header"] = {
              {"nbytes", log.header.nbytes},
              {"opcode", log.header.opcode},
              {"slot", log.header.slot},
              {"offset", log.header.offset},
              {"length", log.header.length},
              {"roffset", log.header.roffset}
          };


          // Write the JSON object to the log file
          log_file << log_entry.dump() << std::endl;
      }
      buffer_to_write.clear();

      // Unlock the file
      if (flock(fd, LOCK_UN) == -1) {
        throw std::runtime_error("Failed to unlock log file");
      }
    }

    close(fd);
  }

  std::chrono::system_clock::time_point start_time_;
  std::vector<LogEntry> current_buffer_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread log_thread_;
  bool done_;
  int from_rank_;
  int to_rank_;
  std::string log_dir_;
  std::string log_file_path_;
  size_t buffer_max_depth_;
};
