#pragma once
// Two-sided RC SEND/RECV for live MuCQL's fixed-size control frames.
// The TCP socket remains exclusively for QP metadata exchange before GO.
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace mucql {
class LiveControlChannel {
 public:
  LiveControlChannel(int bootstrap_fd, const std::string& device, uint8_t ib_port,
                     bool rdma, std::size_t message_size);
  ~LiveControlChannel();
  LiveControlChannel(const LiveControlChannel&)=delete;
  LiveControlChannel& operator=(const LiveControlChannel&)=delete;
  void send(const void* message, std::size_t size);
  void recv(void* message, std::size_t size);
  void stop() noexcept; // cancel a polling reader; caller still joins before destroying
  bool is_rdma() const noexcept;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace mucql
