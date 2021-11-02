#include "QueryEngine/CompilationOptions.h"

#include <ostream>

std::ostream& operator<<(std::ostream& os, const ExecutorDeviceType& dt) {
  switch (dt) {
    case ExecutorDeviceType::CPU:
      return os << "ExecutorDeviceType::CPU";
    case ExecutorDeviceType::CUDA:
      return os << "ExecutorDeviceType::CUDA";
    case ExecutorDeviceType::L0:
      return os << "ExecutorDeviceType::L0";

    default:
      throw std::runtime_error("Unknown device type.");
  }
}