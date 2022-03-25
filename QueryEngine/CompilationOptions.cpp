#include "QueryEngine/CompilationOptions.h"
#include <ostream>

std::ostream& operator<<(std::ostream& os, const ExecutorDeviceType& dt) {
  os << (dt == ExecutorDeviceType::CPU ? "CPU" : "GPU");
  return os;
}