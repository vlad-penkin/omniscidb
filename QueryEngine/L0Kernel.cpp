#include "QueryEngine/L0Kernel.h"
#include "Logger/Logger.h"  // CHECK

L0BinResult spv_to_bin(const std::string& spv,
                       const unsigned block_size,
                       const l0::L0Device* dev) {
  CHECK(!spv.empty());
  CHECK(dev);

  void* bin{nullptr};
  size_t binSize{0};

  return {bin};
}

L0DeviceCompilationContext::L0DeviceCompilationContext(const void* image,
                                                       const size_t image_size,
                                                       const std::string& kernel_name,
                                                       const int device_id,
                                                       unsigned int num_options,
                                                       void** option_vals)
    : device_id_(device_id) {}

L0DeviceCompilationContext::~L0DeviceCompilationContext() {}