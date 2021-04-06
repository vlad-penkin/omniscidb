#include "QueryEngine/L0Kernel.h"
#include "L0Mgr/Utils.h"
#include "L0Mgr/L0Exception.h"
#include "Logger/Logger.h"  // CHECK

L0BinResult spv_to_bin(const std::string& spv,
                       const unsigned block_size,
                       const l0::L0Manager* mgr) {
  CHECK(!spv.empty());
  CHECK(mgr);

  void* bin{nullptr};
  size_t binSize{0};

  // size_t codeSize = spv.size();
  // unsigned char *codeBin = new unsigned char[codeSize];
  // std::copy(spv.data(), spv.data() + codeSize, codeBin);

  // ze_module_desc_t moduleDesc;
  // moduleDesc.stype = ZE_STRUCTURE_TYPE_MODULE_DESC;
  // moduleDesc.pNext = nullptr;
  // moduleDesc.pBuildFlags = "";
  // moduleDesc.format = ZE_MODULE_FORMAT_IL_SPIRV;
  // moduleDesc.inputSize = codeSize;
  // moduleDesc.pConstants = nullptr;
  // moduleDesc.pInputModule = (uint8_t *)codeBin;

  // auto driver = mgr->drivers()[0];
  // auto context_handle = driver->ctx();
  // // this is awkward... should store modules along with devices?
  // auto device_handle = driver->devices()[0].get()->device();

  // ze_module_handle_t hModule = nullptr;

  // L0_SAFE_CALL(
  //     zeModuleCreate(context_handle, device_handle, &moduleDesc, &hModule, nullptr));

  // ze_kernel_desc_t kernelDesc;
  // kernelDesc.stype = ZE_STRUCTURE_TYPE_KERNEL_DESC;
  // kernelDesc.pNext = nullptr;
  // kernelDesc.flags = 0;
  // kernelDesc.pKernelName = ""; // where from?

  // ze_kernel_handle_t hKernel;
  // L0_SAFE_CALL(zeKernelCreate(hModule, &kernelDesc, &hKernel));



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