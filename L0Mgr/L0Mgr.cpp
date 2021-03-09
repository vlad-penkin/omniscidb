/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "L0Mgr/L0Mgr.h"

#include "Logger/Logger.h"

#include <level_zero/ze_api.h>

#include <limits>

namespace L0Mgr_Namespace {

// TODO: move me
void L0InitGPUContext(ze_driver_handle_t &hDriver,
                          ze_device_handle_t &hDevice,
                          ze_command_queue_handle_t &hCommandQueue,
                          ze_context_handle_t &hContext) {
  L0_SAFE_CALL(zeInit(0));

  // Discover all the driver instances
  uint32_t driverCount = 0;
  L0_SAFE_CALL(zeDriverGet(&driverCount, nullptr));

  ze_driver_handle_t *allDrivers =
      (ze_driver_handle_t *)malloc(driverCount * sizeof(ze_driver_handle_t));
  L0_SAFE_CALL(zeDriverGet(&driverCount, allDrivers));

  // Find a driver instance with a GPU device
  for (uint32_t i = 0; i < driverCount; ++i) {
    uint32_t deviceCount = 0;
    hDriver = allDrivers[i];
    L0_SAFE_CALL(zeDeviceGet(hDriver, &deviceCount, nullptr));
    ze_device_handle_t *allDevices =
        (ze_device_handle_t *)malloc(deviceCount * sizeof(ze_device_handle_t));
    L0_SAFE_CALL(zeDeviceGet(hDriver, &deviceCount, allDevices));
    for (uint32_t d = 0; d < deviceCount; ++d) {
      ze_device_properties_t device_properties;
      L0_SAFE_CALL(zeDeviceGetProperties(allDevices[d], &device_properties));
      if (ZE_DEVICE_TYPE_GPU == device_properties.type) {
        hDevice = allDevices[d];
        break;
      }
    }
    free(allDevices);
    if (nullptr != hDevice) {
      break;
    }
  }
  free(allDrivers);
  assert(hDriver);
  assert(hDevice);

  ze_context_desc_t ctxtDesc = {ZE_STRUCTURE_TYPE_CONTEXT_DESC, nullptr, 0};
  L0_SAFE_CALL(zeContextCreate(hDriver, &ctxtDesc, &hContext));
  ze_command_queue_desc_t commandQueueDesc = {
      ZE_STRUCTURE_TYPE_COMMAND_QUEUE_DESC,
      nullptr,
      0, // computeQueueGroupOrdinal
      0, // index
      0, // flags
      ZE_COMMAND_QUEUE_MODE_DEFAULT,
      ZE_COMMAND_QUEUE_PRIORITY_NORMAL};
  L0_SAFE_CALL(zeCommandQueueCreate(hContext, hDevice, &commandQueueDesc,
                                    &hCommandQueue));

  
}

L0Mgr::L0Mgr() {
  L0InitGPUContext(hDriver, hDevice, hCommandQueue, hContext);
}
L0Mgr::~L0Mgr() {}

void L0Mgr::setSPV(std::string& spv) {
  size_t codeSize = spv.size();
  assert(codeSize != 0 && "Code size is 0.");
  unsigned char *codeBin = new unsigned char[codeSize];
  std::copy(spv.data(), spv.data() + codeSize, codeBin);

  std::ofstream out("complete.spv", std::ios::binary);
  out.write((char *)codeBin, codeSize);

  assert(codeSize && "CodeBin is empty");

  // L0Mgr::InitModule
  // ze_module_desc_t moduleDesc;
  moduleDesc.stype = ZE_STRUCTURE_TYPE_MODULE_DESC;
  moduleDesc.pNext = nullptr;
  moduleDesc.pBuildFlags = "";
  moduleDesc.format = ZE_MODULE_FORMAT_IL_SPIRV;
  moduleDesc.inputSize = codeSize;
  moduleDesc.pConstants = nullptr;
  moduleDesc.pInputModule = (uint8_t *)codeBin;

  ze_module_build_log_handle_t buildlog = nullptr;
  L0_SAFE_CALL(
      zeModuleCreate(hContext, hDevice, &moduleDesc, &hModule, &buildlog));
  size_t szLog = 0;
  L0_SAFE_CALL(zeModuleBuildLogGetString(buildlog, &szLog, nullptr));
  std::cout << "Got build log size " << szLog << std::endl;
  char *strLog = (char *)malloc(szLog);
  L0_SAFE_CALL(zeModuleBuildLogGetString(buildlog, &szLog, strLog));
  std::fstream log;
  log.open("log.txt", std::ios::app);
  if (!log.good()) {
    std::cerr << "Unable to open log file" << std::endl;
    exit(1);
  }
  log << strLog;
  log.close();

  ze_command_list_desc_t commandListDesc;
  commandListDesc.stype = ZE_STRUCTURE_TYPE_COMMAND_LIST_DESC;
  commandListDesc.pNext = nullptr;
  commandListDesc.flags = 0;
  commandListDesc.commandQueueGroupOrdinal = 0;

  L0_SAFE_CALL(
      zeCommandListCreate(hContext, hDevice, &commandListDesc, &hCommandList));

  initKernel("plus1");
  std::cerr << "Kernel created" << std::endl;
}

void L0Mgr::copyHostToDevice(void* device_ptr,
                             void* host_ptr,
                             size_t num_bytes,
                             int device_num) {
  L0_SAFE_CALL(zeCommandListAppendMemoryCopy(
      hCommandList, device_ptr, host_ptr, num_bytes, nullptr, 0, nullptr));
  L0_SAFE_CALL(zeCommandListAppendBarrier(hCommandList, nullptr, 0, nullptr));
}

void L0Mgr::copyDeviceToHost(void* host_ptr,
                             void* device_ptr,
                             size_t num_bytes,
                             int device_num) {
  L0_SAFE_CALL(zeCommandListAppendMemoryCopy(
      hCommandList, host_ptr, device_ptr, num_bytes, nullptr, 0, nullptr));
  L0_SAFE_CALL(zeCommandListAppendBarrier(hCommandList, nullptr, 0, nullptr));
}

void L0Mgr::copyDeviceToDevice(int8_t* dest_ptr,
                               int8_t* src_ptr,
                               size_t num_bytes,
                               int dest_device_num,
                               int src_device_num) {
  CHECK(false);
}


void* L0Mgr::allocateDeviceMem(size_t num_bytes, int device_num) {
  ze_device_mem_alloc_desc_t alloc_desc;
  alloc_desc.stype = ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC;
  alloc_desc.pNext = nullptr;
  alloc_desc.flags = 0;
  alloc_desc.ordinal = 0;

  void *mem = nullptr;
  L0_SAFE_CALL(zeMemAllocDevice(hContext, &alloc_desc, num_bytes, 0/*align*/, hDevice, &mem));
  return mem;
}

void L0Mgr::createModule(unsigned char* code, size_t size_bytes) {
  initModule(code, size_bytes);
}

void L0Mgr::setMemArgument(void* mem, size_t pos) {
  assert(mem);

  L0_SAFE_CALL(zeKernelSetArgumentValue(hKernel, pos, sizeof(void*), &mem));
}

void L0Mgr::initModule(unsigned char* code, size_t size_bytes) {
  moduleDesc.stype = ZE_STRUCTURE_TYPE_MODULE_DESC;
  moduleDesc.pNext = nullptr;
  moduleDesc.pBuildFlags = ""; // ?
  moduleDesc.format = ZE_MODULE_FORMAT_IL_SPIRV;
  moduleDesc.inputSize = size_bytes;
  moduleDesc.pConstants = nullptr;
  moduleDesc.pInputModule = code;
  L0_SAFE_CALL(zeModuleCreate(hContext, hDevice, &moduleDesc, &hModule, nullptr));
}

void L0Mgr::launch(/*todo: params?*/) {
  L0_SAFE_CALL(zeKernelSetGroupSize(hKernel, 1, 1, 1));
  ze_group_count_t dispatchTraits = {1, 1, 1};
  L0_SAFE_CALL(zeCommandListAppendLaunchKernel(
      hCommandList, hKernel, &dispatchTraits, nullptr, 0, nullptr));
  L0_SAFE_CALL(zeCommandListAppendBarrier(hCommandList, nullptr, 0, nullptr));
}

void L0Mgr::initKernel(std::string name) {
  kernelDesc.stype = ZE_STRUCTURE_TYPE_KERNEL_DESC;
  kernelDesc.pNext = nullptr;
  kernelDesc.flags = 0;
  kernelDesc.pKernelName = name.c_str();

  L0_SAFE_CALL(zeKernelCreate(hModule, &kernelDesc, &hKernel));
}

void L0Mgr::commit() {
  L0_SAFE_CALL(zeCommandListClose(hCommandList));
  L0_SAFE_CALL(
      zeCommandQueueExecuteCommandLists(hCommandQueue, 1, &hCommandList, nullptr));
  L0_SAFE_CALL(
      zeCommandQueueSynchronize(hCommandQueue, std::numeric_limits<uint32_t>::max()));
}

}  // namespace L0Mgr_Namespace