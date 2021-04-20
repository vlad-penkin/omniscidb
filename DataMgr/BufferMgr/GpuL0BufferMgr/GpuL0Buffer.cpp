/*
 * Copyright 2017 MapD Technologies, Inc.
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

#include "DataMgr/BufferMgr/GpuL0BufferMgr/GpuL0Buffer.h"

#include <cassert>

#include "L0Mgr/L0Mgr.h"
#include "Logger/Logger.h"

namespace Buffer_Namespace {

GpuL0Buffer::GpuL0Buffer(BufferMgr* bm,
                         BufferList::iterator seg_it,
                         const int device_id,
                         l0::L0Manager* l0_mgr,
                         const size_t page_size,
                         const size_t num_bytes)
    : Buffer(bm, seg_it, device_id, page_size, num_bytes), l0_mgr_(l0_mgr) {}

void GpuL0Buffer::readData(int8_t* const dst,
                           const size_t num_bytes,
                           const size_t offset,
                           const MemoryLevel dst_buffer_type,
                           const int dst_device_id) {
#ifdef HAVE_L0
  if (dst_buffer_type == CPU_LEVEL) {
    auto& device = l0_mgr_->drivers()[0]->devices()[device_id_];
    auto cl = device->create_command_list();
    auto queue = device->command_queue();

    cl->copy(dst, mem_ + offset, num_bytes);
    cl->submit(queue);
    L0_SAFE_CALL(zeCommandQueueSynchronize(queue, std::numeric_limits<uint32_t>::max()));
  } else if (dst_buffer_type == GPU_LEVEL) {
    throw std::runtime_error("copyDeviceToDevice is not yet supported in l0Manager");
  } else {
    LOG(FATAL) << "Unsupported buffer type";
  }
#endif  // HAVE_L0
}

void GpuL0Buffer::writeData(int8_t* const src,
                            const size_t num_bytes,
                            const size_t offset,
                            const MemoryLevel src_buffer_type,
                            const int src_device_id) {
#ifdef HAVE_L0
  if (src_buffer_type == CPU_LEVEL) {
    auto& device = l0_mgr_->drivers()[0]->devices()[device_id_];
    auto cl = device->create_command_list();
    auto queue = device->command_queue();

    cl->copy(mem_ + offset, src, num_bytes);
    cl->submit(queue);
    L0_SAFE_CALL(zeCommandQueueSynchronize(queue, std::numeric_limits<uint32_t>::max()));

  } else if (src_buffer_type == GPU_LEVEL) {
    throw std::runtime_error("copyDeviceToDevice is not yet supported in l0Manager");
  } else {
    LOG(FATAL) << "Unsupported buffer type";
  }
#endif  // HAVE_L0
}

}  // namespace Buffer_Namespace
