/*
 * Copyright 2021 OmniSci, Inc.
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

#include "QueryMemoryInitializer.h"

#include "Execute.h"
#include "GpuInitGroups.h"
#include "GpuMemUtils.h"
#include "HashTableDesc.h"
#include "Logger/Logger.h"
#include "OutputBufferInitialization.h"
#include "ResultSet.h"
#include "StreamingTopN.h"

#include <Shared/Globals.h>
#include <Shared/checked_alloc.h>
#include <ThirdParty/robin_hood.h>
#include <x86intrin.h>

// 8 GB, the limit of perfect hash group by under normal conditions
int64_t g_bitmap_memory_limit{8LL * 1000 * 1000 * 1000};

extern bool g_optimize_row_initialization;
extern size_t g_max_memory_allocation_size;

namespace {

inline void check_total_bitmap_memory(const QueryMemoryDescriptor& query_mem_desc) {
  const int32_t groups_buffer_entry_count = query_mem_desc.getEntryCount();
  checked_int64_t total_bytes_per_group = 0;
  const size_t num_count_distinct_descs =
      query_mem_desc.getCountDistinctDescriptorsSize();
  for (size_t i = 0; i < num_count_distinct_descs; i++) {
    const auto count_distinct_desc = query_mem_desc.getCountDistinctDescriptor(i);
    if (count_distinct_desc.impl_type_ != CountDistinctImplType::Bitmap) {
      continue;
    }
    total_bytes_per_group += count_distinct_desc.bitmapPaddedSizeBytes();
  }
  int64_t total_bytes{0};
  // Using OutOfHostMemory until we can verify that SlabTooBig would also be properly
  // caught
  try {
    total_bytes = static_cast<int64_t>(total_bytes_per_group * groups_buffer_entry_count);
  } catch (...) {
    // Absurd amount of memory, merely computing the number of bits overflows int64_t.
    // Don't bother to report the real amount, this is unlikely to ever happen.
    throw OutOfHostMemory(std::numeric_limits<int64_t>::max() / 8);
  }
  if (total_bytes >= g_bitmap_memory_limit) {
    throw OutOfHostMemory(total_bytes);
  }
}

int64_t* alloc_group_by_buffer(const size_t numBytes,
                               const size_t thread_idx,
                               RowSetMemoryOwner* mem_owner) {
  return reinterpret_cast<int64_t*>(mem_owner->allocate(numBytes, thread_idx));
}

inline int64_t get_consistent_frag_size(const std::vector<uint64_t>& frag_offsets) {
  if (frag_offsets.size() < 2) {
    return int64_t(-1);
  }
  const auto frag_size = frag_offsets[1] - frag_offsets[0];
  for (size_t i = 2; i < frag_offsets.size(); ++i) {
    const auto curr_size = frag_offsets[i] - frag_offsets[i - 1];
    if (curr_size != frag_size) {
      return int64_t(-1);
    }
  }
  return !frag_size ? std::numeric_limits<int64_t>::max()
                    : static_cast<int64_t>(frag_size);
}

inline std::vector<int64_t> get_consistent_frags_sizes(
    const std::vector<std::vector<uint64_t>>& frag_offsets) {
  if (frag_offsets.empty()) {
    return {};
  }
  std::vector<int64_t> frag_sizes;
  for (size_t tab_idx = 0; tab_idx < frag_offsets[0].size(); ++tab_idx) {
    std::vector<uint64_t> tab_offs;
    for (auto& offsets : frag_offsets) {
      tab_offs.push_back(offsets[tab_idx]);
    }
    frag_sizes.push_back(get_consistent_frag_size(tab_offs));
  }
  return frag_sizes;
}

inline std::vector<int64_t> get_consistent_frags_sizes(
    const std::vector<Analyzer::Expr*>& target_exprs,
    const std::vector<int64_t>& table_frag_sizes) {
  std::vector<int64_t> col_frag_sizes;
  for (auto expr : target_exprs) {
    if (const auto col_var = dynamic_cast<Analyzer::ColumnVar*>(expr)) {
      if (col_var->get_rte_idx() < 0) {
        CHECK_EQ(-1, col_var->get_rte_idx());
        col_frag_sizes.push_back(int64_t(-1));
      } else {
        col_frag_sizes.push_back(table_frag_sizes[col_var->get_rte_idx()]);
      }
    } else {
      col_frag_sizes.push_back(int64_t(-1));
    }
  }
  return col_frag_sizes;
}

inline std::vector<std::vector<int64_t>> get_col_frag_offsets(
    const std::vector<Analyzer::Expr*>& target_exprs,
    const std::vector<std::vector<uint64_t>>& table_frag_offsets) {
  std::vector<std::vector<int64_t>> col_frag_offsets;
  for (auto& table_offsets : table_frag_offsets) {
    std::vector<int64_t> col_offsets;
    for (auto expr : target_exprs) {
      if (const auto col_var = dynamic_cast<Analyzer::ColumnVar*>(expr)) {
        if (col_var->get_rte_idx() < 0) {
          CHECK_EQ(-1, col_var->get_rte_idx());
          col_offsets.push_back(int64_t(-1));
        } else {
          CHECK_LT(static_cast<size_t>(col_var->get_rte_idx()), table_offsets.size());
          col_offsets.push_back(
              static_cast<int64_t>(table_offsets[col_var->get_rte_idx()]));
        }
      } else {
        col_offsets.push_back(int64_t(-1));
      }
    }
    col_frag_offsets.push_back(col_offsets);
  }
  return col_frag_offsets;
}

constexpr size_t const_log2(size_t v) {
  if (v == 1) {
    return 0;
  } else {
    assert(!(v & 1));
    return const_log2(v / 2) + 1;
  }
}

// Return a min number of rows required to fill a pattern
// divisible by vec_size.
template <size_t vec_size>
size_t get_num_rows_for_vec_sample(const size_t row_size) {
  auto rem = row_size & (vec_size - 1);
  if (!rem) {
    return 1;
  }

  auto vp2 = const_log2(vec_size);
  auto p2 = 0;
  while (!(rem & 1)) {
    rem >>= 1;
    p2++;
  }

  return 1 << (vp2 - p2);
}

// It's assumed sample has 64 extra bytes to handle alignment.
__attribute__((target("avx512f"))) void spread_vec_sample(int8_t* dst,
                                                          const size_t dst_size,
                                                          const int8_t* sample_ptr,
                                                          const size_t sample_size) {
  assert((reinterpret_cast<uint64_t>(dst) & 0x3F) ==
         (reinterpret_cast<uint64_t>(sample_ptr) & 0x3F));
  // Align buffers.
  int64_t align_bytes = ((64ULL - reinterpret_cast<uint64_t>(dst)) & 0x3F);
  memcpy(dst, sample_ptr, align_bytes);

  int8_t* align_dst = dst + align_bytes;
  const int8_t* align_sample = sample_ptr + align_bytes;
  size_t rem = dst_size - align_bytes;
  size_t rem_scalar = rem % sample_size;
  size_t rem_vector = rem - rem_scalar;

  // Aligned vector part.
  auto vecs = sample_size / 64;
  auto vec_end = align_dst + rem_vector;
  while (align_dst < vec_end) {
    for (size_t i = 0; i < vecs; ++i) {
      __m512i vec_val =
          _mm512_load_si512(reinterpret_cast<const __m512i*>(align_sample) + i);
      _mm512_stream_si512(reinterpret_cast<__m512i*>(align_dst) + i, vec_val);
    }
    align_dst += sample_size;
  }

  // Scalar tail.
  memcpy(align_dst, align_sample, rem_scalar);
}

__attribute__((target("default"))) void spread_vec_sample(int8_t* dst,
                                                          const size_t dst_size,
                                                          const int8_t* sample_ptr,
                                                          const size_t sample_size) {
  size_t rem = dst_size;
  while (rem >= sample_size) {
    memcpy(dst, sample_ptr, sample_size);
    rem -= sample_size;
    dst += sample_size;
  }
  memcpy(dst, sample_ptr, rem);
}

}  // namespace

// Row-based execution constructor
QueryMemoryInitializer::QueryMemoryInitializer(
    const RelAlgExecutionUnit& ra_exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const int device_id,
    const ExecutorDeviceType device_type,
    const ExecutorDispatchMode dispatch_mode,
    const bool output_columnar,
    const bool sort_on_gpu,
    const bool use_hash_table_desc,
    const int64_t num_rows,
    const std::vector<std::vector<const int8_t*>>& col_buffers,
    const std::vector<std::vector<uint64_t>>& frag_offsets,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    DeviceAllocator* device_allocator,
    const size_t thread_idx,
    const Executor* executor)
    : num_rows_(num_rows)
    , row_set_mem_owner_(row_set_mem_owner)
    , init_agg_vals_(executor->plan_state_->init_agg_vals_)
    , num_buffers_(computeNumberOfBuffers(query_mem_desc, device_type, executor))
    , varlen_output_buffer_(0)
    , varlen_output_buffer_host_ptr_(nullptr)
    , count_distinct_bitmap_mem_(0)
    , count_distinct_bitmap_mem_bytes_(0)
    , count_distinct_bitmap_crt_ptr_(nullptr)
    , count_distinct_bitmap_host_mem_(nullptr)
    , device_allocator_(device_allocator)
    , use_hash_table_desc_(use_hash_table_desc)
    , thread_idx_(thread_idx) {
  CHECK(!sort_on_gpu || output_columnar);

  const auto& consistent_frag_sizes = get_consistent_frags_sizes(frag_offsets);
  if (consistent_frag_sizes.empty()) {
    // No fragments in the input, no underlying buffers will be needed.
    return;
  }
  if (!ra_exe_unit.use_bump_allocator) {
    check_total_bitmap_memory(query_mem_desc);
  }
  if (device_type == ExecutorDeviceType::GPU) {
    allocateCountDistinctGpuMem(query_mem_desc);
  }

  if (!query_mem_desc.isGroupBy()) {
    allocateCountDistinctBuffers(query_mem_desc, false, executor);
    allocateTDigests(query_mem_desc, false, executor);
  }

  if (ra_exe_unit.estimator) {
    return;
  }

  const auto thread_count = device_type == ExecutorDeviceType::GPU
                                ? executor->blockSize() * executor->gridSize()
                                : 1;

  size_t group_buffer_size{0};
  if (ra_exe_unit.use_bump_allocator) {
    // For kernel per fragment execution, just allocate a buffer equivalent to the size of
    // the fragment
    if (dispatch_mode == ExecutorDispatchMode::KernelPerFragment) {
      group_buffer_size = num_rows * query_mem_desc.getRowSize();
    } else {
      // otherwise, allocate a GPU buffer equivalent to the maximum GPU allocation size
      group_buffer_size = g_max_memory_allocation_size / query_mem_desc.getRowSize();
    }
  } else {
    group_buffer_size =
        query_mem_desc.getBufferSizeBytes(ra_exe_unit, thread_count, device_type);
  }
  CHECK_GE(group_buffer_size, size_t(0));

  const auto group_buffers_count = !query_mem_desc.isGroupBy() ? 1 : num_buffers_;
  int64_t* group_by_buffer_template{nullptr};
  if (!query_mem_desc.lazyInitGroups(device_type) && group_buffers_count > 1) {
    group_by_buffer_template = reinterpret_cast<int64_t*>(
        row_set_mem_owner_->allocate(group_buffer_size, thread_idx_));
    initGroupByBuffer(group_by_buffer_template,
                      ra_exe_unit,
                      query_mem_desc,
                      device_type,
                      output_columnar,
                      executor);
  }

  if (query_mem_desc.interleavedBins(device_type)) {
    CHECK(query_mem_desc.hasKeylessHash());
  }

  const auto step = device_type == ExecutorDeviceType::GPU &&
                            query_mem_desc.threadsShareMemory() &&
                            query_mem_desc.isGroupBy()
                        ? executor->blockSize()
                        : size_t(1);
  const auto index_buffer_qw = device_type == ExecutorDeviceType::GPU && sort_on_gpu &&
                                       query_mem_desc.hasKeylessHash()
                                   ? query_mem_desc.getEntryCount()
                                   : size_t(0);
  const auto actual_group_buffer_size =
      group_buffer_size + index_buffer_qw * sizeof(int64_t);
  CHECK_GE(actual_group_buffer_size, group_buffer_size);

  if (query_mem_desc.hasVarlenOutput()) {
    CHECK(!use_hash_table_desc_);
    const auto varlen_buffer_elem_size_opt = query_mem_desc.varlenOutputBufferElemSize();
    CHECK(varlen_buffer_elem_size_opt);  // TODO(adb): relax
    auto varlen_output_buffer = reinterpret_cast<int64_t*>(row_set_mem_owner_->allocate(
        query_mem_desc.getEntryCount() * varlen_buffer_elem_size_opt.value()));
    num_buffers_ += 1;
    group_by_buffers_.push_back(varlen_output_buffer);
  }

  for (size_t i = 0; i < group_buffers_count; i += step) {
    auto group_by_buffer = alloc_group_by_buffer(
        actual_group_buffer_size, thread_idx_, row_set_mem_owner_.get());

    if (!query_mem_desc.lazyInitGroups(device_type)) {
      if (group_by_buffer_template) {
        memcpy(group_by_buffer + index_buffer_qw,
               group_by_buffer_template,
               group_buffer_size);
      } else {
        initGroupByBuffer(group_by_buffer + index_buffer_qw,
                          ra_exe_unit,
                          query_mem_desc,
                          device_type,
                          output_columnar,
                          executor);
      }
    }

    if (query_mem_desc.getQueryDescriptionType() ==
            QueryDescriptionType::GroupByBaselineHash &&
        use_hash_table_desc_) {
      //  when Hash Table Descriptors are used, int64_t pointers passed to IR's function
      //  are casted back correctly to HashTableDesc*.
      auto* desc = new HashTableDesc(reinterpret_cast<int8_t*>(group_by_buffer),
                                     query_mem_desc.getEntryCount());
      group_by_buffers_.push_back(reinterpret_cast<int64_t*>(desc));
      hash_table_desc_holders_.emplace_back(desc);
    } else {
      group_by_buffers_.push_back(group_by_buffer);
    }

    for (size_t j = 1; j < step; ++j) {
      group_by_buffers_.push_back(nullptr);
    }

    const auto column_frag_offsets =
        get_col_frag_offsets(ra_exe_unit.target_exprs, frag_offsets);
    const auto column_frag_sizes =
        get_consistent_frags_sizes(ra_exe_unit.target_exprs, consistent_frag_sizes);

    result_sets_.emplace_back(
        new ResultSet(target_exprs_to_infos(ra_exe_unit.target_exprs, query_mem_desc),
                      executor->getColLazyFetchInfo(ra_exe_unit.target_exprs),
                      col_buffers,
                      column_frag_offsets,
                      column_frag_sizes,
                      device_type,
                      device_id,
                      ResultSet::fixupQueryMemoryDescriptor(query_mem_desc),
                      row_set_mem_owner_,
                      executor->getDataMgr(),
                      executor->getBufferProvider(),
                      executor->blockSize(),
                      executor->gridSize()));
    result_sets_.back()->allocateStorage(reinterpret_cast<int8_t*>(group_by_buffer),
                                         executor->plan_state_->init_agg_vals_,
                                         getVarlenOutputInfo());
    for (size_t j = 1; j < step; ++j) {
      result_sets_.emplace_back(nullptr);
    }
  }
}

// Table functions execution constructor
QueryMemoryInitializer::QueryMemoryInitializer(
    const TableFunctionExecutionUnit& exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const int device_id,
    const ExecutorDeviceType device_type,
    const bool use_hash_table_desc,
    const int64_t num_rows,
    const std::vector<std::vector<const int8_t*>>& col_buffers,
    const std::vector<std::vector<uint64_t>>& frag_offsets,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    DeviceAllocator* device_allocator,
    const Executor* executor)
    : num_rows_(num_rows)
    , row_set_mem_owner_(row_set_mem_owner)
    , init_agg_vals_(init_agg_val_vec(exe_unit.target_exprs, {}, query_mem_desc))
    , num_buffers_(1)
    , varlen_output_buffer_(0)
    , varlen_output_buffer_host_ptr_(nullptr)
    , count_distinct_bitmap_mem_(0)
    , count_distinct_bitmap_mem_bytes_(0)
    , count_distinct_bitmap_crt_ptr_(nullptr)
    , count_distinct_bitmap_host_mem_(nullptr)
    , device_allocator_(device_allocator)
    , use_hash_table_desc_(use_hash_table_desc)
    , thread_idx_(0) {
  // Table functions output columnar, basically treat this as a projection
  const auto& consistent_frag_sizes = get_consistent_frags_sizes(frag_offsets);
  if (consistent_frag_sizes.empty()) {
    // No fragments in the input, no underlying buffers will be needed.
    return;
  }

  size_t group_buffer_size{0};
  const size_t num_columns = query_mem_desc.getBufferColSlotCount();
  group_buffer_size = num_rows_ * num_columns * sizeof(int64_t);
  CHECK_GE(group_buffer_size, size_t(0));

  const auto index_buffer_qw =
      device_type == ExecutorDeviceType::GPU && query_mem_desc.hasKeylessHash()
          ? query_mem_desc.getEntryCount()
          : size_t(0);
  const auto actual_group_buffer_size =
      group_buffer_size + index_buffer_qw * sizeof(int64_t);
  CHECK_GE(actual_group_buffer_size, group_buffer_size);

  CHECK_EQ(num_buffers_, size_t(1));
  auto group_by_buffer = alloc_group_by_buffer(
      actual_group_buffer_size, thread_idx_, row_set_mem_owner.get());
  if (!query_mem_desc.lazyInitGroups(device_type)) {
    initColumnarGroups(
        query_mem_desc, group_by_buffer + index_buffer_qw, init_agg_vals_, executor);
  }

  if (use_hash_table_desc_) {
    auto* desc = new HashTableDesc(reinterpret_cast<int8_t*>(group_by_buffer),
                                   query_mem_desc.getEntryCount());
    group_by_buffers_.push_back(reinterpret_cast<int64_t*>(desc));
    hash_table_desc_holders_.emplace_back(desc);
  } else {
    group_by_buffers_.push_back(group_by_buffer);
  }

  const auto column_frag_offsets =
      get_col_frag_offsets(exe_unit.target_exprs, frag_offsets);
  const auto column_frag_sizes =
      get_consistent_frags_sizes(exe_unit.target_exprs, consistent_frag_sizes);
  result_sets_.emplace_back(
      new ResultSet(target_exprs_to_infos(exe_unit.target_exprs, query_mem_desc),
                    /*col_lazy_fetch_info=*/{},
                    col_buffers,
                    column_frag_offsets,
                    column_frag_sizes,
                    device_type,
                    device_id,
                    ResultSet::fixupQueryMemoryDescriptor(query_mem_desc),
                    row_set_mem_owner_,
                    executor->getDataMgr(),
                    executor->getBufferProvider(),
                    executor->blockSize(),
                    executor->gridSize()));
  result_sets_.back()->allocateStorage(reinterpret_cast<int8_t*>(group_by_buffer),
                                       init_agg_vals_);
}

void QueryMemoryInitializer::initGroupByBuffer(
    int64_t* buffer,
    const RelAlgExecutionUnit& ra_exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const ExecutorDeviceType device_type,
    const bool output_columnar,
    const Executor* executor) {
  if (output_columnar) {
    // When we run CPU projection code, we always compute a number of matched rows.
    // This number is then used for columnar output buffer compaction (see
    // compactProjectionBuffersCpu). This means we would never read uninitialized keys.
    if (device_type == ExecutorDeviceType::GPU ||
        query_mem_desc.getQueryDescriptionType() != QueryDescriptionType::Projection) {
      initColumnarGroups(query_mem_desc, buffer, init_agg_vals_, executor);
    }
  } else {
    auto rows_ptr = buffer;
    auto actual_entry_count = query_mem_desc.getEntryCount();
    const auto thread_count = device_type == ExecutorDeviceType::GPU
                                  ? executor->blockSize() * executor->gridSize()
                                  : 1;
    auto warp_size =
        query_mem_desc.interleavedBins(device_type) ? executor->warpSize() : 1;
    if (query_mem_desc.useStreamingTopN()) {
      const auto node_count_size = thread_count * sizeof(int64_t);
      memset(rows_ptr, 0, node_count_size);
      const auto n = ra_exe_unit.sort_info.offset + ra_exe_unit.sort_info.limit;
      const auto rows_offset = streaming_top_n::get_rows_offset_of_heaps(n, thread_count);
      memset(rows_ptr + thread_count, -1, rows_offset - node_count_size);
      rows_ptr += rows_offset / sizeof(int64_t);
      actual_entry_count = n * thread_count;
      warp_size = 1;
    }
    initRowGroups(query_mem_desc,
                  rows_ptr,
                  init_agg_vals_,
                  actual_entry_count,
                  warp_size,
                  executor);
  }
}

bool QueryMemoryInitializer::useVectorRowGroupsInit(const size_t row_size,
                                                    const size_t entries) const {
  if (!g_optimize_row_initialization) {
    return false;
  }

  // Assume 512-bit vector size. Don't bother if
  // the sample is too big.
  auto rows_per_sample = get_num_rows_for_vec_sample<64>(row_size);
  return entries / rows_per_sample > 3;
}

void QueryMemoryInitializer::initRowGroups(const QueryMemoryDescriptor& query_mem_desc,
                                           int64_t* groups_buffer,
                                           const std::vector<int64_t>& init_vals,
                                           const int32_t groups_buffer_entry_count,
                                           const size_t warp_size,
                                           const Executor* executor) {
  const size_t key_count{query_mem_desc.getGroupbyColCount()};
  const size_t row_size{query_mem_desc.getRowSize()};
  const size_t col_base_off{query_mem_desc.getColOffInBytes(0)};

  auto agg_bitmap_size = allocateCountDistinctBuffers(query_mem_desc, true, executor);
  auto quantile_params = allocateTDigests(query_mem_desc, true, executor);
  auto buffer_ptr = reinterpret_cast<int8_t*>(groups_buffer);

  const auto query_mem_desc_fixedup =
      ResultSet::fixupQueryMemoryDescriptor(query_mem_desc);

  auto const is_true = [](auto const& x) { return static_cast<bool>(x); };
  // not COUNT DISTINCT / APPROX_COUNT_DISTINCT / APPROX_QUANTILE
  // we fallback to default implementation in that cases
  if (!std::any_of(agg_bitmap_size.begin(), agg_bitmap_size.end(), is_true) &&
      !std::any_of(quantile_params.begin(), quantile_params.end(), is_true) &&
      useVectorRowGroupsInit(row_size, groups_buffer_entry_count * warp_size)) {
    auto rows_per_sample = get_num_rows_for_vec_sample<64>(row_size);
    auto sample_size = row_size * rows_per_sample;
    // Additional bytes are required to achieve the same alignment
    // as groups_buffer has and still use aligned vector operations to
    // copy the whole sample.
    std::vector<int8_t> vec_sample(sample_size + 128);
    int8_t* sample_ptr = vec_sample.data();
    sample_ptr += (reinterpret_cast<uint64_t>(buffer_ptr) -
                   reinterpret_cast<uint64_t>(sample_ptr)) &
                  0x3F;

    for (size_t i = 0; i < rows_per_sample; ++i) {
      initColumnsPerRow(query_mem_desc_fixedup,
                        sample_ptr + row_size * i + col_base_off,
                        init_vals,
                        agg_bitmap_size,
                        quantile_params);
    }

    size_t rows_count = groups_buffer_entry_count;
    if (query_mem_desc.hasKeylessHash()) {
      CHECK(warp_size >= 1);
      CHECK(key_count == 1 || warp_size == 1);
      rows_count *= warp_size;
    } else {
      for (size_t i = 0; i < rows_per_sample; ++i) {
        result_set::fill_empty_key(
            sample_ptr + row_size * i, key_count, query_mem_desc.getEffectiveKeyWidth());
      }
    }

    // Duplicate the first 64 bytes of the sample to enable
    // copy after alignment.
    memcpy(sample_ptr + sample_size, sample_ptr, 64);

    spread_vec_sample(buffer_ptr, rows_count * row_size, sample_ptr, sample_size);
  } else {
    if (query_mem_desc.hasKeylessHash()) {
      CHECK(warp_size >= 1);
      CHECK(key_count == 1 || warp_size == 1);
      for (size_t warp_idx = 0; warp_idx < warp_size; ++warp_idx) {
        for (size_t bin = 0; bin < static_cast<size_t>(groups_buffer_entry_count);
             ++bin, buffer_ptr += row_size) {
          initColumnsPerRow(query_mem_desc_fixedup,
                            &buffer_ptr[col_base_off],
                            init_vals,
                            agg_bitmap_size,
                            quantile_params);
        }
      }
      return;
    }

    for (size_t bin = 0; bin < static_cast<size_t>(groups_buffer_entry_count);
         ++bin, buffer_ptr += row_size) {
      result_set::fill_empty_key(
          buffer_ptr, key_count, query_mem_desc.getEffectiveKeyWidth());
      initColumnsPerRow(query_mem_desc_fixedup,
                        &buffer_ptr[col_base_off],
                        init_vals,
                        agg_bitmap_size,
                        quantile_params);
    }
  }
}

namespace {

template <typename T>
int8_t* initColumnarBuffer(T* buffer_ptr, const T init_val, const uint32_t entry_count) {
  static_assert(sizeof(T) <= sizeof(int64_t), "Unsupported template type");
  for (uint32_t i = 0; i < entry_count; ++i) {
    buffer_ptr[i] = init_val;
  }
  return reinterpret_cast<int8_t*>(buffer_ptr + entry_count);
}

}  // namespace

void QueryMemoryInitializer::initColumnarGroups(
    const QueryMemoryDescriptor& query_mem_desc,
    int64_t* groups_buffer,
    const std::vector<int64_t>& init_vals,
    const Executor* executor) {
  CHECK(groups_buffer);
  for (const auto target_expr : executor->plan_state_->target_exprs_) {
    const auto agg_info = get_target_info(target_expr, g_bigint_count);
    CHECK(!is_distinct_target(agg_info));
  }
  const int32_t agg_col_count = query_mem_desc.getSlotCount();
  auto buffer_ptr = reinterpret_cast<int8_t*>(groups_buffer);

  const auto groups_buffer_entry_count = query_mem_desc.getEntryCount();
  if (!query_mem_desc.hasKeylessHash()) {
    const size_t key_count{query_mem_desc.getGroupbyColCount()};
    for (size_t i = 0; i < key_count; ++i) {
      buffer_ptr = initColumnarBuffer<int64_t>(reinterpret_cast<int64_t*>(buffer_ptr),
                                               EMPTY_KEY_64,
                                               groups_buffer_entry_count);
    }
  }

  if (query_mem_desc.getQueryDescriptionType() != QueryDescriptionType::Projection) {
    // initializing all aggregate columns:
    int32_t init_val_idx = 0;
    for (int32_t i = 0; i < agg_col_count; ++i) {
      if (query_mem_desc.getPaddedSlotWidthBytes(i) > 0) {
        CHECK_LT(static_cast<size_t>(init_val_idx), init_vals.size());
        switch (query_mem_desc.getPaddedSlotWidthBytes(i)) {
          case 1:
            buffer_ptr = initColumnarBuffer<int8_t>(
                buffer_ptr, init_vals[init_val_idx++], groups_buffer_entry_count);
            break;
          case 2:
            buffer_ptr =
                initColumnarBuffer<int16_t>(reinterpret_cast<int16_t*>(buffer_ptr),
                                            init_vals[init_val_idx++],
                                            groups_buffer_entry_count);
            break;
          case 4:
            buffer_ptr =
                initColumnarBuffer<int32_t>(reinterpret_cast<int32_t*>(buffer_ptr),
                                            init_vals[init_val_idx++],
                                            groups_buffer_entry_count);
            break;
          case 8:
            buffer_ptr =
                initColumnarBuffer<int64_t>(reinterpret_cast<int64_t*>(buffer_ptr),
                                            init_vals[init_val_idx++],
                                            groups_buffer_entry_count);
            break;
          case 0:
            break;
          default:
            CHECK(false);
        }

        buffer_ptr = align_to_int64(buffer_ptr);
      }
    }
  }
}

void QueryMemoryInitializer::initColumnsPerRow(
    const QueryMemoryDescriptor& query_mem_desc,
    int8_t* row_ptr,
    const std::vector<int64_t>& init_vals,
    const std::vector<int64_t>& bitmap_sizes,
    const std::vector<QuantileParam>& quantile_params) {
  int8_t* col_ptr = row_ptr;
  size_t init_vec_idx = 0;
  for (size_t col_idx = 0; col_idx < query_mem_desc.getSlotCount();
       col_ptr += query_mem_desc.getNextColOffInBytesRowOnly(col_ptr, col_idx++)) {
    const int64_t bm_sz{bitmap_sizes[col_idx]};
    int64_t init_val{0};
    if (bm_sz && query_mem_desc.isGroupBy()) {
      // COUNT DISTINCT / APPROX_COUNT_DISTINCT
      CHECK_EQ(static_cast<size_t>(query_mem_desc.getPaddedSlotWidthBytes(col_idx)),
               sizeof(int64_t));
      init_val =
          bm_sz > 0 ? allocateCountDistinctBitmap(bm_sz) : allocateCountDistinctSet();
      ++init_vec_idx;
    } else if (query_mem_desc.isGroupBy() && quantile_params[col_idx]) {
      auto const q = *quantile_params[col_idx];
      // allocate for APPROX_QUANTILE only when slot is used
      init_val = reinterpret_cast<int64_t>(row_set_mem_owner_->nullTDigest(q));
      ++init_vec_idx;
    } else {
      if (query_mem_desc.getPaddedSlotWidthBytes(col_idx) > 0) {
        CHECK_LT(init_vec_idx, init_vals.size());
        init_val = init_vals[init_vec_idx++];
      }
    }
    switch (query_mem_desc.getPaddedSlotWidthBytes(col_idx)) {
      case 1:
        *col_ptr = static_cast<int8_t>(init_val);
        break;
      case 2:
        *reinterpret_cast<int16_t*>(col_ptr) = (int16_t)init_val;
        break;
      case 4:
        *reinterpret_cast<int32_t*>(col_ptr) = (int32_t)init_val;
        break;
      case 8:
        *reinterpret_cast<int64_t*>(col_ptr) = init_val;
        break;
      case 0:
        continue;
      default:
        CHECK(false);
    }
  }
}

void QueryMemoryInitializer::allocateCountDistinctGpuMem(
    const QueryMemoryDescriptor& query_mem_desc) {
  if (query_mem_desc.countDistinctDescriptorsLogicallyEmpty()) {
    return;
  }
  CHECK(device_allocator_);

  size_t total_bytes_per_entry{0};
  const size_t num_count_distinct_descs =
      query_mem_desc.getCountDistinctDescriptorsSize();
  for (size_t i = 0; i < num_count_distinct_descs; i++) {
    const auto count_distinct_desc = query_mem_desc.getCountDistinctDescriptor(i);
    if (count_distinct_desc.impl_type_ == CountDistinctImplType::Invalid) {
      continue;
    }
    CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap);
    total_bytes_per_entry += count_distinct_desc.bitmapPaddedSizeBytes();
  }

  count_distinct_bitmap_mem_bytes_ =
      total_bytes_per_entry * query_mem_desc.getEntryCount();
  count_distinct_bitmap_mem_ = reinterpret_cast<CUdeviceptr>(
      device_allocator_->alloc(count_distinct_bitmap_mem_bytes_));
  device_allocator_->zeroDeviceMem(reinterpret_cast<int8_t*>(count_distinct_bitmap_mem_),
                                   count_distinct_bitmap_mem_bytes_);

  count_distinct_bitmap_crt_ptr_ = count_distinct_bitmap_host_mem_ =
      row_set_mem_owner_->allocate(count_distinct_bitmap_mem_bytes_, thread_idx_);
}

// deferred is true for group by queries; initGroups will allocate a bitmap
// for each group slot
std::vector<int64_t> QueryMemoryInitializer::allocateCountDistinctBuffers(
    const QueryMemoryDescriptor& query_mem_desc,
    const bool deferred,
    const Executor* executor) {
  const size_t agg_col_count{query_mem_desc.getSlotCount()};
  std::vector<int64_t> agg_bitmap_size(deferred ? agg_col_count : 0);

  CHECK_GE(agg_col_count, executor->plan_state_->target_exprs_.size());
  for (size_t target_idx = 0; target_idx < executor->plan_state_->target_exprs_.size();
       ++target_idx) {
    const auto target_expr = executor->plan_state_->target_exprs_[target_idx];
    const auto agg_info = get_target_info(target_expr, g_bigint_count);
    if (is_distinct_target(agg_info)) {
      CHECK(agg_info.is_agg &&
            (agg_info.agg_kind == kCOUNT || agg_info.agg_kind == kAPPROX_COUNT_DISTINCT));
      CHECK(!agg_info.sql_type.is_varlen());

      const size_t agg_col_idx = query_mem_desc.getSlotIndexForSingleSlotCol(target_idx);
      CHECK_LT(static_cast<size_t>(agg_col_idx), agg_col_count);

      CHECK_EQ(static_cast<size_t>(query_mem_desc.getLogicalSlotWidthBytes(agg_col_idx)),
               sizeof(int64_t));
      const auto& count_distinct_desc =
          query_mem_desc.getCountDistinctDescriptor(target_idx);
      CHECK(count_distinct_desc.impl_type_ != CountDistinctImplType::Invalid);
      if (count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
        const auto bitmap_byte_sz = count_distinct_desc.bitmapPaddedSizeBytes();
        if (deferred) {
          agg_bitmap_size[agg_col_idx] = bitmap_byte_sz;
        } else {
          init_agg_vals_[agg_col_idx] = allocateCountDistinctBitmap(bitmap_byte_sz);
        }
      } else {
        CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
        if (deferred) {
          agg_bitmap_size[agg_col_idx] = -1;
        } else {
          init_agg_vals_[agg_col_idx] = allocateCountDistinctSet();
        }
      }
    }
  }

  return agg_bitmap_size;
}

int64_t QueryMemoryInitializer::allocateCountDistinctBitmap(const size_t bitmap_byte_sz) {
  if (count_distinct_bitmap_host_mem_) {
    CHECK(count_distinct_bitmap_crt_ptr_);
    auto ptr = count_distinct_bitmap_crt_ptr_;
    count_distinct_bitmap_crt_ptr_ += bitmap_byte_sz;
    row_set_mem_owner_->addCountDistinctBuffer(
        ptr, bitmap_byte_sz, /*physial_buffer=*/false);
    return reinterpret_cast<int64_t>(ptr);
  }
  return reinterpret_cast<int64_t>(
      row_set_mem_owner_->allocateCountDistinctBuffer(bitmap_byte_sz, thread_idx_));
}

int64_t QueryMemoryInitializer::allocateCountDistinctSet() {
  auto count_distinct_set = new robin_hood::unordered_set<int64_t>();
  row_set_mem_owner_->addCountDistinctSet(count_distinct_set);
  return reinterpret_cast<int64_t>(count_distinct_set);
}

std::vector<QueryMemoryInitializer::QuantileParam>
QueryMemoryInitializer::allocateTDigests(const QueryMemoryDescriptor& query_mem_desc,
                                         const bool deferred,
                                         const Executor* executor) {
  size_t const slot_count = query_mem_desc.getSlotCount();
  size_t const ntargets = executor->plan_state_->target_exprs_.size();
  CHECK_GE(slot_count, ntargets);
  std::vector<QuantileParam> quantile_params(deferred ? slot_count : 0);

  for (size_t target_idx = 0; target_idx < ntargets; ++target_idx) {
    auto const target_expr = executor->plan_state_->target_exprs_[target_idx];
    if (auto const agg_expr = dynamic_cast<const Analyzer::AggExpr*>(target_expr)) {
      if (agg_expr->get_aggtype() == kAPPROX_QUANTILE) {
        size_t const agg_col_idx =
            query_mem_desc.getSlotIndexForSingleSlotCol(target_idx);
        CHECK_LT(agg_col_idx, slot_count);
        CHECK_EQ(query_mem_desc.getLogicalSlotWidthBytes(agg_col_idx),
                 static_cast<int8_t>(sizeof(int64_t)));
        auto const q = agg_expr->get_arg1()->get_constval().doubleval;
        if (deferred) {
          quantile_params[agg_col_idx] = q;
        } else {
          // allocate for APPROX_QUANTILE only when slot is used
          init_agg_vals_[agg_col_idx] =
              reinterpret_cast<int64_t>(row_set_mem_owner_->nullTDigest(q));
        }
      }
    }
  }
  return quantile_params;
}

#ifdef HAVE_CUDA
GpuGroupByBuffers QueryMemoryInitializer::prepareTopNHeapsDevBuffer(
    const QueryMemoryDescriptor& query_mem_desc,
    const CUdeviceptr init_agg_vals_dev_ptr,
    const size_t n,
    const int device_id,
    const unsigned block_size_x,
    const unsigned grid_size_x) {
  CHECK(device_allocator_);
  const auto thread_count = block_size_x * grid_size_x;
  const auto total_buff_size =
      streaming_top_n::get_heap_size(query_mem_desc.getRowSize(), n, thread_count);
  CUdeviceptr dev_buffer =
      reinterpret_cast<CUdeviceptr>(device_allocator_->alloc(total_buff_size));

  std::vector<CUdeviceptr> dev_buffers(thread_count);

  for (size_t i = 0; i < thread_count; ++i) {
    dev_buffers[i] = dev_buffer;
  }

  auto dev_ptr = device_allocator_->alloc(thread_count * sizeof(CUdeviceptr));
  device_allocator_->copyToDevice(dev_ptr,
                                  reinterpret_cast<int8_t*>(dev_buffers.data()),
                                  thread_count * sizeof(CUdeviceptr));

  CHECK(query_mem_desc.lazyInitGroups(ExecutorDeviceType::GPU));

  device_allocator_->zeroDeviceMem(reinterpret_cast<int8_t*>(dev_buffer),
                                   thread_count * sizeof(int64_t));

  device_allocator_->setDeviceMem(
      reinterpret_cast<int8_t*>(dev_buffer + thread_count * sizeof(int64_t)),
      (unsigned char)-1,
      thread_count * n * sizeof(int64_t));

  init_group_by_buffer_on_device(
      reinterpret_cast<int64_t*>(
          dev_buffer + streaming_top_n::get_rows_offset_of_heaps(n, thread_count)),
      reinterpret_cast<int64_t*>(init_agg_vals_dev_ptr),
      n * thread_count,
      query_mem_desc.getGroupbyColCount(),
      query_mem_desc.getEffectiveKeyWidth(),
      query_mem_desc.getRowSize() / sizeof(int64_t),
      query_mem_desc.hasKeylessHash(),
      1,
      block_size_x,
      grid_size_x);

  return {reinterpret_cast<CUdeviceptr>(dev_ptr), dev_buffer};
}

GpuGroupByBuffers QueryMemoryInitializer::createAndInitializeGroupByBufferGpu(
    const RelAlgExecutionUnit& ra_exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const CUdeviceptr init_agg_vals_dev_ptr,
    const int device_id,
    const ExecutorDispatchMode dispatch_mode,
    const unsigned block_size_x,
    const unsigned grid_size_x,
    const int8_t warp_size,
    const bool can_sort_on_gpu,
    const bool output_columnar) {
  if (query_mem_desc.useStreamingTopN()) {
    const auto n = ra_exe_unit.sort_info.offset + ra_exe_unit.sort_info.limit;
    CHECK(!output_columnar);

    return prepareTopNHeapsDevBuffer(
        query_mem_desc, init_agg_vals_dev_ptr, n, device_id, block_size_x, grid_size_x);
  }
  CHECK(!use_hash_table_desc_);
  auto dev_group_by_buffers =
      create_dev_group_by_buffers(device_allocator_,
                                  group_by_buffers_,
                                  query_mem_desc,
                                  block_size_x,
                                  grid_size_x,
                                  device_id,
                                  dispatch_mode,
                                  num_rows_,
                                  can_sort_on_gpu,
                                  false,
                                  ra_exe_unit.use_bump_allocator,
                                  query_mem_desc.hasVarlenOutput(),
                                  nullptr);
  if (query_mem_desc.hasVarlenOutput()) {
    CHECK(dev_group_by_buffers.varlen_output_buffer);
    varlen_output_buffer_ = dev_group_by_buffers.varlen_output_buffer;
    CHECK(query_mem_desc.varlenOutputBufferElemSize());
    const size_t varlen_output_buf_bytes =
        query_mem_desc.getEntryCount() *
        query_mem_desc.varlenOutputBufferElemSize().value();
    varlen_output_buffer_host_ptr_ =
        row_set_mem_owner_->allocate(varlen_output_buf_bytes, thread_idx_);
    CHECK(varlen_output_info_);
    varlen_output_info_->gpu_start_address = static_cast<int64_t>(varlen_output_buffer_);
    varlen_output_info_->cpu_buffer_ptr = varlen_output_buffer_host_ptr_;
  }
  if (query_mem_desc.lazyInitGroups(ExecutorDeviceType::GPU)) {
    const size_t step{query_mem_desc.threadsShareMemory() ? block_size_x : 1};
    size_t groups_buffer_size{query_mem_desc.getBufferSizeBytes(
        ExecutorDeviceType::GPU, dev_group_by_buffers.entry_count)};
    auto group_by_dev_buffer = dev_group_by_buffers.data;
    const size_t col_count = query_mem_desc.getSlotCount();
    int8_t* col_widths_dev_ptr{nullptr};
    if (output_columnar) {
      std::vector<int8_t> compact_col_widths(col_count);
      for (size_t idx = 0; idx < col_count; ++idx) {
        compact_col_widths[idx] = query_mem_desc.getPaddedSlotWidthBytes(idx);
      }
      col_widths_dev_ptr = device_allocator_->alloc(col_count * sizeof(int8_t));
      device_allocator_->copyToDevice(
          col_widths_dev_ptr, compact_col_widths.data(), col_count * sizeof(int8_t));
    }
    const int8_t warp_count =
        query_mem_desc.interleavedBins(ExecutorDeviceType::GPU) ? warp_size : 1;
    const auto num_group_by_buffers =
        getGroupByBuffersSize() - (query_mem_desc.hasVarlenOutput() ? 1 : 0);
    for (size_t i = 0; i < num_group_by_buffers; i += step) {
      if (output_columnar) {
        init_columnar_group_by_buffer_on_device(
            reinterpret_cast<int64_t*>(group_by_dev_buffer),
            reinterpret_cast<const int64_t*>(init_agg_vals_dev_ptr),
            dev_group_by_buffers.entry_count,
            query_mem_desc.getGroupbyColCount(),
            col_count,
            col_widths_dev_ptr,
            /*need_padding = */ true,
            query_mem_desc.hasKeylessHash(),
            sizeof(int64_t),
            block_size_x,
            grid_size_x);
      } else {
        init_group_by_buffer_on_device(reinterpret_cast<int64_t*>(group_by_dev_buffer),
                                       reinterpret_cast<int64_t*>(init_agg_vals_dev_ptr),
                                       dev_group_by_buffers.entry_count,
                                       query_mem_desc.getGroupbyColCount(),
                                       query_mem_desc.getEffectiveKeyWidth(),
                                       query_mem_desc.getRowSize() / sizeof(int64_t),
                                       query_mem_desc.hasKeylessHash(),
                                       warp_count,
                                       block_size_x,
                                       grid_size_x);
      }
      group_by_dev_buffer += groups_buffer_size;
    }
  }
  return dev_group_by_buffers;
}

GpuGroupByBuffers QueryMemoryInitializer::setupTableFunctionGpuBuffers(
    const QueryMemoryDescriptor& query_mem_desc,
    const int device_id,
    const unsigned block_size_x,
    const unsigned grid_size_x) {
  const size_t num_columns = query_mem_desc.getBufferColSlotCount();
  CHECK_GT(num_columns, size_t(0));

  const size_t column_size = num_rows_ * sizeof(int64_t);
  const size_t groups_buffer_size = num_columns * (column_size == 0 ? 1 : column_size);
  const size_t mem_size =
      groups_buffer_size * (query_mem_desc.blocksShareMemory() ? 1 : grid_size_x);

  int8_t* dev_buffers_allocation{nullptr};
  dev_buffers_allocation = device_allocator_->alloc(mem_size);
  CHECK(dev_buffers_allocation);

  CUdeviceptr dev_buffers_mem = reinterpret_cast<CUdeviceptr>(dev_buffers_allocation);
  const size_t step{block_size_x};
  const size_t num_ptrs{block_size_x * grid_size_x};
  std::vector<CUdeviceptr> dev_buffers(num_columns * num_ptrs);
  auto dev_buffer = dev_buffers_mem;
  for (size_t i = 0; i < num_ptrs; i += step) {
    for (size_t j = 0; j < step; j += 1) {
      for (size_t k = 0; k < num_columns; k++) {
        dev_buffers[(i + j) * num_columns + k] = dev_buffer + k * column_size;
      }
    }
    if (!query_mem_desc.blocksShareMemory()) {
      dev_buffer += groups_buffer_size;
    }
  }

  auto dev_ptr = device_allocator_->alloc(num_columns * num_ptrs * sizeof(CUdeviceptr));
  device_allocator_->copyToDevice(dev_ptr,
                                  reinterpret_cast<int8_t*>(dev_buffers.data()),
                                  num_columns * num_ptrs * sizeof(CUdeviceptr));

  return {reinterpret_cast<CUdeviceptr>(dev_ptr), dev_buffers_mem, (size_t)num_rows_};
}

void QueryMemoryInitializer::copyFromTableFunctionGpuBuffers(
    BufferProvider* buffer_provider,
    const QueryMemoryDescriptor& query_mem_desc,
    const size_t entry_count,
    const GpuGroupByBuffers& gpu_group_by_buffers,
    const int device_id,
    const unsigned block_size_x,
    const unsigned grid_size_x) {
  CHECK(!use_hash_table_desc_);
  const size_t num_columns = query_mem_desc.getBufferColSlotCount();
  const size_t column_size = entry_count * sizeof(int64_t);
  const size_t orig_column_size = gpu_group_by_buffers.entry_count * sizeof(int64_t);
  int8_t* dev_buffer = reinterpret_cast<int8_t*>(gpu_group_by_buffers.data);
  int8_t* host_buffer = reinterpret_cast<int8_t*>(group_by_buffers_[0]);
  CHECK_LE(column_size, orig_column_size);
  if (orig_column_size == column_size) {
    buffer_provider->copyFromDevice(host_buffer,
                                    reinterpret_cast<const int8_t*>(dev_buffer),
                                    column_size * num_columns,
                                    device_id);
  } else {
    for (size_t k = 0; k < num_columns; ++k) {
      buffer_provider->copyFromDevice(host_buffer,
                                      reinterpret_cast<const int8_t*>(dev_buffer),
                                      column_size,
                                      device_id);
      dev_buffer += orig_column_size;
      host_buffer += column_size;
    }
  }
}

#endif

size_t QueryMemoryInitializer::computeNumberOfBuffers(
    const QueryMemoryDescriptor& query_mem_desc,
    const ExecutorDeviceType device_type,
    const Executor* executor) const {
  return device_type == ExecutorDeviceType::CPU
             ? 1
             : executor->blockSize() *
                   (query_mem_desc.blocksShareMemory() ? 1 : executor->gridSize());
}

namespace {

// in-place compaction of output buffer
void compact_projection_buffer_for_cpu_columnar(
    const QueryMemoryDescriptor& query_mem_desc,
    int8_t* projection_buffer,
    const size_t projection_count) {
  // the first column (row indices) remains unchanged.
  CHECK(projection_count <= query_mem_desc.getEntryCount());
  constexpr size_t row_index_width = sizeof(int64_t);
  size_t buffer_offset1{projection_count * row_index_width};
  // other columns are actual non-lazy columns for the projection:
  for (size_t i = 0; i < query_mem_desc.getSlotCount(); i++) {
    if (query_mem_desc.getPaddedSlotWidthBytes(i) > 0) {
      auto column_proj_size =
          projection_count * query_mem_desc.getPaddedSlotWidthBytes(i);
      auto buffer_offset2 = query_mem_desc.getColOffInBytes(i);
      if (buffer_offset1 + column_proj_size >= buffer_offset2) {
        // overlapping
        std::memmove(projection_buffer + buffer_offset1,
                     projection_buffer + buffer_offset2,
                     column_proj_size);
      } else {
        std::memcpy(projection_buffer + buffer_offset1,
                    projection_buffer + buffer_offset2,
                    column_proj_size);
      }
      buffer_offset1 += align_to_int64(column_proj_size);
    }
  }
}

}  // namespace

void QueryMemoryInitializer::compactProjectionBuffersCpu(
    const QueryMemoryDescriptor& query_mem_desc,
    const size_t projection_count) {
  const auto num_allocated_rows =
      std::min(projection_count, query_mem_desc.getEntryCount());
  const size_t buffer_start_idx = query_mem_desc.hasVarlenOutput() ? 1 : 0;

  // copy the results from the main buffer into projection_buffer
  compact_projection_buffer_for_cpu_columnar(
      query_mem_desc,
      reinterpret_cast<int8_t*>(group_by_buffers_[buffer_start_idx]),
      num_allocated_rows);

  // update the entry count for the result set, and its underlying storage
  CHECK(!result_sets_.empty());
  result_sets_.front()->updateStorageEntryCount(num_allocated_rows);
}

void QueryMemoryInitializer::compactProjectionBuffersGpu(
    const QueryMemoryDescriptor& query_mem_desc,
    BufferProvider* buffer_provider,
    const GpuGroupByBuffers& gpu_group_by_buffers,
    const size_t projection_count,
    const int device_id) {
  CHECK(!use_hash_table_desc_);

  // store total number of allocated rows:
  const auto num_allocated_rows =
      std::min(projection_count, query_mem_desc.getEntryCount());

  // copy the results from the main buffer into projection_buffer
  const size_t buffer_start_idx = query_mem_desc.hasVarlenOutput() ? 1 : 0;
  copy_projection_buffer_from_gpu_columnar(
      buffer_provider,
      gpu_group_by_buffers,
      query_mem_desc,
      reinterpret_cast<int8_t*>(group_by_buffers_[buffer_start_idx]),
      num_allocated_rows,
      device_id);

  // update the entry count for the result set, and its underlying storage
  CHECK(!result_sets_.empty());
  result_sets_.front()->updateStorageEntryCount(num_allocated_rows);
}

void QueryMemoryInitializer::copyGroupByBuffersFromGpu(
    BufferProvider* buffer_provider,
    const QueryMemoryDescriptor& query_mem_desc,
    const size_t entry_count,
    const GpuGroupByBuffers& gpu_group_by_buffers,
    const RelAlgExecutionUnit* ra_exe_unit,
    const unsigned block_size_x,
    const unsigned grid_size_x,
    const int device_id,
    const bool prepend_index_buffer) const {
  const auto thread_count = block_size_x * grid_size_x;

  size_t total_buff_size{0};
  if (ra_exe_unit && query_mem_desc.useStreamingTopN()) {
    const size_t n = ra_exe_unit->sort_info.offset + ra_exe_unit->sort_info.limit;
    total_buff_size =
        streaming_top_n::get_heap_size(query_mem_desc.getRowSize(), n, thread_count);
  } else {
    total_buff_size =
        query_mem_desc.getBufferSizeBytes(ExecutorDeviceType::GPU, entry_count);
  }
  CHECK(!use_hash_table_desc_);
  copy_group_by_buffers_from_gpu(buffer_provider,
                                 group_by_buffers_,
                                 total_buff_size,
                                 gpu_group_by_buffers.data,
                                 query_mem_desc,
                                 block_size_x,
                                 grid_size_x,
                                 device_id,
                                 prepend_index_buffer,
                                 query_mem_desc.hasVarlenOutput());
}

void QueryMemoryInitializer::applyStreamingTopNOffsetCpu(
    const QueryMemoryDescriptor& query_mem_desc,
    const RelAlgExecutionUnit& ra_exe_unit) {
  const size_t buffer_start_idx = query_mem_desc.hasVarlenOutput() ? 1 : 0;
  CHECK_EQ(group_by_buffers_.size(), buffer_start_idx + 1);

  const auto rows_copy = streaming_top_n::get_rows_copy_from_heaps(
      group_by_buffers_[buffer_start_idx],
      query_mem_desc.getBufferSizeBytes(ra_exe_unit, 1, ExecutorDeviceType::CPU),
      ra_exe_unit.sort_info.offset + ra_exe_unit.sort_info.limit,
      1);
  CHECK_EQ(rows_copy.size(),
           query_mem_desc.getEntryCount() * query_mem_desc.getRowSize());
  memcpy(group_by_buffers_[buffer_start_idx], &rows_copy[0], rows_copy.size());
}

void QueryMemoryInitializer::applyStreamingTopNOffsetGpu(
    BufferProvider* buffer_provider,
    const QueryMemoryDescriptor& query_mem_desc,
    const GpuGroupByBuffers& gpu_group_by_buffers,
    const RelAlgExecutionUnit& ra_exe_unit,
    const unsigned total_thread_count,
    const int device_id) {
#ifdef HAVE_CUDA
  CHECK(!use_hash_table_desc_);

  CHECK_EQ(group_by_buffers_.size(), num_buffers_);
  const size_t buffer_start_idx = query_mem_desc.hasVarlenOutput() ? 1 : 0;

  const auto rows_copy = pick_top_n_rows_from_dev_heaps(
      buffer_provider,
      reinterpret_cast<int64_t*>(gpu_group_by_buffers.data),
      ra_exe_unit,
      query_mem_desc,
      total_thread_count,
      device_id);
  CHECK_EQ(
      rows_copy.size(),
      static_cast<size_t>(query_mem_desc.getEntryCount() * query_mem_desc.getRowSize()));
  memcpy(group_by_buffers_[buffer_start_idx], &rows_copy[0], rows_copy.size());
#else
  UNREACHABLE();
#endif
}

std::shared_ptr<VarlenOutputInfo> QueryMemoryInitializer::getVarlenOutputInfo() {
  if (varlen_output_info_) {
    return varlen_output_info_;
  }

  // shared_ptr so that both the ResultSet and QMI can hold on to the varlen info object
  // and update it as needed
  varlen_output_info_ = std::make_shared<VarlenOutputInfo>(VarlenOutputInfo{
      static_cast<int64_t>(varlen_output_buffer_), varlen_output_buffer_host_ptr_});
  return varlen_output_info_;
}
