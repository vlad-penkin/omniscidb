#include "HashJoinRuntime.h"

//template <typename F, typename... ARGS>
void l0_kernel_launch_wrapper(/*F func, ARGS&&... args*/) {
  std::cout << "Im in wrapper caller!" << std::endl;
}

void init_hash_join_buff_on_device(int32_t* buff,
                                   const int64_t hash_entry_count,
                                   const int32_t invalid_slot_val) {
  // cuda_kernel_launch_wrapper(
  //     init_hash_join_buff_wrapper, buff, hash_entry_count, invalid_slot_val);
  l0_kernel_launch_wrapper();
}
