
#include "QueryEngine/Kernel.h"

void Kernel::run(Executor* executor,
                 const size_t thread_idx,
                 SharedKernelContext& shared_context) {
  legacy_kernel.run(executor, thread_idx, shared_context);
}
