#pragma once

#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Pass.h"

namespace {
struct MyIntrinsicsCleanupPass: public llvm::FunctionPass
{
  static char ID;
  MyIntrinsicsCleanupPass() : llvm::FunctionPass(ID) {}
  bool runOnFunction(llvm::Function &F) override;
};
char MyIntrinsicsCleanupPass::ID = 0;
}

namespace llvm {
  FunctionPass* createMyIntrinsicsCleanupPass();
}