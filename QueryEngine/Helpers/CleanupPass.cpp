#include "QueryEngine/Helpers/CleanupPass.h"
#include "llvm/IR/IntrinsicInst.h"
#include "llvm/IR/Intrinsics.h"
#include "llvm/Transforms/Utils/BasicBlockUtils.h"
#include "llvm/IR/Constants.h"

#include <string>

using namespace llvm;

static bool isUnsupported(llvm::StringRef name) {
  return name == "llvm.memmove";
}

bool MyIntrinsicsCleanupPass::runOnFunction(llvm::Function& F) {
  bool changed = false;
  for (auto BB = F.begin(); BB != F.end(); ++BB) {
    for (auto Inst = BB->begin(); Inst != BB->end(); ++Inst) {
      auto* intrin = dyn_cast<IntrinsicInst>(Inst);
      if (!intrin)
        continue;

      std::string name = Intrinsic::getName(intrin->getIntrinsicID());
      if (isUnsupported(name)) {
        Value* V = ConstantInt::get(Type::getInt32Ty(BB->getContext()), 0);
        Instruction* Nop = BinaryOperator::CreateAdd(V, V, "nop");
        ReplaceInstWithInst(Inst->getParent()->getInstList(), Inst, Nop);
      }
    }
  }

  return changed;
}

namespace llvm {
  FunctionPass* createMyIntrinsicsCleanupPass() {
    return new MyIntrinsicsCleanupPass();
  }
}