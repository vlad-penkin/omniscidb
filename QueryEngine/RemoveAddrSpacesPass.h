#pragma once
#include <iostream>
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/InstVisitor.h"
#include "llvm/Pass.h"
#include "llvm/Support/Casting.h"
#include "llvm/Support/raw_ostream.h"

struct RemoveAddrSpacesPass : llvm::ModulePass {
  static char ID;
  RemoveAddrSpacesPass() : llvm::ModulePass(ID) {}

  static bool isPointerWithNonZeroAddrSpace(llvm::Type* type) {
    llvm::PointerType* ptr_type = llvm::dyn_cast<llvm::PointerType>(type);
    if (!ptr_type) {
      return false;
    }
    return ptr_type->getAddressSpace() != 0;
  }

  static bool requireConversion(llvm::FunctionType* sig) {
    if (isPointerWithNonZeroAddrSpace(sig->getReturnType())) {
      return true;
    }
    if (llvm::any_of(sig->params(), isPointerWithNonZeroAddrSpace)) {
      return true;
    }
    return false;
  }

  static llvm::Type* removeAddrSpace(llvm::Type* type) {
    llvm::PointerType* ptr_type = llvm::dyn_cast<llvm::PointerType>(type);
    if (!ptr_type) {
      return type;
    }
    return llvm::PointerType::get(removeAddrSpace(ptr_type->getElementType()), 0);
  }

  static llvm::FunctionType* convertFunctionSignature(llvm::FunctionType* old_sig) {
    llvm::SmallVector<llvm::Type*, 16> params(
        llvm::map_range(old_sig->params(), removeAddrSpace));
    llvm::FunctionType* res =
        llvm::FunctionType::get(removeAddrSpace(old_sig->getReturnType()), params, false);
    return res;
  }

  void convertInstruction(llvm::Instruction* inst,
                          std::vector<llvm::Instruction*>& erase_list) {
    if (auto asc = llvm::dyn_cast<llvm::AddrSpaceCastInst>(inst)) {
      asc->replaceAllUsesWith(asc->getPointerOperand());
      erase_list.push_back(asc);
      return;
    }
    if (auto alloc = llvm::dyn_cast<llvm::AllocaInst>(inst)) {
      alloc->setAllocatedType(removeAddrSpace(alloc->getAllocatedType()));
      return;
    }
    if (auto gep = llvm::dyn_cast<llvm::GetElementPtrInst>(inst)) {
      gep->setSourceElementType(removeAddrSpace(gep->getSourceElementType()));
      gep->setResultElementType(removeAddrSpace(gep->getResultElementType()));
      return;
    }
    if (auto call = llvm::dyn_cast<llvm::CallInst>(inst)) {
      call->mutateFunctionType(convertFunctionSignature(call->getFunctionType()));
    }

    inst->mutateType(removeAddrSpace(inst->getType()));
    for (auto op_iter = inst->op_begin(); op_iter != inst->op_end(); ++op_iter) {
      llvm::Value* op = *op_iter;
      if (llvm::isa<llvm::ConstantData>(op)) {
        if (auto null_ptr = llvm::dyn_cast<llvm::ConstantPointerNull>(op)) {
          *op_iter = llvm::ConstantPointerNull::get(
              llvm::cast<llvm::PointerType>(removeAddrSpace(null_ptr->getType())));
        } else {
          // We don't need to change non pointers type
          continue;
        }
      } else {
        op->mutateType(removeAddrSpace(op->getType()));
      }
    }
  }

  bool runOnModule(llvm::Module& module) override {
    bool converted = false;
    std::vector<llvm::Function*> dead_funcs;
    for (auto& f : module) {
      if (!requireConversion(f.getFunctionType())) {
        continue;
      }
      converted = true;
      dead_funcs.emplace_back(&f);
    }

    for (auto& f : dead_funcs) {
      f->removeFromParent();  // TODO(L0): Maybe erase

      auto nf = llvm::Function::Create(convertFunctionSignature(f->getFunctionType()),
                                       f->getLinkage(),
                                       f->getAddressSpace(),
                                       f->getName(),
                                       &module);

      nf->setAttributes(f->getAttributes());

      for (int i = 0; i < f->arg_size(); i++) {
        f->getArg(i)->replaceAllUsesWith(nf->getArg(i));
      }

      nf->getBasicBlockList().splice(nf->begin(), f->getBasicBlockList());
      f->replaceAllUsesWith(nf);
    }

    std::vector<llvm::Instruction*> dead_instructions;
    for (auto& f : module) {
      for (llvm::BasicBlock& b : f.getBasicBlockList()) {
        for (llvm::Instruction& i : b) {
          convertInstruction(&i, dead_instructions);
        }
      }
    }

    for (auto i : dead_instructions) {
      i->eraseFromParent();
    }

    return converted;
  }
};

char RemoveAddrSpacesPass::ID = 0;

llvm::Pass* createRemoveAddrSpacesPass() {
  return new RemoveAddrSpacesPass();
};