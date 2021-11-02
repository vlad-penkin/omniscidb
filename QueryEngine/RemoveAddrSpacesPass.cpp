#include "RemoveAddrSpacesPass.h"

llvm::Pass* createRemoveAddrSpacesPass() {
  return new RemoveAddrSpacesPass();
};

char RemoveAddrSpacesPass::ID = 0;