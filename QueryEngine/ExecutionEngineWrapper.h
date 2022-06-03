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

#pragma once

#include "Logger/Logger.h"
#include "QueryEngine/LLVMGlobalContext.h"

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/IR/Module.h>

struct CompilationOptions;

#ifdef ENABLE_ORCJIT

#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>

inline std::string llvmErrorToString(const llvm::Error& err) {
  std::string msg;
  llvm::raw_string_ostream os(msg);
  os << err;
  return msg;
};

/**
 * LLVM ORC (at least in LLVM 9.0.1) doesn't deregister sections with EH frames
 * on module deallocation. It leads to dangling pointers in EH unwinder. Later,
 * memory of those sections can be re-allocated and re-written with random data.
 * The next raised exception will cause unwinder to read those registered sections
 * and random data in it might cause various failures.
 *
 * This wrapper of SectionMemoryManager is passed to the object layer. It will be
 * used by ORC to register EH frames for materialized modules. In its destructor we
 * deregister all registered EH frames. Original class keeps track of all registered
 * EH frames and it is safe to call deregisterEHFrames multiple times. So, even if
 * newer ORC version would deregister EH frames automatically, it would still be safe
 * to use this wrapper.
 */
class SectionMemoryManagerWithEHCleanup : public llvm::SectionMemoryManager {
 public:
  ~SectionMemoryManagerWithEHCleanup() override { deregisterEHFrames(); }
};

class ORCJITExecutionEngineWrapper {
 public:
  ORCJITExecutionEngineWrapper();
  ORCJITExecutionEngineWrapper(
      std::unique_ptr<llvm::orc::ExecutionSession> execution_session,
      llvm::orc::JITTargetMachineBuilder target_machine_builder,
      std::unique_ptr<llvm::DataLayout> data_layout)
      : execution_session_(std::move(execution_session))
      , data_layout_(std::move(data_layout))
      , mangle_(std::make_unique<llvm::orc::MangleAndInterner>(*this->execution_session_,
                                                               *data_layout_))
      , object_layer_(std::make_unique<llvm::orc::RTDyldObjectLinkingLayer>(
            *execution_session_,
            []() { return std::make_unique<SectionMemoryManagerWithEHCleanup>(); }))
      , compiler_layer_(std::make_unique<llvm::orc::IRCompileLayer>(
            *execution_session_,
            *object_layer_,
            std::make_unique<llvm::orc::ConcurrentIRCompiler>(
                std::move(target_machine_builder)))) {
    auto dylib_or_error = execution_session_->createJITDylib("<main>");
    if (!dylib_or_error) {
      LOG(FATAL) << "Failed to initialize JITTargetMachineBuilder: "
                 << llvmErrorToString(dylib_or_error.takeError());
    }
    main_dylib_ = &(*dylib_or_error);
    dylib_or_error->addGenerator(
        llvm::cantFail(llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(
            data_layout_->getGlobalPrefix())));
  }

  ORCJITExecutionEngineWrapper(const ORCJITExecutionEngineWrapper& other) = delete;
  ORCJITExecutionEngineWrapper(ORCJITExecutionEngineWrapper&& other) = default;

  void addModule(std::unique_ptr<llvm::Module> module) {
    module->setDataLayout(*data_layout_);
    llvm::orc::ThreadSafeModule tsm(std::move(module), getGlobalLLVMThreadSafeContext());
    auto err = compiler_layer_->add(*main_dylib_, std::move(tsm));
    if (err) {
      LOG(FATAL) << "Cannot add LLVM module: " << llvmErrorToString(err);
    }
  }

  void* getPointerToFunction(llvm::Function* function) {
    CHECK(function);
    CHECK(execution_session_);
    auto symbol =
        execution_session_->lookup({main_dylib_}, (*mangle_)(function->getName()));
    if (!symbol) {
      LOG(FATAL) << "Failed to find function " << std::string(function->getName())
                 << "\nError: " << llvmErrorToString(symbol.takeError());
    }
    return reinterpret_cast<void*>(symbol->getAddress());
  }

  bool exists() const { return !(execution_session_ == nullptr); }

  void removeModule(llvm::Module* module) {
    // Do nothing here. Module is deleted by ORC after materialization.
  }

  ORCJITExecutionEngineWrapper& operator=(const ORCJITExecutionEngineWrapper& other) =
      delete;
  ORCJITExecutionEngineWrapper& operator=(ORCJITExecutionEngineWrapper&& other) = default;

 private:
  std::unique_ptr<llvm::orc::ExecutionSession> execution_session_;
  std::unique_ptr<llvm::DataLayout> data_layout_;
  std::unique_ptr<llvm::orc::MangleAndInterner> mangle_;
  std::unique_ptr<llvm::orc::RTDyldObjectLinkingLayer> object_layer_;
  std::unique_ptr<llvm::orc::IRCompileLayer> compiler_layer_;
  std::unique_ptr<llvm::JITEventListener> intel_jit_listener_;
  llvm::orc::JITDylib* main_dylib_ = nullptr;
};

using ExecutionEngineWrapper = ORCJITExecutionEngineWrapper;
#else
class MCJITExecutionEngineWrapper {
 public:
  MCJITExecutionEngineWrapper();
  MCJITExecutionEngineWrapper(llvm::ExecutionEngine* execution_engine,
                              const CompilationOptions& co);

  MCJITExecutionEngineWrapper(const MCJITExecutionEngineWrapper& other) = delete;
  MCJITExecutionEngineWrapper(MCJITExecutionEngineWrapper&& other) = default;

  void* getPointerToFunction(llvm::Function* function) {
    CHECK(execution_engine_);
    return execution_engine_->getPointerToFunction(function);
  }

  void finalize() {
    CHECK(execution_engine_);
    execution_engine_->finalizeObject();
  }

  bool exists() const { return !(execution_engine_ == nullptr); }

  void removeModule(llvm::Module* module) { execution_engine_->removeModule(module); }

  llvm::ExecutionEngine* operator->() { return execution_engine_.get(); }
  const llvm::ExecutionEngine* operator->() const { return execution_engine_.get(); }

  MCJITExecutionEngineWrapper& operator=(const MCJITExecutionEngineWrapper& other) =
      delete;
  MCJITExecutionEngineWrapper& operator=(MCJITExecutionEngineWrapper&& other) = default;

  MCJITExecutionEngineWrapper& operator=(llvm::ExecutionEngine* execution_engine);

 private:
  std::unique_ptr<llvm::ExecutionEngine> execution_engine_;
  std::unique_ptr<llvm::JITEventListener> intel_jit_listener_;
};

using ExecutionEngineWrapper = MCJITExecutionEngineWrapper;
#endif
