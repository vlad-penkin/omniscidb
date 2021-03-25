#include "CodeGenerator.h"
#include "ScalarExprVisitor.h"
#include "QueryEngine/Helpers/CleanupPass.h"

#include "llvm/IR/LegacyPassManager.h"
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar/InstSimplifyPass.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/Scalar.h>
#include "LLVMSPIRVLib/LLVMSPIRVLib.h"

// duplicate code for now
namespace {

class UsedColumnExpressions : public ScalarExprVisitor<SpirvCodeGenerator::ColumnMap> {
 protected:
  SpirvCodeGenerator::ColumnMap visitColumnVar(
      const Analyzer::ColumnVar* column) const override {
    SpirvCodeGenerator::ColumnMap m;
    InputColDescriptor input_desc(
        column->get_column_id(), column->get_table_id(), column->get_rte_idx());
    m.emplace(input_desc,
              std::static_pointer_cast<Analyzer::ColumnVar>(column->deep_copy()));
    return m;
  }

  SpirvCodeGenerator::ColumnMap aggregateResult(
      const SpirvCodeGenerator::ColumnMap& aggregate,
      const SpirvCodeGenerator::ColumnMap& next_result) const override {
    auto result = aggregate;
    result.insert(next_result.begin(), next_result.end());
    return result;
  }
};

std::vector<InputTableInfo> g_table_infos;

llvm::Type* llvm_type_from_sql(const SQLTypeInfo& ti, llvm::LLVMContext& ctx) {
  switch (ti.get_type()) {
    case kINT: {
      return get_int_type(32, ctx);
    }
    default: {
      LOG(FATAL) << "Unsupported type";
      return nullptr;  // satisfy -Wreturn-type
    }
  }
}

void eliminate_dead_self_recursive_funcs(
    llvm::Module& M,
    const std::unordered_set<llvm::Function*>& live_funcs) {
  std::vector<llvm::Function*> dead_funcs;
  for (auto& F : M) {
    bool bAlive = false;
    if (live_funcs.count(&F)) {
      continue;
    }
    for (auto U : F.users()) {
      auto* C = llvm::dyn_cast<const llvm::CallInst>(U);
      if (!C || C->getParent()->getParent() != &F) {
        bAlive = true;
        break;
      }
    }
    if (!bAlive) {
      dead_funcs.push_back(&F);
    }
  }
  for (auto pFn : dead_funcs) {
    pFn->eraseFromParent();
  }
}

}  // namespace

SpirvCodeGenerator::ColumnMap SpirvCodeGenerator::prepare(const Analyzer::Expr* expr) {
  UsedColumnExpressions visitor;
  const auto used_columns = visitor.visit(expr);
  std::list<std::shared_ptr<const InputColDescriptor>> global_col_ids;
  for (const auto& used_column : used_columns) {
    global_col_ids.push_back(std::make_shared<InputColDescriptor>(
        used_column.first.getColId(),
        used_column.first.getScanDesc().getTableId(),
        used_column.first.getScanDesc().getNestLevel()));
  }
  plan_state_->allocateLocalColumnIds(global_col_ids);
  return used_columns;
}

SpirvCodeGenerator::CompiledExpression SpirvCodeGenerator::compile(
    const Analyzer::Expr* expr,
    const bool fetch_columns,
    const CompilationOptions& co) {
  assert(co.device_type == ExecutorDeviceType::iGPU);
  module_->setTargetTriple("spir-unknown-unknown");
  own_plan_state_ = std::make_unique<PlanState>(
      false, std::vector<InputTableInfo>{}, PlanState::DeletedColumnsMap{}, nullptr);
  plan_state_ = own_plan_state_.get();
  const auto used_columns = prepare(expr);
  std::vector<llvm::Type*> arg_types(plan_state_->global_to_local_col_ids_.size() + 1);
  std::vector<std::shared_ptr<Analyzer::ColumnVar>> inputs(arg_types.size() - 1);
  auto& ctx = module_->getContext();
  for (const auto& kv : plan_state_->global_to_local_col_ids_) {
    size_t arg_idx = kv.second;
    CHECK_LT(arg_idx, arg_types.size());
    const auto it = used_columns.find(kv.first);
    const auto col_expr = it->second;
    inputs[arg_idx] = col_expr;
    const auto& ti = col_expr->get_type_info();
    arg_types[arg_idx + 1] = llvm_type_from_sql(ti, ctx);
  }
  arg_types[0] =
      llvm::PointerType::get(llvm_type_from_sql(expr->get_type_info(), ctx), 0);
  auto ft = llvm::FunctionType::get(get_int_type(32, ctx), arg_types, false);
  auto scalar_expr_func = llvm::Function::Create(
      ft, llvm::Function::ExternalLinkage, "scalar_expr", module_.get());
  auto bb_entry = llvm::BasicBlock::Create(ctx, ".entry", scalar_expr_func, 0);
  own_cgen_state_ = std::make_unique<CgenState>(g_table_infos.size(), false);
  own_cgen_state_->module_ = module_.get();
  own_cgen_state_->row_func_ = own_cgen_state_->current_func_ = scalar_expr_func;
  own_cgen_state_->ir_builder_.SetInsertPoint(bb_entry);
  cgen_state_ = own_cgen_state_.get();
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto expr_lvs = codegen(expr, fetch_columns, co);
  CHECK_EQ(expr_lvs.size(), size_t(1));
  cgen_state_->ir_builder_.CreateStore(expr_lvs.front(),
                                       cgen_state_->row_func_->arg_begin());
  cgen_state_->ir_builder_.CreateRet(ll_int<int32_t>(0, ctx));
  //   if (co.device_type == ExecutorDeviceType::iGPU) {
  std::vector<llvm::Type*> wrapper_arg_types(arg_types.size() + 1);
  wrapper_arg_types[0] = llvm::PointerType::get(get_int_type(32, ctx), 0);
  wrapper_arg_types[1] = arg_types[0];
  for (size_t i = 1; i < arg_types.size(); ++i) {
    wrapper_arg_types[i + 1] = llvm::PointerType::get(arg_types[i], 0);
  }
  auto wrapper_ft =
      llvm::FunctionType::get(llvm::Type::getVoidTy(ctx), wrapper_arg_types, false);
  auto wrapper_scalar_expr_func = llvm::Function::Create(
      wrapper_ft, llvm::Function::ExternalLinkage, "wrapper_scalar_expr", module_.get());
  auto wrapper_bb_entry =
      llvm::BasicBlock::Create(ctx, ".entry", wrapper_scalar_expr_func, 0);
  llvm::IRBuilder<> b(ctx);
  b.SetInsertPoint(wrapper_bb_entry);
  std::vector<llvm::Value*> loaded_args = {wrapper_scalar_expr_func->arg_begin() + 1};
  for (size_t i = 2; i < wrapper_arg_types.size(); ++i) {
    loaded_args.push_back(b.CreateLoad(wrapper_scalar_expr_func->arg_begin() + i));
  }
  auto error_lv = b.CreateCall(scalar_expr_func, loaded_args);
  b.CreateStore(error_lv, wrapper_scalar_expr_func->arg_begin());
  b.CreateRetVoid();
//   scalar_expr_func->getParent()->dump();
  return {scalar_expr_func, wrapper_scalar_expr_func, inputs};
  //   }
}

std::string SpirvCodeGenerator::generateSpirv(
    const SpirvCodeGenerator::CompiledExpression& compiled_expression,
    const CompilationOptions& co) {
  if (!l0_mgr_) {
    l0_mgr_ = std::make_unique<L0Mgr_Namespace::L0Mgr>();
  }

  SPIRV::TranslatorOpts opts;
  opts.enableAllExtensions();
  opts.setDesiredBIsRepresentation(SPIRV::BIsRepresentation::OpenCL12);
  opts.setDebugInfoEIS(SPIRV::DebugInfoEIS::OpenCL_DebugInfo_100);

  // auto live_funcs = CodeGenerator::markDeadRuntimeFuncs(*module_.get(), {compiled_expression.func}, {compiled_expression.wrapper_func});
  std::unordered_set<llvm::Function*> roots = {compiled_expression.func, compiled_expression.wrapper_func};
  std::unordered_set<llvm::Function*> live_funcs;
  live_funcs.insert(roots.begin(), roots.end());

  for (const llvm::Function* F: roots) {
    for(const llvm::BasicBlock& BB : *F) {
      for(const llvm::Instruction& I : BB) {
        if (const llvm::CallInst* CI = llvm::dyn_cast<const llvm::CallInst>(&I)) {
          live_funcs.insert(CI->getCalledFunction());
        }
      }
    }
  }

  for (llvm::Function& F : *module_) {
    if (!live_funcs.count(&F) && !F.isDeclaration()) {
      F.setLinkage(llvm::GlobalValue::InternalLinkage);
    }
  }

  std::error_code ec;
  llvm::raw_fd_ostream funopt("unopt.ir", ec, llvm::sys::fs::F_None);
  module_->print(funopt, nullptr);

  llvm::legacy::PassManager PM;
  PM.add(llvm::createAlwaysInlinerLegacyPass());
  PM.add(llvm::createPromoteMemoryToRegisterPass());
  PM.add(llvm::createInstSimplifyLegacyPass());
  PM.add(llvm::createInstructionCombiningPass());
  PM.add(llvm::createGlobalOptimizerPass());
  PM.add(llvm::createLICMPass());
  // PM.add(llvm::createMyIntrinsicsCleanupPass());
  PM.run(*module_.get());

  llvm::raw_fd_ostream fopt("opt.ir", ec, llvm::sys::fs::F_None);
  module_->print(fopt, nullptr);

  eliminate_dead_self_recursive_funcs(*module_.get(), live_funcs);
  llvm::raw_fd_ostream fdce("dce.ir", ec, llvm::sys::fs::F_None);
  module_->print(fdce, nullptr);

  compiled_expression.wrapper_func->setCallingConv(llvm::CallingConv::SPIR_KERNEL);
  // auto bb = llvm::BasicBlock::Create(module_->getContext(), ".entry", F, nullptr);
  // llvm::IRBuilder<> b(module_->getContext());
  // b.SetInsertPoint(bb);
  // b.CreateRetVoid();


  std::ostringstream ss;
  std::string err;
  auto success = writeSpirv(module_.get(), opts, ss, err);
  if (!success) {
    llvm::errs() << "Spirv translation failed with error: " << err << "\n";
  } else {
    llvm::errs() << "Spirv tranlsation success.\n";
  }
  return ss.str();
}