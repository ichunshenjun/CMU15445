//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {
// exec_ctx提供上下文信息，plan提供执行查询操作所需的所有信息
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      auto lock_success = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!lock_success) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == table_info_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      auto lock_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
      for (auto rid : lock_row_set) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, rid);
      }
      exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
    }
    return false;
  }
  *tuple = *table_iter_;
  *rid = table_iter_->GetRid();
  ++table_iter_;
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      auto lock_success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                               LockManager::LockMode::SHARED, table_info_->oid_, *rid);
      if (!lock_success) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }
  return true;
}
}  // namespace bustub
