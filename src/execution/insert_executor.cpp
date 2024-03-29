//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "concurrency/lock_manager.h"
#include "execution/executors/insert_executor.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  this->table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    auto lock_success = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!lock_success) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto insert_success = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    if (insert_success) {
      try {
        auto lock_success = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
        if (!lock_success) {
          throw ExecutionException("Insert Executor Get Table Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
    }

    for (auto index_info : table_indexes_) {
      const auto key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, cnt);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
