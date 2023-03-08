//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_context.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  this->table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() { 
  child_executor_->Init(); 
  table_indexes_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    for (auto index_info : table_indexes_) {
      const auto key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
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
