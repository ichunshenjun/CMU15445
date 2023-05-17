//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    std::vector<RID> result;
    auto vals = plan_->KeyPredicate()->Evaluate(tuple, child_executor_->GetOutputSchema());
    Tuple key = Tuple{{vals}, &index_info_->key_schema_};
    index_info_->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
    std::vector<Value> value;
    Tuple inner_tuple{};
    if (!result.empty()) {
      table_info_->table_->GetTuple(result[0], &inner_tuple, exec_ctx_->GetTransaction());
      for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        value.push_back(tuple->GetValue(&child_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
        value.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), idx));
      }
      *tuple = Tuple{value, &GetOutputSchema()};
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        value.push_back(tuple->GetValue(&child_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
        value.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumns()[idx].GetType()));
      }
      *tuple = Tuple{value, &GetOutputSchema()};
      return true;
    }
  }
  return false;
  ;
}

}  // namespace bustub
