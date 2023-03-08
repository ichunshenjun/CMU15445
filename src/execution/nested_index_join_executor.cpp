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
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_excutor_(std::move(child_executor)){
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get()); 
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
  child_excutor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  Tuple left_tuple{};
  RID rid_temp{};
  std::vector<Value> vals;
  while(child_excutor_->Next(&left_tuple , &rid_temp)){
  auto value=plan_->KeyPredicate()->Evaluate(&left_tuple, child_excutor_->GetOutputSchema());
  std::vector<RID> rids;
  tree_->ScanKey(Tuple{{value},index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());
  Tuple right_tuple{};
  if(!rids.empty()){
    table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
    for(uint32_t idx=0;idx<child_excutor_->GetOutputSchema().GetColumnCount();idx++){
      vals.push_back(left_tuple.GetValue(&child_excutor_->GetOutputSchema(), idx));
    }
    for(uint32_t idx=0;idx<plan_->InnerTableSchema().GetColumnCount();idx++){
      vals.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), idx));
    }
    *tuple=Tuple(vals,&GetOutputSchema());
    return true;
  }
  if(plan_->GetJoinType()==JoinType::LEFT){
    for(uint32_t idx=0;idx<child_excutor_->GetOutputSchema().GetColumnCount();idx++){
      vals.push_back(left_tuple.GetValue(&child_excutor_->GetOutputSchema(), idx));
    }
    for(uint32_t idx=0;idx<plan_->InnerTableSchema().GetColumnCount();idx++){
      vals.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(idx).GetType()));
    }
    *tuple=Tuple(vals,&GetOutputSchema());
    return true;
  }
  }
  return false;
}

}  // namespace bustub
