// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // nested_loop_join_executor.cpp
// //
// // Identification: src/execution/nested_loop_join_executor.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "execution/executors/nested_loop_join_executor.h"
// #include <vector>
// #include "binder/table_ref/bound_join_ref.h"
// #include "type/value_factory.h"

// namespace bustub {

// NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
//                                                std::unique_ptr<AbstractExecutor> &&left_executor,
//                                                std::unique_ptr<AbstractExecutor> &&right_executor)
//     : AbstractExecutor(exec_ctx),
//       plan_(plan),
//       left_executor_(std::move(left_executor)),
//       right_executor_(std::move(right_executor)) {}

// void NestedLoopJoinExecutor::Init() {
//   left_executor_->Init();
//   right_executor_->Init();
// }

// auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   RID left_rid{};
//   RID right_rid{};
//   while (left_tuple_.IsValid() || left_executor_->Next(&left_tuple_, &left_rid)) {
//     if (right_tuple_.IsValid()) {
//       right_executor_->Init();
//     }
//     while (right_executor_->Next(&right_tuple_, &right_rid)) {
//       if (Matched(&left_tuple_, &right_tuple_)) {
//         std::vector<Value> vals;
//         for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
//           vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col_idx));
//         }
//         for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
//           vals.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), col_idx));
//         }
//         *tuple = Tuple(vals, &GetOutputSchema());
//         right_tuple_.SetValid(false);
//         left_tuple_.SetValid(true);
//         return true;
//       }
//     }
//     if (plan_->GetJoinType() == JoinType::LEFT && !left_tuple_.IsValid()) {
//       std::vector<Value> vals;
//       for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
//         vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col_idx));
//       }
//       for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
//         vals.push_back(
//             ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(col_idx).GetType()));
//       }
//       *tuple = Tuple(vals, &GetOutputSchema());
//       right_tuple_.SetValid(true);
//       left_tuple_.SetValid(false);
//       return true;
//     }
//     left_tuple_.SetValid(false);
//     right_tuple_.SetValid(true);
//   }
//   return false;
// }
// auto NestedLoopJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
//   auto value = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
//                                                right_executor_->GetOutputSchema());
//   return !value.IsNull() && value.GetAs<bool>();
// }
// }  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  RID rid{};
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&left_tuple_, &rid);
  // right_executor_->Next(&right_tuple_,&rid);
  // left_executor_->Init();
  // right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID rid_temp{};
  // while(true){
  //   if (left_tuple_ == nullptr || right_tuple_ == nullptr) {
  //     return false;
  //   }
  while (true) {
    std::vector<Value> vals;
    if (right_executor_->Next(&right_tuple_, &rid_temp)) {
      if (Matched(&left_tuple_, &right_tuple_)) {
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        match_flag_ = 1;
        return true;
      }
    } else {
      if (match_flag_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        if (left_executor_->Next(&left_tuple_, &rid_temp)) {
          right_executor_->Init();
          match_flag_ = -1;
        } else {
          match_flag_ = 0;
          return true;
        }
        return true;
      }
      if (match_flag_ == 0) {
        return false;
      }
      if (left_executor_->Next(&left_tuple_, &rid_temp)) {
        right_executor_->Init();
        match_flag_ = -1;
      } else {
        return false;
      }
    }
  }
}
auto NestedLoopJoinExecutor::Matched(Tuple *left_tuple, Tuple *right_tuple) const -> bool {
  auto value = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                               right_executor_->GetOutputSchema());

  return !value.IsNull() && value.GetAs<bool>();
}

}  // namespace bustub
