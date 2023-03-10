#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple{};
  RID rid{};
  child_executor_->Init();
  sorted_tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  std::sort(
      sorted_tuples_.begin(), sorted_tuples_.end(),
      [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                     const Tuple &tuple_b) {
        for (const auto &order_key : order_bys) {
          switch (order_key.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
            case OrderByType::DESC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
          }
        }
        return false;
      });
  sorted_iterator_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_iterator_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *sorted_iterator_;
  *rid = tuple->GetRid();
  ++sorted_iterator_;
  return true;
}

}  // namespace bustub
