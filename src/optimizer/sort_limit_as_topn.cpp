#include <memory>
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if(optimized_plan->GetType() == PlanType::Limit){
    const auto &lim_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    if(lim_plan.GetChildPlan()->GetType()==PlanType::Sort){
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*lim_plan.GetChildPlan());
      return std::make_shared<TopNPlanNode>(lim_plan.output_schema_,sort_plan.GetChildAt(0),sort_plan.GetOrderBy(),lim_plan.GetLimit());
    }
  }
  return plan;
}
}  // namespace bustub
