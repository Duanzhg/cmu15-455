#include "optimizer/optimizer.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for(const auto& child : plan->GetChildren()){
      children.push_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if(optimized_plan->GetType() == PlanType::Limit && optimized_plan->children_[0]->GetType() == PlanType::Sort){
      const auto &limit_plan = dynamic_cast<LimitPlanNode &>(*optimized_plan);
     // const auto &sort_plan = dynamic_cast<SortPlanNode &>(*(optimized_plan->children_[0]));
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*(optimized_plan->children_[0]));
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildPlan(),sort_plan.order_bys_, limit_plan.GetLimit());

  }
  return optimized_plan;
}

}  // namespace bustub
