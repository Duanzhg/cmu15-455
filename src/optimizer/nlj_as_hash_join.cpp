#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {


auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>


  std::vector<AbstractPlanNodeRef> children;
  for(const auto &child : plan->GetChildren()){
      children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if(optimized_plan->GetType() == PlanType::NestedLoopJoin){

      const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

      BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children");

      if(const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr){
          if(expr->comp_type_ == ComparisonType::Equal){
              if(const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get()); left_expr != nullptr){
                  if(const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get()); right_expr != nullptr){
                    auto left_expr_tuple_0 =
                        std::make_shared<ColumnValueExpression>(left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());

                      auto right_expr_tuple_0 =
                        std::make_shared<ColumnValueExpression>(right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());

                      std::vector<AbstractExpressionRef> v_left_expr;
                      std::vector<AbstractExpressionRef> v_right_expr;
                      if(left_expr_tuple_0->GetTupleIdx() == 0 && right_expr_tuple_0->GetTupleIdx() == 1){

                          v_left_expr.push_back(left_expr_tuple_0);
                          v_right_expr.push_back(right_expr_tuple_0);
                      }

                      if(left_expr_tuple_0->GetTupleIdx() == 1 && right_expr_tuple_0->GetTupleIdx() == 0){
                          v_left_expr.push_back(right_expr_tuple_0);
                          v_right_expr.push_back(left_expr_tuple_0);
                      }

                      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                                nlj_plan.GetRightPlan(), v_left_expr,
                                                                v_right_expr, nlj_plan.GetJoinType());
                  }

              }
          }
      }

      if(const auto* expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr ){
          if(expr->logic_type_ == LogicType::And){
              if(const auto* left_expr = dynamic_cast<const ComparisonExpression*>(expr->children_[0].get()); left_expr != nullptr && left_expr->comp_type_ == ComparisonType::Equal){
                  if(const auto * right_expr = dynamic_cast<const ComparisonExpression*>(expr->children_[1].get()); right_expr != nullptr && right_expr->comp_type_ == ComparisonType::Equal){
                      std::vector<AbstractExpressionRef> v_left_expr;
                      std::vector<AbstractExpressionRef> v_right_expr;

                      if(const auto *left_left_expr = dynamic_cast<const ColumnValueExpression*>(left_expr->children_[0].get()); left_left_expr != nullptr){
                          if(const auto *left_right_expr = dynamic_cast<const ColumnValueExpression*>(left_expr->children_[1].get()); left_right_expr != nullptr){
                            auto left_left_expr_tuple_0 =
                                std::make_shared<ColumnValueExpression>(left_left_expr->GetTupleIdx(), left_left_expr->GetColIdx(), left_left_expr->GetReturnType());

                              auto left_right_expr_tuple_0 =
                                std::make_shared<ColumnValueExpression>(left_right_expr->GetTupleIdx(), left_right_expr->GetColIdx(), left_right_expr->GetReturnType());

                              if(left_left_expr_tuple_0->GetTupleIdx()==0 && left_right_expr_tuple_0->GetTupleIdx()==1){

                                  v_left_expr.push_back(left_left_expr_tuple_0);
                                  v_right_expr.push_back(left_right_expr_tuple_0);
                              }

                              if(left_left_expr_tuple_0->GetTupleIdx()==1 && left_right_expr_tuple_0->GetTupleIdx()==0){

                                  v_left_expr.push_back(left_right_expr_tuple_0);
                                  v_right_expr.push_back(left_left_expr_tuple_0);
                              }
                          }
                      }

                      if(const auto *right_left_expr = dynamic_cast<const ColumnValueExpression*>(right_expr->children_[0].get()); right_left_expr != nullptr){
                          if(const auto *right_right_expr = dynamic_cast<const ColumnValueExpression*>(right_expr->children_[1].get()); right_right_expr != nullptr){

                              auto right_left_expr_tuple_0 =
                                  std::make_shared<ColumnValueExpression>(right_left_expr->GetTupleIdx(), right_left_expr->GetColIdx(), right_left_expr->GetReturnType());

                              auto right_right_expr_tuple_0 =
                                  std::make_shared<ColumnValueExpression>(right_right_expr->GetTupleIdx(), right_right_expr->GetColIdx(), right_right_expr->GetReturnType());

                              if(right_left_expr_tuple_0->GetTupleIdx() == 0 && right_right_expr_tuple_0->GetTupleIdx()==1){

                                  v_left_expr.push_back(right_left_expr_tuple_0);
                                  v_right_expr.push_back(right_right_expr_tuple_0);
                              }

                              if(right_left_expr_tuple_0->GetTupleIdx()==1 && right_right_expr_tuple_0->GetTupleIdx() == 0){

                                  v_left_expr.push_back(right_right_expr_tuple_0);
                                  v_right_expr.push_back(right_left_expr_tuple_0);
                              }

                          }
                      }

                      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                                nlj_plan.GetRightPlan(), v_left_expr, v_right_expr,
                                                                nlj_plan.GetJoinType());
                  }
              }
          }
      }

  }
  return optimized_plan;
}



}  // namespace bustub
