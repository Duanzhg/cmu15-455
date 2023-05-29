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
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
    //throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
    left_executor_->Init();
    right_executor_->Init();

    Tuple right_child_tuple;
    RID right_child_rid;
    while(right_executor_->Next(&right_child_tuple, &right_child_rid)){
        right_tuples_.push_back(right_child_tuple);
    }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    Tuple left_child_tuple;
    auto join_expr = plan_->Predicate();
    while(true){
        if(!match_tuples_.empty()){
            *tuple = match_tuples_.front();
            match_tuples_.pop_front();
            return true;
        }


        auto status = left_executor_->Next(&left_child_tuple, rid);

        if(!status){
          return false;
        }

        int flag = 0;
        for(auto t : right_tuples_){

          auto value = join_expr->EvaluateJoin(&left_child_tuple, left_executor_->GetOutputSchema(), &t, right_executor_->GetOutputSchema());
          if(!value.IsNull()  && value.GetAs<bool>()){
            flag = 1;
            std::vector<Value> values;
            for(size_t i=0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++){
                values.push_back(left_child_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
            }

            for(size_t i=0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++){
                values.push_back(t.GetValue(&right_executor_->GetOutputSchema(), i));
            }

            Tuple match_tuple(values, &GetOutputSchema());
            match_tuples_.push_back(match_tuple);
          }
        }

        if(plan_->GetJoinType() == JoinType::LEFT && flag == 0){
            std::vector<Value> values;
            for(size_t i=0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++){
                values.push_back(left_child_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
            }

            for(size_t i=0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++){
                values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            }

            Tuple match_tuple(values, &GetOutputSchema());
            match_tuples_.push_back(match_tuple);
        }

        if(!match_tuples_.empty()){
          *tuple = match_tuples_.front();
          match_tuples_.pop_front();
          return true;
        }
    }
}

}  // namespace bustub
