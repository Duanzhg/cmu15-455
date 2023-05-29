//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)), right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
    //throw NotImplementedException("HashJoinExecutor is not implemented");
    left_executor_->Init();
    right_executor_->Init();


    Tuple right_child_tuple;
    RID right_child_rid;
    while(right_executor_->Next(&right_child_tuple, &right_child_rid)){

        //std::cout << right_child_tuple.ToString(&right_executor_->GetOutputSchema()) << std::endl;
        JoinKey join_key = MakeRightJoinKey(right_child_tuple);
        JoinValue join_value{right_child_tuple};

        jht_.Insert(join_key, join_value);
    }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    Tuple left_child_tuple;
    //jht_.Print(&right_executor_->GetOutputSchema());
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

        JoinKey left_join_key = MakeLeftJoinKey(left_child_tuple);
        //std::cout << left_child_tuple.ToString(&left_executor_->GetOutputSchema()) << std::endl;
        if(jht_.Count(left_join_key)){


            auto join_values = jht_.GetValue(left_join_key);

            for(auto v : join_values){
                std::vector<Value> values;
                for(size_t i=0; i<left_executor_->GetOutputSchema().GetColumnCount(); i++){
                  values.push_back(left_child_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
                }

                for(size_t i = 0; i<right_executor_->GetOutputSchema().GetColumnCount(); i++){
                  values.push_back(v.value.GetValue(&right_executor_->GetOutputSchema(), i));
                }

                Tuple match_tuple(values, &plan_->OutputSchema());
                match_tuples_.push_back(match_tuple);
            }


            //*tuple = Tuple(values, &plan_->OutputSchema());
            //return true;
        } else if(plan_->GetJoinType() == JoinType::LEFT){
            std::vector<Value> values;
            for(size_t i=0; i<left_executor_->GetOutputSchema().GetColumnCount(); i++){
                values.push_back(left_child_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
            }

            for(size_t i=0; i<right_executor_->GetOutputSchema().GetColumnCount(); i++){
                values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            }

            Tuple match_tuple(values, &plan_->OutputSchema());
            match_tuples_.push_back(match_tuple);
            //*tuple = Tuple(values, &plan_->OutputSchema());
            //return true;
        }

        if(!match_tuples_.empty()){
            *tuple = match_tuples_.front();
            match_tuples_.pop_front();
            return true;
        }
    }


}

}  // namespace bustub
