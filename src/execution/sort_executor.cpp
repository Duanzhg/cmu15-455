#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
    //throw NotImplementedException("SortExecutor is not implemented");
    child_executor_->Init();

    Tuple child_tuple;
    RID child_rid;
    while(child_executor_->Next(&child_tuple, &child_rid)){
        tuples_.push_back(child_tuple);
    }

    std::sort(tuples_.begin(), tuples_.end(), [this](Tuple left, Tuple right)->bool{
        auto order_bys = plan_->order_bys_;
        for(auto order_by : order_bys){
            if(order_by.first == OrderByType::INVALID){
                throw Exception("OrderByType invalid");
            }

            auto left_value = order_by.second->Evaluate(&left, child_executor_->GetOutputSchema());
            auto right_value = order_by.second->Evaluate(&right, child_executor_->GetOutputSchema());

            auto cmp_result = left_value.CompareEquals(right_value);
            if(cmp_result == CmpBool::CmpTrue){
                continue;
            }
            cmp_result = left_value.CompareGreaterThan(right_value);
            if(order_by.first == OrderByType::DESC){
                return cmp_result == CmpBool::CmpTrue;
            } else{
                return cmp_result == CmpBool::CmpFalse;
            }
        }

        return true;
    });

    iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //return false;

    if(iter_ == tuples_.end()){
        return false;
    }

    *tuple = *iter_;
    ++iter_;
    return true;
}

}  // namespace bustub
