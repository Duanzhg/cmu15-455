#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){
}

void TopNExecutor::Init() {
    //throw NotImplementedException("===TopNExecutor is not implemented");
    child_executor_->Init();


    Tuple child_tuple;
    RID child_rid;
    //std::cout << plan_->GetChildPlan()->ToString() << std::endl;

    //child_executor_->Next(&child_tuple, &child_rid);
    //std::cout << "===========" << std::endl;
    while(child_executor_->Next(&child_tuple, &child_rid)){

      tuples_.push_back(child_tuple);
    }


    std::map<size_t, bool> visited;
    size_t n = plan_->GetN() < tuples_.size() ? plan_->GetN() : tuples_.size();

    for(size_t i=0; i<n; i++){

        Tuple temp_tuple = tuples_[0] ;
        size_t temp_index = 0;
        for(size_t j = 1; j< tuples_.size(); j++){

            if(visited.count(j) ){
              if(temp_index == j){
                temp_tuple = tuples_[j+1];
                temp_index = j+1;
                j++;
              }
              continue ;
            }
            for(auto order_by : plan_->order_bys_){
                if(order_by.first == OrderByType::INVALID){
                    throw Exception("OrderByType invalid");
                }

                auto temp = order_by.second->Evaluate(&temp_tuple, plan_->OutputSchema());
                auto curr = order_by.second->Evaluate(&tuples_[j], plan_->OutputSchema());


                auto cmp_result = temp.CompareEquals(curr);
                if(cmp_result == CmpBool::CmpTrue){
                    continue ;
                }

                cmp_result = temp.CompareGreaterThan(curr);
                if(order_by.first == OrderByType::DESC){
                    if(cmp_result == CmpBool::CmpFalse){
                        temp_tuple = tuples_[j];
                        temp_index = j;
                    }
                    break ;
                } else{
                    if(cmp_result == CmpBool::CmpTrue){
                        temp_tuple = tuples_[j];
                        temp_index = j;
                    }
                    break ;
                }
            }
        }

        visited[temp_index] = true;
        top_n.push_back(temp_tuple);
    }

    iter_ = top_n.begin();

}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(iter_ == top_n.end()){
        return false;
    }

    *tuple = *iter_;
    ++iter_;
    return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    //throw NotImplementedException("TopNExecutor is not implemented");
    return plan_->GetN();
};

}  // namespace bustub
