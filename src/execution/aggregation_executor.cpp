//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),plan_(plan),child_(std::move(child)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {}

void AggregationExecutor::Init() {

    child_->Init();

    aht_.SetFlag(0);
    Tuple child_tuple;
    RID child_rid;

    int flag = 0;
    //auto group_bys = plan_->GetGroupBys();
    while(child_->Next(&child_tuple, &child_rid)){
      flag = 1;
      aht_.SetFlag(1);
      AggregateKey agg_key = MakeAggregateKey(&child_tuple);
      AggregateValue agg_value;
        for(auto &agg_expr : plan_->GetAggregates()){
            agg_value.aggregates_.push_back(agg_expr->Evaluate(&child_tuple, child_->GetOutputSchema()));
        }

        aht_.InsertCombine(agg_key, agg_value);
    }

    //当group by为空并且表为空时插入一条空记录。
    if( plan_->group_bys_.empty() && flag == 0){
        auto agg_value = aht_.GenerateInitialAggregateValue();
        std::vector<Value> values;
        for(size_t i=0; i<plan_->group_bys_.size(); i++){
            values.push_back(Value(TypeId::INTEGER, -1));
        }
        AggregateKey agg_key{values};
        aht_.InsertCombine(agg_key, agg_value);
    }
    aht_iterator = new SimpleAggregationHashTable::Iterator(aht_.Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //return false;

    if((*aht_iterator) == aht_.End()){
        delete aht_iterator;
        return false;
    }

    auto key = aht_iterator->Key();
    auto value = aht_iterator->Val();

    std::vector<Value> values;
    for(auto & k : key.group_bys_){
        values.push_back(k);
    }

    for(auto & v : value.aggregates_){
        values.push_back(v);
    }



    *tuple = Tuple(values, &GetOutputSchema());
    ++(*aht_iterator);

    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
