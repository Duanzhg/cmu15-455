//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {


class SimpleJoinHashTable{
 public:
    SimpleJoinHashTable(){
    }

    void Insert(const JoinKey& join_key, const JoinValue& join_value){
        if(!Count(join_key)){
          ht_[join_key] = {};
        }
        ht_[join_key].push_back(join_value);
    }

    auto Count(const JoinKey& join_key)-> bool {
        std::cout << ht_.count(join_key) << std::endl;
        return ht_.count(join_key) != 0;
    }

    auto GetValue(const JoinKey& join_key) -> std::vector<JoinValue> {
        return ht_[join_key];
    }

    auto Print(const Schema* schema){
        for(auto iter = ht_.begin(); iter != ht_.end(); iter++){
           for(auto viter = iter->second.begin(); viter != iter->second.end(); viter++){
             std::cout << viter->value.ToString(schema) << std::endl;
           }
        }
    }

 private:
    std::unordered_map<JoinKey, std::vector<JoinValue>> ht_{};

};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto MakeLeftJoinKey(const Tuple& tuple) -> JoinKey{
      std::vector<Value> values;
      for(auto expr : plan_->LeftJoinKeyExpressions()){
          values.push_back(expr->Evaluate(&tuple, left_executor_->GetOutputSchema()));
      }

      /*
      std::cout << "left key: ";
      for(auto v : values){
          std::cout << v.ToString() << " ";
      }
      std::cout << std::endl;
    */
       return {values};
  }

  auto MakeRightJoinKey(const Tuple& tuple) -> JoinKey{
      std::vector<Value> values;
      for(auto expr : plan_->RightJoinKeyExpressions()){
          values.push_back(expr->Evaluate(&tuple, right_executor_->GetOutputSchema()));
      }


      /*
      std::cout << "right key: ";
      for(auto v : values){
          std::cout << v.ToString() << " ";
      }
      std::cout << std::endl;
    */
      return {values};
  }

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  SimpleJoinHashTable jht_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  std::list<Tuple> match_tuples_;
};

}  // namespace bustub
