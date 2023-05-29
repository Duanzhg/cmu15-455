//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.

    //Init();
}


void UpdateExecutor::Init() {
    //throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  auto table_oid = plan_->TableOid();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
    update_rows_ = 0;

    Tuple child_tuple{};
    RID child_rid;

    while(child_executor_->Next(&child_tuple, &child_rid)){
        update_rows_++;
        TupleMeta meta = table_info_->table_->GetTupleMeta(child_rid);
        meta.is_deleted_ = true;
        table_info_->table_->UpdateTupleMeta(meta, child_rid);

        //删除索引
        auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
        for(auto index_info : index_infos){
            std::vector<uint32_t> indexes;
            for(auto column : index_info->key_schema_.GetColumns()){
                indexes.push_back(table_info_->schema_.GetColIdx(column.GetName()));
            }

            auto  key_tuple = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, indexes);
            index_info->index_->DeleteEntry(key_tuple, child_rid, exec_ctx_->GetTransaction());
        }

        //插入更新后的tuple。
        std::vector<Value> values;
        values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
        for(auto expr : plan_->target_expressions_){
            values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
        }

        Tuple new_tuple(values, &child_executor_->GetOutputSchema());
        TupleMeta new_meta;
        meta.is_deleted_ = false;
        auto rid = table_info_->table_->InsertTuple(new_meta, new_tuple);

        //更新索引
        for(auto index_info : index_infos){
            std::vector<uint32_t> indexes;
            for(auto column : index_info->key_schema_.GetColumns()){
                indexes.push_back(table_info_->schema_.GetColIdx(column.GetName()));
            }

            auto  key_tuple = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, indexes);
            index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
        }
    }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
   /*
    Tuple child_tuple{};
    std::cout << "========" << std::endl;
    const auto status = child_executor_->Next(&child_tuple, rid);

    if(!status){
        return false;
    }

    update_rows_++;
    TupleMeta meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);

    //删除索引
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for( auto & index_info : index_infos){
        index_info->index_->DeleteEntry(child_tuple, *rid, exec_ctx_->GetTransaction());
    }

    //插入更新后的tuple
    std::vector<Value> values;
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for(const auto &expr : plan_->target_expressions_){
        values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    Tuple new_tuple(values, &child_executor_->GetOutputSchema());
    TupleMeta new_meta;
    new_meta.is_deleted_ = false;
    auto new_rid = table_info_->table_->InsertTuple(new_meta, new_tuple);

    //更新索引
    //index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(const auto &index_info : index_infos){
        index_info->index_->InsertEntry(new_tuple, *new_rid, exec_ctx_->GetTransaction());
    }

    //设置返回值
    std::vector<Value> value;
    value.push_back(Value(TypeId::INTEGER, update_rows_));
   // std::cout << value.size() << " " << GetOutputSchema().GetColumnCount() << std::endl;
    *tuple = Tuple(value, &GetOutputSchema());
    return true;
    */

    if(update_rows_ == -1){
        return false;
    }

    std::vector<Value> values;
    values.push_back(Value(TypeId::INTEGER, update_rows_));
    *tuple = Tuple(values, &GetOutputSchema());
    update_rows_ = -1;
    return true;
}
}  // namespace bustub
