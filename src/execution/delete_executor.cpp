//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){

}

void DeleteExecutor::Init() {
    //throw NotImplementedException("DeleteExecutor is not implemented");
    child_executor_->Init();
    auto table_oid = plan_->TableOid();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
    delete_rows_ = 0;

    Tuple child_tuple;
    RID child_rid;

    while(child_executor_->Next(&child_tuple, &child_rid)){
        delete_rows_++;
        auto meta = table_info_->table_->GetTupleMeta(child_rid);
        meta.is_deleted_ = true;
        table_info_->table_->UpdateTupleMeta(meta, child_rid);

        //删除索引
        auto index_infos =  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
        for(auto index_info : index_infos){
          std::vector<uint32_t> indexes;
          for(auto column : index_info->key_schema_.GetColumns()){
            indexes.push_back(table_info_->schema_.GetColIdx(column.GetName()));
          }

          auto  key_tuple = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, indexes);
          index_info->index_->DeleteEntry(key_tuple, child_rid, exec_ctx_->GetTransaction());
        }
    }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

    /*
    Tuple child_tuple{};

    auto status = child_executor_->Next(&child_tuple, rid);
    if(!status){
        return false;
    }

    delete_rows_++;
    //设置删除标记
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);

    //删除索引
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(auto index_info : index_infos){
        index_info->index_->DeleteEntry(child_tuple, *rid, exec_ctx_->GetTransaction());
    }


    //设置返回的tuple
    std::vector<Value> value;
    value.push_back(Value(TypeId::INTEGER, delete_rows_));
    *tuple = Tuple(value, &GetOutputSchema());
    return true;
     */

    if(delete_rows_ == -1){
        return false;
    }

    std::vector<Value> value;
    value.push_back(Value(TypeId::INTEGER, delete_rows_));
    *tuple = Tuple(value, &GetOutputSchema());
    delete_rows_ = -1;
    return true;
}

}  // namespace bustub
