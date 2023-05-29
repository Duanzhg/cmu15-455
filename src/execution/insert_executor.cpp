//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {

}

void InsertExecutor::Init() {
    //throw NotImplementedException("InsertExecutor is not implemented");
    child_executor_->Init();
    insert_rows_ = 0;

    Tuple child_tuple;
    RID  child_rid;
    auto table_oid = plan_->TableOid();
    auto table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

    while(child_executor_->Next(&child_tuple, &child_rid)){
        insert_rows_++;


        TupleMeta meta;
        meta.is_deleted_ = false;
        auto rid = table_info->table_->InsertTuple(meta, child_tuple);


        for(auto index_info : index_infos){
            //std::cout << index_infos.size() << std::endl;
            //std::cout << child_tuple.ToString(&index_info->key_schema_) << std::endl;

            std::vector<uint32_t> indexes;
            for(auto column : index_info->key_schema_.GetColumns()){
                std::cout << column.GetName() << std::endl;
                indexes.push_back(table_info->schema_.GetColIdx(column.GetName()));
            }

            auto key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),index_info->key_schema_, indexes);
            std::cout << key_tuple.ToString(&index_info->key_schema_) << std::endl;
            index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
        }
    }

}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
   /*
    Tuple child_tuple{};

    const auto status = child_executor_->Next(&child_tuple, rid);

    if(!status){
        return false;
    }

    insert_rows_++;
    auto table_oid = plan_->TableOid();
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable(table_oid);

    TupleMeta meta;
    meta.is_deleted_ = false;
    *rid = *(table_info->table_->InsertTuple(meta,child_tuple));

    //更新Index
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

    for(auto index_info : index_infos){
        index_info->index_->InsertEntry(child_tuple, *rid, exec_ctx_->GetTransaction());
    }

    //tuple = nullptr;
    std::vector<Value> value;
    value.push_back(Value(TypeId::INTEGER, insert_rows_));
    *tuple = Tuple(value, &GetOutputSchema());
    return true;
    */

    if(insert_rows_ == -1){
        return false;
    }

    std::vector<Value> value;
    value.push_back(Value(TypeId::INTEGER, insert_rows_));
    *tuple = Tuple(value, &GetOutputSchema());
    insert_rows_ = -1;
    return true;

}
}  // namespace bustub
