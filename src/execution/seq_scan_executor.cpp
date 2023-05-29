//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
   plan_ = plan;
   iter_ = nullptr;

}

void SeqScanExecutor::Init() {
    //throw NotImplementedException("SeqScanExecutor is not implemented");
    auto table_oid = plan_->GetTableOid();
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable(table_oid);

    iter_ = new TableIterator(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while(true){
        if(iter_->IsEnd()){
          delete iter_;
          return false;
        }

        auto [meta, temp_tuple] = iter_->GetTuple();
        *tuple = temp_tuple;
        *rid = iter_->GetRID();
        ++(*iter_);

        //处理tuple被删除的情况
        if(!meta.is_deleted_){
            return true;
        }
    }
}
}  // namespace bustub
