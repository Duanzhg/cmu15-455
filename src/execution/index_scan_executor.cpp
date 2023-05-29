//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    //throw NotImplementedException("IndexScanExecutor is not implemented");
    index_oid_ = plan_->GetIndexOid();
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(index_oid_);
    auto index_tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
    iter_ = new BPlusTreeIndexIteratorForTwoIntegerColumn(index_tree->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if(iter_->IsEnd()){
        delete iter_;
        return false;
    }

    //auto [temp, temp_rid] = *(*iter_);
    //*rid = temp_rid;
    *rid = (*(*iter_)).second;
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(index_oid_);
    auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
    *tuple = table_info->table_->GetTuple(*rid).second;


    ++(*iter_);
    return true;
}

}  // namespace bustub
