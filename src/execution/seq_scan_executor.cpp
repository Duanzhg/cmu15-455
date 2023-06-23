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
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
   plan_ = plan;
   iter_ = nullptr;

}

void SeqScanExecutor::Init() {
    //throw NotImplementedException("SeqScanExecutor is not implemented");
    auto table_oid = plan_->GetTableOid();
    auto table_info = GetExecutorContext()->GetCatalog()->GetTable(table_oid);

    //iter_ = new TableIterator(table_info->table_->MakeIterator());
    //iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
    if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){

    }else{
        try{
            bool is_success;
            if(!exec_ctx_->IsDelete()){
                is_success =  exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,table_oid);
            }else{
                is_success = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
            }
            if(!is_success){
                throw ExecutionException("fail lock in seq scan executor init");
            }
        }catch (TransactionAbortException &e){
            //执行回滚操作....
            exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
        };

    }
    iter_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while(true){
        if(iter_->IsEnd()){
          //delete iter_;
          return false;
        }
        *rid = iter_->GetRID();
        if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED){
            try{
                bool is_success;
                if(!exec_ctx_->IsDelete()){
                    is_success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), *rid);
                }else{
                    is_success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), *rid);
                }
                if(!is_success){
                    throw ExecutionException("fail lock in seq scan executor next");
                }
            }catch(TransactionAbortException &e){
                exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
            }
        }
        auto [meta, temp_tuple] = iter_->GetTuple();
        *tuple = temp_tuple;

        //根据隔离级别释放
        if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
            try{
                if(!exec_ctx_->IsDelete()){
                    exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(),plan_->GetTableOid(), *rid);
                }
            }catch(TransactionAbortException &e){
                exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
            }
        }
        ++(*iter_);

        //处理tuple被删除的情况
        if(!meta.is_deleted_){
            return true;
        }
    }
}
}  // namespace bustub
