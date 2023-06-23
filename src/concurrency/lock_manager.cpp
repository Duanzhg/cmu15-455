//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"


namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
    CheckTableLock(txn, lock_mode);
    //获取oid对应的lock request_queue
    std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
    if(table_lock_map_.find(oid) == table_lock_map_.end()){
        table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    auto queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> queue_lock(queue->latch_);
    table_map_lock.unlock();

    //检查是否是锁升级
    LockRequest *delete_request = nullptr;
    bool is_upgrade = IsLockUpgrade(txn, queue, lock_mode, &delete_request);

    if(is_upgrade && delete_request == nullptr){
        return true;
    }

    auto new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    queue->request_queue_.push_back(new_request);
    if(is_upgrade){
       queue->request_queue_.remove(delete_request);
       queue->cv_.notify_all();
    }else{
        //....
    }

    //阻塞
    while(!GrandTableLock(txn, queue, new_request)){
        if(txn->GetState() == TransactionState::ABORTED){
            queue->request_queue_.remove(new_request);
            delete delete_request;
            txn_manager_->Abort(txn);
            return false;
        }
        queue->cv_.wait(queue_lock);
    }
    queue_lock.unlock();
    //成功获取锁，更新事务
    //如果是锁升级，移除之前的锁的相关信息
    txn->LockTxn();
    if(is_upgrade){
        if(delete_request->lock_mode_ == LockMode::SHARED){
            txn->GetSharedTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::INTENTION_SHARED){
            txn->GetIntentionSharedTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::EXCLUSIVE){
            txn->GetExclusiveTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
            txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
            txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        }
        delete delete_request;
    }

    //插入新的锁信息
    if(lock_mode == LockMode::SHARED){
        txn->GetSharedTableLockSet()->insert(oid);
    }else if(lock_mode == LockMode::INTENTION_SHARED){
        txn->GetIntentionSharedTableLockSet()->insert(oid);
    }else if(lock_mode == LockMode::EXCLUSIVE){
        txn->GetExclusiveTableLockSet()->insert(oid);
    }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    }else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    }
    txn->UnlockTxn();
    return true;
}

void LockManager::CheckTableLock(Transaction *txn, LockMode lock_mode) {
    if(txn->GetState() == TransactionState::SHRINKING){
        if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        } else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
            if(lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE){
                txn->SetState(TransactionState(TransactionState::ABORTED));
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }
    }

    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
        if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
    }
}

void LockManager::CheckRowLock(Transaction *txn, LockMode lock_mode) {
    if(lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    }

    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
        if(lock_mode == LockMode::SHARED){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
    }

    if(txn->GetState() == TransactionState::SHRINKING){
        if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }
    }
}
auto LockManager::IsLockUpgrade(Transaction *txn, std::shared_ptr<LockRequestQueue> queue,
                                LockManager::LockMode lock_mode, LockRequest **delete_request) -> bool {
    for(auto q : queue->request_queue_){
        if(q->txn_id_ == txn->GetTransactionId()){
            if(queue->upgrading_ != INVALID_TXN_ID){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
            }else{ //检查是否兼容
                if(q->lock_mode_ == lock_mode){
                    return true;
                }

                if(lock_mode == LockMode::SHARED){
                    if(q->lock_mode_ == LockMode::EXCLUSIVE || q->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
                        return true;
                    }
                }else if(lock_mode == LockMode::INTENTION_SHARED){
                    return true;
                }else if(lock_mode == LockMode::EXCLUSIVE){

                }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
                    if(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
                        return true;
                    }
                }else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
                    if(lock_mode == LockMode::EXCLUSIVE){
                        return true;
                    }
                }
                if(q->lock_mode_ == LockMode::INTENTION_SHARED){

                }else if(q->lock_mode_ == LockMode::SHARED){
                    if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE){
                        txn->SetState(TransactionState::ABORTED);
                        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
                    }
                }else if(q->lock_mode_ == LockMode::EXCLUSIVE){
                    txn->SetState(TransactionState::ABORTED);
                    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
                }else if(q->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
                    if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE){
                        txn->SetState(TransactionState::ABORTED);
                        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
                    }
                }else if(q->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
                    txn->SetState(TransactionState::ABORTED);
                    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
                }

                //可以升级锁
                *delete_request = q;
                queue->upgrading_ = txn->GetTransactionId();
                return true;

            }
        }
    }

    return false;
}
void LockManager::MayBeUpdateToShrinking(Transaction *txn, LockMode lock_mode) {
    if(txn->GetState() == TransactionState::GROWING){
        if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
            if(lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::SHRINKING);
            }
        }else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::SHRINKING);
            }
        }else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::SHRINKING);
            }
        }
    }
}

auto LockManager::GrandTableLock(Transaction* txn, std::shared_ptr<LockRequestQueue> queue, LockRequest *request) -> bool {
    //auto txn = txn_manager_->GetTransaction(request->txn_id_);
    if(txn->GetState() == TransactionState::ABORTED){
        return false;
    }
    bool is_compatible = false;
    for(auto req : queue->request_queue_){
        if(req->granted_ == true){
            is_compatible = true;
            if(req->lock_mode_ == LockMode::SHARED){
                if(request->lock_mode_ != LockMode::INTENTION_SHARED && request->lock_mode_ !=LockMode::SHARED){
                    return false;
                }
            }else if(req->lock_mode_ == LockMode::INTENTION_SHARED){
                if(request->lock_mode_ == LockMode::EXCLUSIVE){
                    return false;
                }
            }else if(req->lock_mode_ == LockMode::EXCLUSIVE){
                return false;
            }else if(req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
                if(request->lock_mode_ != LockMode::INTENTION_SHARED && request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE){
                    return false;
                }
            }else if(req->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
                if(request->lock_mode_ != LockMode::INTENTION_SHARED){
                    return false;
                }
            }
        }
    }

    if(is_compatible){
        if(request->txn_id_== queue->upgrading_){
            queue->upgrading_ = INVALID_TXN_ID;
        }
        request->granted_ = true;
        return true;
    }

    //寻找优先级最高的请求
    LockRequest* highest_priority = nullptr;
    if(queue->upgrading_ != INVALID_TXN_ID){
        for(auto req : queue->request_queue_){
            if(queue->upgrading_ == req->txn_id_){
                highest_priority = req;
                break;
            }
        }

        BUSTUB_ASSERT(highest_priority != nullptr, "highest_priority is nullptr");
        if(highest_priority->txn_id_ == txn->GetTransactionId()){
            queue->upgrading_ = INVALID_TXN_ID;
        }
    }else{
        highest_priority = queue->request_queue_.front();
        BUSTUB_ASSERT(!queue->request_queue_.empty(), " queue is empty");
        BUSTUB_ASSERT(highest_priority != nullptr, "highest_priority is nullptr");
        BUSTUB_ASSERT(highest_priority->granted_ == false, "highest_priority granted is true");

    }
    if(highest_priority->txn_id_ == request->txn_id_){
        highest_priority->granted_ = true;
        return true;
    }

    //检查request 和 highest_priority的兼容性
    if(highest_priority->lock_mode_ == LockMode::SHARED){
        if(request->lock_mode_ != LockMode::SHARED && request->lock_mode_ != LockMode::INTENTION_SHARED){
            return false;
        }
    } else if(highest_priority->lock_mode_ == LockMode::INTENTION_SHARED){
        if(request->lock_mode_ == LockMode::EXCLUSIVE){
            return false;
        }
    }else if(highest_priority->lock_mode_ == LockMode::EXCLUSIVE){
        return false;
    }else if(highest_priority->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
        if(request->lock_mode_ != LockMode::INTENTION_SHARED && request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE){
            return false;
        }
    }else if(highest_priority->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
        if(request->lock_mode_ != LockMode::INTENTION_SHARED){
            return false;
        }
    }

    request->granted_ = true;
    return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {

    //获取oid对应的请求队列
    std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
    if(table_lock_map_.find(oid) == table_lock_map_.end()){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    auto queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> queue_lock(queue->latch_);
    table_map_lock.unlock();

    std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
    for(auto p : row_lock_map_){
        for(auto q : p.second->request_queue_){
            if(q->oid_ == oid && q->txn_id_ == txn->GetTransactionId()){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
                //return true;
            }
        }
    }
    //找到oid对应的请求
    LockRequest *request = nullptr;
    for(auto req : queue->request_queue_){
        if(req->txn_id_ == txn->GetTransactionId()){
            request = req;
            break;
        }
    }

    if(request == nullptr){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    txn->LockTxn();

    MayBeUpdateToShrinking(txn, request->lock_mode_);

    //释放锁
    if(request->lock_mode_ == LockMode::SHARED){
        txn->GetSharedTableLockSet()->erase(oid);
    }else if(request->lock_mode_ == LockMode::INTENTION_SHARED){
        txn->GetIntentionSharedTableLockSet()->erase(oid);
    }else if(request->lock_mode_ == LockMode::EXCLUSIVE){
        txn->GetExclusiveTableLockSet()->erase(oid);
    }else if(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    }else if(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    }
    txn->UnlockTxn();

    queue->request_queue_.remove(request);
    delete request;
    if(queue->request_queue_.empty()){
        table_lock_map_.erase(oid);
    }
    queue->cv_.notify_all();
    return true;
}

auto LockManager::IsCompatible(Transaction *txn, LockMode lock_mode, table_oid_t oid) -> bool {
    std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
    if(table_lock_map_.find(oid) == table_lock_map_.end()){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }

    auto table_request_queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> table_request_queue_lock(table_request_queue->latch_);
    table_map_lock.unlock();


    LockRequest *table_request = nullptr;
    for(auto req : table_request_queue->request_queue_){
        if(req->txn_id_ == txn->GetTransactionId()){
            if(req->granted_ == true){
                table_request = req;
            }
            break;
        }
    }

    if(table_request == nullptr){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }else{
        //todo 表锁和行锁之间的关系？
        if(lock_mode == LockMode::EXCLUSIVE){
            if(table_request->lock_mode_ == LockMode::SHARED){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
            }
        }else if(lock_mode == LockMode::SHARED){
            if(table_request->lock_mode_ == LockMode::EXCLUSIVE ){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
            }
        }
    }

    return true;
}
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
    //检查锁是否合理
    CheckRowLock(txn, lock_mode);
    //检查是否包含兼容的表锁
    IsCompatible(txn, lock_mode, oid);

    //添加行锁请求
    std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
    if(row_lock_map_.find(rid) == row_lock_map_.end()){
        row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }
    auto row_request_queue = row_lock_map_[rid];
    std::unique_lock<std::mutex> row_queue_lock(row_request_queue->latch_);
    row_map_lock.unlock();

    //检查是否为锁升级
    LockRequest *delete_request = nullptr;
    bool is_upgrade = IsLockUpgrade(txn, row_request_queue, lock_mode, &delete_request);

    if(is_upgrade == true && delete_request == nullptr){
        return true;
    }
    auto new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
    row_request_queue->request_queue_.push_back(new_request);
    if(is_upgrade){
        row_request_queue->request_queue_.remove(delete_request);
        row_request_queue->cv_.notify_all();
    }

    while(!GrandTableLock(txn, row_request_queue, new_request)){
        if(txn->GetState() == TransactionState::ABORTED){
            row_request_queue->request_queue_.remove(new_request);
            delete new_request;
            txn_manager_->Abort(txn);
            return false;
        }
        row_request_queue->cv_.wait(row_queue_lock);
    }

    //修改事务
    txn->LockTxn();

    //如果是锁升级，则先删除相关信息
    if(is_upgrade){
        if(delete_request->lock_mode_ == LockMode::SHARED){
            txn->GetSharedLockSet()->erase(rid);
            (*txn->GetSharedRowLockSet())[oid].erase(rid);
        }else if(delete_request->lock_mode_ == LockMode::EXCLUSIVE){
            txn->GetExclusiveLockSet()->erase(rid);
            (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
        }
        delete delete_request;
    }

    if(lock_mode == LockMode::SHARED){
        txn->GetSharedLockSet()->insert(rid);
        (*txn->GetSharedRowLockSet())[oid].insert(rid);
    }else if(lock_mode == LockMode::EXCLUSIVE){
        txn->GetExclusiveLockSet()->insert(rid);
        (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
    }
    txn->UnlockTxn();

    return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
    //找到对应的锁请求
    std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
    if(row_lock_map_.find(rid) == row_lock_map_.end()){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    auto row_request_queue = row_lock_map_[rid];
    std::unique_lock<std::mutex> row_queue_lock(row_request_queue->latch_);
    row_map_lock.unlock();

    LockRequest *request = nullptr;
    for(auto req : row_request_queue->request_queue_){
        if(req->txn_id_ == txn->GetTransactionId()){
            if(req->granted_ == true){
                request = req;
            }
            break;
        }
    }

    if(request == nullptr){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    row_request_queue->request_queue_.remove(request);
    row_request_queue->cv_.notify_all();

    txn->LockTxn();
    MayBeUpdateToShrinking(txn, request->lock_mode_);
    //释放事务中的锁
    if(request->lock_mode_ == LockMode::SHARED){
        txn->GetSharedLockSet()->erase(rid);
        (*txn->GetSharedRowLockSet())[oid].erase(rid);
        if((*txn->GetSharedRowLockSet())[oid].empty()){
            txn->GetSharedRowLockSet()->erase((oid));
        }
    }else if(request->lock_mode_ == LockMode::EXCLUSIVE){
        txn->GetExclusiveLockSet()->erase(rid);
        (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
        if((*txn->GetExclusiveRowLockSet())[oid].empty()){
            txn->GetExclusiveRowLockSet()->erase((oid));
        }
    }

    txn->UnlockTxn();
    delete request;
    //row_request_queue->cv_.notify_all();
    return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
     if(waits_for_.find(t1) == waits_for_.end()){
         waits_for_[t1] = std::vector<txn_id_t>();
     }

     //按顺序插入
     auto &txn_ids = waits_for_[t1];
     txn_ids.push_back(t2);
     std::sort(txn_ids.begin(), txn_ids.end());
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    if(waits_for_.find(t1) == waits_for_.end()){
        return;
    }

    auto &txn_ids = waits_for_[t1];
    if(txn_ids.empty()){
        waits_for_.erase(t1);
        return;
    }

    for(auto iter = txn_ids.begin(); iter != txn_ids.end(); iter++){
        if(t2 == *iter){
            txn_ids.erase(iter);
            break;
        }
    }

    if(txn_ids.empty()){
        waits_for_.erase(t1);
    }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
    std::vector<txn_id_t> keys;
    for(auto p : waits_for_){
        keys.push_back(p.first);
    }

    std::sort(keys.begin(), keys.end());


    for(auto iter = keys.begin(); iter != keys.end(); iter++){
        for(auto & v : visited_){
            v.second = false;
        }
        *txn_id = -1;
        if(hasCycle(*iter, txn_id)){
            return true;
        }

    }
    return false;
}

auto LockManager::hasCycle(txn_id_t first_txd_id, bustub::txn_id_t *txn_id) -> bool {
    auto txn_ids = waits_for_[first_txd_id];
    for(auto id : txn_ids){
        if(id > *txn_id){
            *txn_id = id;
        }

        if(visited_[id] == true){
            return true;
        }

        visited_[id] = true;
        hasCycle(id, txn_id);
    }

    return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for(auto iter = waits_for_.begin(); iter != waits_for_.end(); iter++){
      auto txn_ids = iter->second;
      for(auto id_iter = txn_ids.begin(); id_iter != txn_ids.end(); id_iter++){
          edges.push_back(std::pair<txn_id_t, txn_id_t>{ iter->first, *id_iter});
      }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
        //构建图
        txn_manager_->BlockAllTransactions();
       // std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
        //std::unique_lock<std::mutex> row_map_lock(table_lock_map_latch_);
        for(auto iter = table_lock_map_.begin(); iter != table_lock_map_.end(); iter++){
            auto queue = iter->second;
            for(auto req : queue->request_queue_){
                if(req->granted_ == false){
                    for(auto r : queue->request_queue_){
                        if(r->granted_  == true){
                            AddEdge(req->txn_id_, r->txn_id_);
                        }
                    }
                }
            }
        }


        for(auto iter = row_lock_map_.begin(); iter != row_lock_map_.end(); iter++){
            auto queue = iter->second;
            for(auto req : queue->request_queue_){
                if(req->granted_ == false){
                    for(auto r : queue->request_queue_){
                        if(r->granted_ == true){
                            AddEdge(req->txn_id_, r->txn_id_);
                        }
                    }
                }
            }
        }
        txn_manager_->ResumeTransactions();
        for(auto &i : waits_for_){
            visited_[i.first] = false;
        }
        //寻找环
        txn_id_t txn_id= -1;

        while(HasCycle(&txn_id)){

            auto txn = txn_manager_->GetTransaction(txn_id);
            if(txn == nullptr){
                //continue;
                //throw Exception("aaaaaaaaaaaaaaaa");
            }
            //txn->LockTxn();
            txn->SetState(TransactionState::ABORTED);
            //std::unique_lock<std::shared_mutex> l(txn_manager_->txn_map_mutex);
            //txn_manager_->txn_map.erase(txn_id);

            for(auto q : table_lock_map_){
                q.second->cv_.notify_all();
            }

            for(auto q : table_lock_map_){
                q.second->cv_.notify_all();
            }
            //释放资源
            /*
            std::unordered_map<table_oid_t, std::unordered_set<RID>> row_lock_set;
            for(auto &s_row_lock_set : *txn->GetSharedRowLockSet()){
                for(auto &rid : s_row_lock_set.second){
                    row_lock_set[s_row_lock_set.first].emplace(rid);
                }
            }

            for(auto &x_row_lock_set : *txn->GetExclusiveRowLockSet()){
                for(auto &rid : x_row_lock_set.second){
                    row_lock_set[x_row_lock_set.first].emplace(rid);
                }
            }

            std::unordered_set<table_oid_t> table_lock_set;
            for(auto oid : *txn->GetSharedTableLockSet()){
                table_lock_set.emplace(oid);
            }

            for(auto oid : *txn->GetIntentionSharedTableLockSet()){
                table_lock_set.emplace(oid);
            }

            for(auto oid : *txn->GetExclusiveTableLockSet()){
                table_lock_set.emplace(oid);
            }

            for(auto oid : *txn->GetIntentionExclusiveTableLockSet()){
                table_lock_set.emplace(oid);
            }

            for(auto oid : *txn->GetSharedIntentionExclusiveTableLockSet()){
                table_lock_set.emplace(oid);
            }

            txn->UnlockTxn();
            for(auto &locked_table_row_set : row_lock_set){
                for(auto &rid : locked_table_row_set.second){
                    UnlockRow(txn, locked_table_row_set.first, rid);
                }
            }

            for(auto oid : table_lock_set){
                UnlockTable(txn, oid);
            }
            */


            //清除wait-for中的相关信息
            waits_for_.erase(txn_id);
            for(auto iter = waits_for_.begin(); iter != waits_for_.end(); iter++){
                for(auto i = iter->second.begin(); i != iter->second.end(); i++){
                    if(*i == txn_id){
                        break;
                    }
                }
            }

        }
    }
  }
}

}  // namespace bustub
