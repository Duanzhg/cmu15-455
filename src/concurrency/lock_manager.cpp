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

    if(txn->GetState() == TransactionState::SHRINKING){
        if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }

        if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }

        if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
    }

    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
        if(lock_mode == LockMode::SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
    }


    std::unique_lock<std::mutex> lock(table_lock_map_latch_);
    if(table_lock_map_.find(oid) == table_lock_map_.end()){
        table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    auto queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> queue_lock(queue->latch_);
    lock.unlock();

    //queue->latch_.lock();

    LockRequest *new_request = nullptr;
    LockRequest *delete_request = nullptr;
    int flag = 0;
    for(auto r : queue->request_queue_){
        if(r->txn_id_ == txn->GetTransactionId()) {
            flag = 1;
            if (queue->upgrading_ != INVALID_TXN_ID) {
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(r->txn_id_, AbortReason::UPGRADE_CONFLICT);
            }

            if (r->lock_mode_ == lock_mode) {
                return true;
            }

            if(r->lock_mode_ == LockMode::INTENTION_SHARED){
                //
            }else if(r->lock_mode_ == LockMode::SHARED || r->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
                if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE){
                    txn->SetState(TransactionState::ABORTED);
                    throw TransactionAbortException(r->txn_id_, AbortReason::UPGRADE_CONFLICT);
                }
            }else if(r->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
                if(r->lock_mode_ != LockMode::EXCLUSIVE){
                    txn->SetState(TransactionState::ABORTED);
                    throw TransactionAbortException(r->txn_id_, AbortReason::UPGRADE_CONFLICT);
                }
            }

            //插入升级锁的请求
            //new_request = new LockRequest(r->txn_id_, lock_mode, oid);
            delete_request = r;
            //break ;
            /*
            queue->request_queue_.remove(r);

            //todo notify?

            queue->request_queue_.push_back(new_request);
            queue->upgrading_ = r->txn_id_;
            queue->cv_.notify_all();

            delete r;
             */

             //break ;
        }
    }


    new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    if(flag == 0){
        queue->request_queue_.push_back(new_request);
    } else{
        queue->request_queue_.push_back(new_request);
        queue->request_queue_.remove(delete_request);

        queue->upgrading_ = new_request->txn_id_;
        queue->cv_.notify_all();
    }


    //尝试获取锁。
    while(!GrandLock(queue, new_request)){

        if(txn->GetState() == TransactionState::ABORTED){
            queue->request_queue_.remove(new_request);
            delete new_request;
        }
        queue->cv_.wait(queue_lock);
    }

    txn->LockTxn();
    if(flag == 1){
        //若是锁升级，先移除之前的锁
        if(delete_request->lock_mode_ == LockMode::INTENTION_SHARED){
            txn->GetIntentionSharedTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::SHARED){
            txn->GetSharedTableLockSet()->erase(oid);
        } else if(delete_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
            txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::EXCLUSIVE){
            txn->GetExclusiveTableLockSet()->erase(oid);
        }else if(delete_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
            txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        }
        delete delete_request;
    }

    //更新事务
    if(lock_mode == LockMode::INTENTION_SHARED){
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    } else if(lock_mode == LockMode::SHARED){
        txn->GetSharedTableLockSet()->insert(oid);
    }else if(lock_mode == LockMode::EXCLUSIVE){
        txn->GetExclusiveTableLockSet()->insert(oid);
    } else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    }
    txn->UnlockTxn();
    return true;
}


auto LockManager::GrandLock(std::shared_ptr<LockRequestQueue> queue, LockRequest *lock_request) -> bool {
    //std::cout << "########" << std::endl;

    auto txn = txn_manager_->GetTransaction(lock_request->txn_id_);
    if(txn->GetState() == TransactionState::ABORTED){
        return false;
    }
    int flag = 0;
    //检查兼容性
    for(auto req : queue->request_queue_){
        if(req->granted_ == true){
            flag = 1;
            if(req->lock_mode_ == LockMode::INTENTION_SHARED){
                //return true;
            }else if(req->lock_mode_ == LockMode::SHARED){
                if(req->lock_mode_ != LockMode::SHARED){
                    return false;
                }
            }else if(req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
                if(req->lock_mode_ != LockMode::INTENTION_EXCLUSIVE){
                    return false;
                }
            }else if(req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || req->lock_mode_ == LockMode::EXCLUSIVE){
                return false;
            }
        }
    }

    //兼容性检查通过
    BUSTUB_ASSERT(lock_request != nullptr, "lock_request is nullptr");
    if(flag ==  1){
        lock_request->granted_ = true;
        return true;
    }

    //找优先级最高的request
    LockRequest * high_priority = nullptr;
    if(queue->upgrading_ != INVALID_TXN_ID){
        for(auto req : queue->request_queue_){
            if(queue->upgrading_ == req->txn_id_){
                high_priority = req;
                break ;
            }
        }

        BUSTUB_ASSERT(high_priority != nullptr, "high_priority is nullptr");
        if(high_priority->txn_id_ == lock_request->txn_id_) {
            high_priority->granted_ = true;
            return true;
        }
    } else{
        //如果没有锁升级的请求
        high_priority = queue->request_queue_.front();
        BUSTUB_ASSERT(!queue->request_queue_.empty(), "queue is empty");
        BUSTUB_ASSERT(high_priority != nullptr, "high_priority is nullptr");
        BUSTUB_ASSERT(high_priority->granted_ == false, "high_priority granted is true");
    }

    if(lock_request->txn_id_ == high_priority->txn_id_){
        lock_request->granted_ = true;
        return true;
    }
    if(high_priority->lock_mode_ == LockMode::INTENTION_SHARED){

    }else if(high_priority->lock_mode_ == LockMode::SHARED){
        if(lock_request->lock_mode_ != LockMode::SHARED){
            return false;
        }
    } else if(high_priority->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
        if(lock_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE){
            return false;
        }
    }else if(lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_request->lock_mode_ == LockMode::EXCLUSIVE){
        return false;
    }

    lock_request->granted_ = true;
    return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
    if(!txn->GetSharedLockSet()->empty() || !txn->GetExclusiveLockSet()->empty()){
        return false;
    }
    //从lock_manager中获取事务的请求
    std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
    if(table_lock_map_.find(oid) == table_lock_map_.end()){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
        return true;
    }

    auto queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> queue_lock(queue->latch_);
    table_map_lock.unlock();
    LockRequest* request = nullptr;
    for(auto req : queue->request_queue_){
        if(req->txn_id_ == txn->GetTransactionId() && req->granted_ == true){
            request = req;
            break ;
        }
    }


    if(request == nullptr){
        txn->SetState(TransactionState::ABORTED);
        throw  TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
        //return true;
    }

    txn->LockTxn();
    //根据请求中的锁类型和事务的隔离级别修改事务中的信息

    if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING){
        txn->SetState(TransactionState::SHRINKING);
    }else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::GROWING){
        if(request->lock_mode_ == LockMode::EXCLUSIVE || request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE
            || request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
            txn->SetState(TransactionState::SHRINKING);
        }
    }



    //释放锁。
    if(request->lock_mode_ == LockMode::INTENTION_SHARED){
        txn->GetIntentionSharedTableLockSet()->erase(oid);
    } else if(request->lock_mode_ == LockMode::SHARED){
        txn->GetSharedTableLockSet()->erase(oid);
    }else if(request->lock_mode_ == LockMode::EXCLUSIVE){
        txn->GetExclusiveTableLockSet()->erase(oid);
    }else if(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE){
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    } else if(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE){
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

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {

    if(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    }

    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED  && lock_mode == LockMode::SHARED){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if(txn->GetState() == TransactionState::SHRINKING){
        if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }

        if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
            if(lock_mode ==  LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }

        if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
            if(lock_mode == LockMode::EXCLUSIVE){
                txn->SetState(TransactionState::ABORTED);
                throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
            }
        }
    }

    //查看是否与对应表锁兼容
    std::unique_lock<std::mutex> lock(table_lock_map_latch_);
    if(table_lock_map_.find(oid) == table_lock_map_.end()){
        //throw Exception("table lock not exist");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    auto queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> table_queue_lock(queue->latch_);
    lock.unlock();

    for(auto req : queue->request_queue_){
        if(req->oid_ == oid && req->granted_ == true){
            if(lock_mode == LockMode::EXCLUSIVE){
                if(req->lock_mode_ == LockMode::SHARED || req->lock_mode_ == LockMode::EXCLUSIVE){
                    txn->SetState(TransactionState::ABORTED);
                    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
                }
            }else if(lock_mode == LockMode::SHARED){
                //if(req->lock_mode_ != LockMode::INTENTION_SHARED && req->lock_mode_ != LockMode::SHARED ){
                if(req->lock_mode_ == LockMode::EXCLUSIVE){
                    txn->SetState(TransactionState::ABORTED);
                    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
                }
            }
        }
    }

    table_queue_lock.unlock();
    //查看和对应行锁是否兼容

    std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
    if(row_lock_map_.find(rid) == row_lock_map_.end()){
        row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }

    auto row_queue = row_lock_map_[rid];
    std::unique_lock<std::mutex> row_queue_lock(row_queue->latch_);
    row_map_lock.unlock();


    //查看是否重复申请
    for(auto req : row_queue->request_queue_){
        if(req->txn_id_ == txn->GetTransactionId() && req->granted_ == true){
            return true;
        }
    }


    LockRequest *new_request = new LockRequest(txn->GetTransactionId(), lock_mode,oid, rid);
    row_queue->request_queue_.push_back(new_request);


    while(!GrandRowLock(row_queue, new_request)){
        if(txn->GetState() == TransactionState::ABORTED){
            row_queue->request_queue_.remove(new_request);
            delete new_request;
            return false;
        }
        row_queue->cv_.wait(row_queue_lock);
    }

    txn->LockTxn();
    if(lock_mode == LockMode::SHARED){
        txn->GetSharedLockSet()->insert(rid);
        auto temp_map =  txn->GetSharedRowLockSet();
        (*temp_map)[oid].insert(rid);
    } else if(lock_mode == LockMode::EXCLUSIVE){
        txn->GetExclusiveLockSet()->insert(rid);
        auto temp_map = txn->GetExclusiveRowLockSet();
        (*temp_map)[oid].insert(rid);
    }
    txn->UnlockTxn();

    return true;
}

auto LockManager::GrandRowLock(std::shared_ptr<LockRequestQueue> queue, LockRequest *lock_request) -> bool {
    //查看是否兼容
    auto txn = txn_manager_->GetTransaction(lock_request->txn_id_);
    if(txn->GetState() == TransactionState::ABORTED){
        return false;
    }
    for(auto req : queue->request_queue_){
        if(req->granted_ == true){
            if(req->lock_mode_ == LockMode::SHARED && lock_request->lock_mode_ == LockMode::SHARED){
                lock_request->granted_ = true;
                return true;
            }else{
                return false;
            }
        }
    }

    //若当前资源尚未分配
     auto highest_priority = queue->request_queue_.front();
     BUSTUB_ASSERT(highest_priority != nullptr, "highest_priority is nullptr");
     if(highest_priority->txn_id_ == lock_request->txn_id_){
          lock_request->granted_ = true;
          return true;
     }else{
          if(highest_priority->lock_mode_ == LockMode::SHARED && lock_request->lock_mode_ == LockMode::SHARED){
              lock_request->granted_ = true;
              return true;
          }
     }

    return false;
}


auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {

    //找到request
    std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
    if(row_lock_map_.find(rid) == row_lock_map_.end()){
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
          //return true;
    }

    auto row_queue = row_lock_map_[rid];
    std::unique_lock<std::mutex> row_queue_lock(row_queue->latch_);
    row_map_lock.unlock();

    LockRequest *request = nullptr;
    for(auto req : row_queue->request_queue_){
          if(req->txn_id_ == txn->GetTransactionId() && req->granted_ == false){
          }
          if(req->txn_id_ == txn->GetTransactionId() && req->granted_ == true){
              request = req;
              break ;
          }
    }

    if(request == nullptr){
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
          //return true;
    }

    //从lock_manager中删除request
    row_queue->request_queue_.remove(request);

    row_queue->cv_.notify_all();
    txn->LockTxn();
    //修改事务
    if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING){
        txn->SetState(TransactionState::SHRINKING);
    }else if(txn->GetState() == TransactionState::GROWING){
        if(request->lock_mode_ == LockMode::EXCLUSIVE){
            txn->SetState(TransactionState::SHRINKING);
        }
    }

    //释放事务中的锁
    if(request->lock_mode_ == LockMode::SHARED){
        txn->GetSharedLockSet()->erase(rid);
        auto temp_map = txn->GetSharedRowLockSet();
        (*temp_map)[oid].erase(rid);
    }else if(request->lock_mode_ == LockMode::EXCLUSIVE){
        txn->GetExclusiveLockSet()->erase(rid);
        auto temp_map = txn->GetExclusiveRowLockSet();
        (*temp_map)[oid].erase(rid);
    }
    txn->UnlockTxn();
    delete request;
    return true;
}

void LockManager::Print() {
    for(auto i : waits_for_){
        for(auto j : i.second){
            std::cout << "(" << i.first << ", " << j  << ")" << " ";
        }
        std::cout << std::endl;
    }
}
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    //添加一条t1--->t2的边
    if(waits_for_.find(t2) == waits_for_.end()){
        waits_for_[t2] = std::vector<txn_id_t>();
    }

    //维护vector中元素的顺序，从小到大。
    bool flag = false;
    for(auto iter = waits_for_[t2].begin(); iter != waits_for_[t2].end(); iter++){
        if(*iter == t1){
            flag = true;
            break ;
        }else if(*iter < t1 ){
            flag = true;
            waits_for_[t2].insert(iter, t1);
            break ;
        }
    }

    if(flag == false){
        waits_for_[t2].push_back(t1);
    }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    if(waits_for_.find(t2) == waits_for_.end()){
        return;
    }

    for(auto iter = waits_for_[t2].begin(); iter != waits_for_[t2].end(); iter++){
        if(*iter == t1){
            waits_for_[t2].erase(iter);
            return;
        }
    }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {

    for(auto iter = waits_for_.begin(); iter != waits_for_.end(); iter++){
        for(auto &v : visited_){
            v.second = false;
        }

        *txn_id = -1;
        if(hasCycle(iter->first, txn_id)){
            return true;
        }
    }
    return false;
}

auto LockManager::hasCycle(txn_id_t begin_txn_id, txn_id_t *txn_id) -> bool {
    for(auto t : waits_for_[begin_txn_id]){
        if(*txn_id < t){
            *txn_id = t;
        }
        if(visited_.find(t) != visited_.end() && visited_[t] == true){
            return true;
        }

        visited_[t] = true;
        if(hasCycle(t, txn_id)){
            return true;
        }
    }

    return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for(auto iter = waits_for_.begin(); iter != waits_for_.end(); iter++){
      for(auto t : iter->second){
          edges.push_back(std::pair<txn_id_t, txn_id_t>(t, iter->first));
      }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
        //初始化wait_for_;
          //Print();

          std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
        for(auto iter = table_lock_map_.begin(); iter != table_lock_map_.end(); iter++){
            for(auto q : iter->second->request_queue_){
                if(q->granted_ == false){
                    for(auto t : iter->second->request_queue_){
                        if(t->granted_ == true){
                          AddEdge(q->txn_id_, t->txn_id_);
                        }
                    }
                }
            }
        }
        table_map_lock.unlock();
        std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
        for(auto iter = row_lock_map_.begin(); iter != row_lock_map_.end(); iter++){
            for(auto q : iter->second->request_queue_){
                if(q->granted_ == false){
                    for(auto t : iter->second->request_queue_){
                        if(t->granted_ == true){

                           AddEdge(q->txn_id_, t->txn_id_);
                        }
                    }
                }
            }
        }

        row_map_lock.unlock();
        //初始化visited_
        for(auto &i : waits_for_){
            visited_[i.first] = false;
        }

        //Print();

        txn_id_t txn_id = -1;

        while(HasCycle(&txn_id)){
            //将txn_id对应的事务设置为abort
            auto txn = txn_manager_->GetTransaction(txn_id);
            txn->SetState(TransactionState::ABORTED);

            //在请求队列中移除相关事务
            txn->LockTxn();
            std::unordered_map<table_oid_t, std::unordered_set<RID>> row_lock_set;
            for(auto &s_row_lock_set : *txn->GetSharedRowLockSet()){
                for(auto &rid : s_row_lock_set.second){
                    row_lock_set[s_row_lock_set.first].emplace(rid);
                }
            }

            for(auto &w_row_lock_set : *txn->GetExclusiveRowLockSet()){
                for(auto &rid : w_row_lock_set.second){
                    row_lock_set[w_row_lock_set.first].emplace(rid);
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

            /*
            for(auto iter = table_lock_map_.begin(); iter != table_lock_map_.end(); iter++){
                for(auto t : iter->second->request_queue_){
                    if(t->txn_id_ == txn_id){
                        iter->second->request_queue_.remove(t);
                        //delete t;
                        iter->second->cv_.notify_all();
                        break ;
                    }
                }
            }

            for(auto iter = row_lock_map_.begin(); iter != row_lock_map_.end(); iter++){
                for(auto t : iter->second->request_queue_){
                    if(t->txn_id_ == txn_id){
                        iter->second->request_queue_.remove(t);
                        //delete t;
                        iter->second->cv_.notify_all();
                        break ;
                    }
                }
            }
             */
            //Print();
            //移除边
            waits_for_.erase(txn_id);
            for(auto iter = waits_for_.begin(); iter != waits_for_.end(); iter++){
                for(auto t = iter->second.begin(); t != iter->second.end(); t++){
                    if(*t == txn_id){
                        iter->second.erase(t);
                        break;
                    }
                }
            }

            //是否要移除visited_中的相关信息？
        }
    }
  }
}

}  // namespace bustub
