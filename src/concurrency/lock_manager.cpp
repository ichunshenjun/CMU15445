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
#include <list>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // judge isolation
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
        lock_mode != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // get lock queue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // examine lock
  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // 已经存在同样的请求
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      // 已经有别的事务在进行锁升级
      if (lock_request_queue->upgrading_ != INVALID_PAGE_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // 判断锁升级是否匹配
      if ((request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::SHARED ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE || (lock_mode != LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // 进行锁升级前更新相应的请求队列和锁表，先释放原来表持有的锁
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteTableLockSet(txn, request, false);
      // 将这次锁升级作为请求插入队列中
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      // 检查升级请求时等待资源可用，并在等待期间保持线程处于阻塞状态。
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_,
                                        std::adopt_lock);  // 第二个参数假定该线程已经拥有了这个锁
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        // 如果不能被授予锁那么释放当前的锁并且等待
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // 赋予需要升级的锁
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);
      // 该线程获取独占锁的情况，别的线程没必要进行锁升级了，因为无法获取升级锁
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  // 如果请求队列为空的情况
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  // 应该先释放行锁再释放表锁才是正确的
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      InsertOrDeleteTableLockSet(txn, lock_request, false);
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 需要先获取表级意向锁再获取行级锁，不能获取行级意向锁
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  // judge isolation
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
        lock_mode != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  // get lock queue
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // examine lock
  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // 已经存在同样的请求
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      // 已经有别的事务在进行锁升级
      if (lock_request_queue->upgrading_ != INVALID_PAGE_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // 判断锁升级是否匹配
      if ((request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::SHARED ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE || (lock_mode != LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // 进行锁升级前更新相应的请求队列和锁表，先释放原来事务持有的锁
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);
      // 将这次锁升级作为请求插入队列中
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      // 检查升级请求时等待资源可用，并在等待期间保持线程处于阻塞状态。
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // 赋予需要升级的锁
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      InsertOrDeleteRowLockSet(txn, upgrade_lock_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  // 如果请求队列为空的情况
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  // 这里用while是为了防止虚假唤醒，操作系统内核可能会无意间唤醒这些等待的线程即使没有满足唤醒条件
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      InsertOrDeleteRowLockSet(txn, lock_request, false);
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter=std::find(waits_for_[t1].begin(),waits_for_[t1].end(),t2);
  if(iter!=waits_for_[t1].end()){
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  for(const auto &pair:waits_for_){
    if(DFS(pair.first)){
      *txn_id=*cycle_txn_id_.begin();
      for(const auto &cycle_txn_id:cycle_txn_id_){
        *txn_id=std::max(*txn_id,cycle_txn_id);
      }
      cycle_txn_id_.clear();
      return true;
    }
    cycle_txn_id_.clear();
  }
  return false; 
}

void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}
void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert) {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        InsertRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        InsertRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}
// 检查当前锁请求与之前已经赋予的锁是否冲突（即是否可以加锁）
auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for(const auto &pair:waits_for_){
    for(const auto &t2:pair.second){
      edges.emplace_back(pair.first,t2);
    }
  }
  return edges;
}

// 这里认为没被授权的请求与之前被授予的请求在竞争同一资源，应该都构建边
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for(auto &pair:table_lock_map_){
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for(auto const & lock_request:pair.second->request_queue_){
          if(lock_request->granted_){
            granted_set.emplace(lock_request->txn_id_);
          }else{
            for(const auto &txn_id:granted_set){
              txn_table_map_.emplace(lock_request->txn_id_,lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock();
      }
      for(auto &pair:row_lock_map_){
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for(auto const & lock_request:pair.second->request_queue_){
          if(lock_request->granted_){
            granted_set.emplace(lock_request->txn_id_);
          }else{
            for(const auto &txn_id:granted_set){
              txn_row_map_.emplace(lock_request->txn_id_,lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock();
      }
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();
      txn_id_t txn_id;
      while(HasCycle(&txn_id)){
        Transaction *txn=TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        waits_for_.erase(txn_id);
        for(auto [t1,v]:waits_for_){
          if(t1!=txn_id){
            RemoveEdge(t1, txn_id);
          }
        }
        if(txn_table_map_.count(txn_id)>0){
          table_lock_map_[txn_table_map_[txn_id]]->latch_.lock();
          table_lock_map_[txn_table_map_[txn_id]]->cv_.notify_all();
          table_lock_map_[txn_table_map_[txn_id]]->latch_.unlock();
        }
        if(txn_row_map_.count(txn_id)>0){
          row_lock_map_[txn_row_map_[txn_id]]->latch_.lock();
          row_lock_map_[txn_row_map_[txn_id]]->cv_.notify_all();
          row_lock_map_[txn_row_map_[txn_id]]->latch_.unlock();
        }
        searched_txn_id_.clear();
        waits_for_.clear();
        txn_table_map_.clear();
        txn_row_map_.clear();
      }
    }
  }
}
// 按算法来说cycle_txn_id_里应该不只是圈的结点，难道不存在1->3->6->2->6这种，只会有6->2->6吗？
auto LockManager::DFS(txn_id_t txn_id) -> bool{
  if(searched_txn_id_.find(txn_id)!=searched_txn_id_.end()){
    return false;
  }
  cycle_txn_id_.insert(txn_id);
  auto start=waits_for_[txn_id];
  std::sort(start.begin(),start.end());
  for(int iter : start){
    if(cycle_txn_id_.find(iter)!=cycle_txn_id_.end()){
      return true;
    }
    if(DFS(iter)){
      return true;
    }
  }
  cycle_txn_id_.erase(txn_id);
  searched_txn_id_.insert(txn_id);
  return false;
}
}  // namespace bustub
