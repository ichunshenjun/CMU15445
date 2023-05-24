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

#include <algorithm>
#include <memory>
#include <mutex>  // NOLINT
#include <stdexcept>
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw std::logic_error("事务状态异常");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if ((request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::SHARED ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode != LockMode::EXCLUSIVE)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteTableLockSet(txn, request, false);
      auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      lock_request_queue->request_queue_.emplace_back(new_request);  // 当前只有一个锁升级请求优先级最高
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(new_request, lock_request_queue, true)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(new_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      new_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, new_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(new_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(new_request, lock_request_queue, false)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->request_queue_.remove(new_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  new_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, new_request, true);
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
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
          (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          (lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
          (lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
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
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw std::logic_error("事务状态异常");
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  // if(txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED){
  //   if(lock_mode==LockMode::SHARED){
  //     txn->SetState(TransactionState::ABORTED);
  //     throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  //   }
  //   if(txn->GetState()==TransactionState::SHRINKING&&lock_mode==LockMode::EXCLUSIVE){
  //     txn->SetState(TransactionState::ABORTED);
  //     throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  //   }
  // }
  // if(txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED){
  //   if(txn->GetState()==TransactionState::SHRINKING&&lock_mode==LockMode::EXCLUSIVE)
  // }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (request->lock_mode_ != LockMode::SHARED || lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);
      auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      lock_request_queue->request_queue_.emplace_back(new_request);  // 当前只有一个锁升级请求优先级最高
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(new_request, lock_request_queue, true)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(new_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      new_request->granted_ = true;
      InsertOrDeleteRowLockSet(txn, new_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(new_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(new_request, lock_request_queue, false)) {
    lock_request_queue->cv_.wait(lock);
    // LOG_DEBUG("%d被唤醒了",txn->GetTransactionId());
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->request_queue_.remove(new_request);
      // LOG_DEBUG("删除请求%d",txn->GetTransactionId());
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  new_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, new_request, true);
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
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
          (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          (lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
          (lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
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

void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, std::shared_ptr<LockRequest> &lock_request, bool insert) {
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
void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, std::shared_ptr<LockRequest> &lock_request,
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

auto LockManager::GrantLock(std::shared_ptr<LockRequest> &lock_request,
                            std::shared_ptr<LockRequestQueue> &lock_request_queue, bool upgrading) -> bool {
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->granted_) {
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (request->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() == request.get()) {
      return true;
    } else {
      if (upgrading) {
        return true;
      }
      // 如果不是锁升级必须判断与前面的等待锁是否兼容
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (request->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    }
    // else if(lock_request.get()!=request.get()){
    //   return false;
    // }
    // else{
    //   return true;
    // }
  }
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::vector<txn_id_t>{});
  }
  waits_for_[t1].emplace_back(t2);
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    return;
  }
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  waits_for_[t1].erase(iter);
  waits_for_latch_.unlock();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (auto edge : waits_for_) {  // NOLINT
    cycle_txn_id_.clear();
    if (DFS(edge.first)) {
      *txn_id = *std::max_element(cycle_txn_id_.begin(), cycle_txn_id_.end());
      // LOG_DEBUG("find %d",*txn_id);
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto pair : waits_for_) {  // NOLINT
    for (auto t2 : pair.second) {
      edges.emplace_back(pair.first, t2);
    }
  }
  return edges;
}

auto LockManager::IsCompatible(std::shared_ptr<LockRequest> &waiting_request,
                               std::shared_ptr<LockRequest> &granted_request) -> bool {
  switch (waiting_request->lock_mode_) {
    case LockMode::SHARED:
      if (granted_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
          granted_request->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
    case LockMode::INTENTION_SHARED:
      if (granted_request->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (granted_request->lock_mode_ == LockMode::SHARED ||
          granted_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
          granted_request->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (granted_request->lock_mode_ != LockMode::INTENTION_SHARED) {
        return false;
      }
      break;
  }
  return true;
}
auto LockManager::DFS(txn_id_t txn_id) -> bool {
  if (cycle_txn_id_.find(txn_id) != cycle_txn_id_.end()) {
    return true;
  }
  cycle_txn_id_.insert(txn_id);
  auto edge_vec = waits_for_[txn_id];
  for (auto edge : edge_vec) {  // NOLINT
    if (DFS(edge)) {
      return true;
    }
  }
  cycle_txn_id_.erase(txn_id);
  return false;
}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for (auto iter = table_lock_map_.begin(); iter != table_lock_map_.end(); iter++) {  // NOLINT
        table_lock_map_[iter->first]->latch_.lock();
        auto lock_request_queue = iter->second->request_queue_;
        for (auto granted_request : lock_request_queue) {  // NOLINT
          if (granted_request->granted_) {
            for (auto waiting_request : lock_request_queue) {  // NOLINT
              if (!waiting_request->granted_) {
                txn_table_map_.emplace(waiting_request->txn_id_, waiting_request->oid_);
                if (!IsCompatible(waiting_request, granted_request)) {
                  AddEdge(waiting_request->txn_id_, granted_request->txn_id_);
                  // LOG_DEBUG("%d %d",waiting_request->txn_id_,granted_request->txn_id_);
                }
              }
            }
          }
        }
        table_lock_map_[iter->first]->latch_.unlock();
      }
      for (auto iter = row_lock_map_.begin(); iter != row_lock_map_.end(); iter++) {  // NOLINT
        row_lock_map_[iter->first]->latch_.lock();
        auto lock_request_queue = iter->second->request_queue_;
        for (auto granted_request : lock_request_queue) {  // NOLINT
          if (granted_request->granted_) {
            for (auto waiting_request : lock_request_queue) {  // NOLINT
              if (!waiting_request->granted_) {
                txn_row_map_.emplace(waiting_request->txn_id_, waiting_request->rid_);
                if (!IsCompatible(waiting_request, granted_request)) {
                  AddEdge(waiting_request->txn_id_, granted_request->txn_id_);
                  // LOG_DEBUG("%d %d",waiting_request->txn_id_,granted_request->txn_id_);
                }
              }
            }
          }
        }
        row_lock_map_[iter->first]->latch_.unlock();
      }
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        for (auto t2 : waits_for_[txn_id]) {
          RemoveEdge(txn_id, t2);
        }
        waits_for_.erase(txn_id);
        // LOG_DEBUG("erase %d",txn_id);
        if (txn_table_map_.count(txn_id) > 0) {
          table_lock_map_[txn_table_map_[txn_id]]->latch_.lock();
          table_lock_map_[txn_table_map_[txn_id]]->cv_.notify_all();
          table_lock_map_[txn_table_map_[txn_id]]->latch_.unlock();
        }
        if (txn_row_map_.count(txn_id) > 0) {
          row_lock_map_[txn_row_map_[txn_id]]->latch_.lock();
          row_lock_map_[txn_row_map_[txn_id]]->cv_.notify_all();
          row_lock_map_[txn_row_map_[txn_id]]->latch_.unlock();
        }
      }
      waits_for_.clear();
      txn_table_map_.clear();
      txn_row_map_.clear();
      cycle_txn_id_.clear();
    }
  }
}

}  // namespace bustub
