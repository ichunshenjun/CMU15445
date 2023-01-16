//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <exception>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (!lru_.empty()) {
    for (auto it = lru_.begin(); it != lru_.end(); it++) {
      if (it->second.evictable_) {
        *frame_id = it->first;
        lru_.erase(it);
        curr_size_--;
        return true;
      }
    }
  }
  if (!lru_k_.empty()) {
    for (auto it = lru_k_.begin(); it != lru_k_.end(); it++) {
      if (it->second.evictable_) {
        *frame_id = it->first;
        lru_k_.erase(it);
        curr_size_--;
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  for (auto it = lru_.begin(); it != lru_.end(); it++) {
    if (it->first == frame_id) {
      auto hit_count = ++(it->second.hit_count_);
      if (hit_count == k_) {
        auto temp = std::make_pair(it->first, it->second);
        lru_.erase(it);
        lru_k_.push_back(temp);
      }
      return;
    }
  }
  for (auto it = lru_k_.begin(); it != lru_k_.end(); it++) {
    if (it->first == frame_id) {
      (it->second.hit_count_)++;
      auto temp = std::make_pair(it->first, it->second);
      lru_k_.erase(it);
      lru_k_.push_back(temp);
      return;
    }
  }
  if (lru_.size() + lru_k_.size() == replacer_size_) {
    // frame_id_t fid;
    // Evict(&fid);
    return;
  }
  FrameEntry new_entry;
  new_entry.hit_count_ = 1;
  auto new_frame = std::make_pair(frame_id, new_entry);
  lru_.push_back(new_frame);
  curr_size_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  for (auto &it : lru_) {
    if (it.first == frame_id) {
      auto origin_evictable = it.second.evictable_;
      it.second.evictable_ = set_evictable;
      if (origin_evictable && !set_evictable) {
        // replacer_size_--;
        curr_size_--;
      } else if (!origin_evictable && set_evictable) {
        // replacer_size_++;
        curr_size_++;
      }
      return;
    }
  }
  for (auto &it : lru_k_) {
    if (it.first == frame_id) {
      auto origin_evictable = it.second.evictable_;
      it.second.evictable_ = set_evictable;
      if (origin_evictable && !set_evictable) {
        // replacer_size_--;
        curr_size_--;
      } else if (!origin_evictable && set_evictable) {
        // replacer_size_++;
        curr_size_++;
      }
      return;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  for (auto it = lru_.begin(); it != lru_.end(); it++) {
    if (it->first == frame_id) {
      if (!it->second.evictable_) {
        throw std::exception();
      }
      lru_.erase(it);
      curr_size_--;
      return;
    }
  }
  for (auto it = lru_k_.begin(); it != lru_k_.end(); it++) {
    if (it->first == frame_id) {
      if (!it->second.evictable_) {
        throw std::exception();
      }
      lru_k_.erase(it);
      curr_size_--;
      return;
    }
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  // auto size=lru_.size()+lru_k_.size();
  // for(auto &it:lru_){
  //     if(!it.second.evictable_){
  //         size--;
  //     }
  // }
  // for(auto &it:lru_k_){
  //     if(!it.second.evictable_){
  //         size--;
  //     }
  // }
  // return size;
  return curr_size_;
}

}  // namespace bustub
