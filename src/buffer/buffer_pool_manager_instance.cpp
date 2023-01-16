//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  //   // TODO(students): remove this line after you have implemented the buffer pool manager
  //   throw NotImplementedException(
  //       "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //       "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
auto BufferPoolManagerInstance::FindNewPg(frame_id_t &fid) -> Page * {
  bool if_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      if_free_page = true;
      break;
    }
  }

  if (!if_free_page) {
    // LOG_INFO("NewPgImp:: bufferpool full\n");
    return nullptr;
  }
  if (!free_list_.empty()) {
    fid = free_list_.front();
    // LOG_DEBUG("list page %d",fid);
    free_list_.pop_front();
    return &(pages_[fid]);
  }
  if (replacer_->Evict(&fid)) {
    // LOG_DEBUG("evict page %d",fid);
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
      pages_[fid].is_dirty_ = false;
    }
    pages_[fid].ResetMemory();
    page_table_->Remove(pages_[fid].GetPageId());
    return &(pages_[fid]);
  }
  return nullptr;
}
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  Page *page = FindNewPg(fid);
  if (page == nullptr) {
    return nullptr;
  }
  page->page_id_ = AllocatePage();
  if (page->page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  *page_id = page->page_id_;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page_table_->Insert(page->GetPageId(), fid);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  frame_id_t fid;
  if (page_table_->Find(page_id, fid)) {
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    pages_[fid].pin_count_++;
    // replacer_->Remove(fid);
    return &(pages_[fid]);
  }
  Page *page = FindNewPg(fid);
  if (page != nullptr) {
    pages_[fid].pin_count_ = 1;
    pages_[fid].page_id_ = page_id;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    disk_manager_->ReadPage(page_id, page->data_);
    page_table_->Insert(page_id, fid);
    return &pages_[fid];
  }
  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid)) {
    if (pages_[fid].GetPinCount() <= 0) {
      LOG_INFO("UnpinPgImp:: invlid unpin! frame id  %d pin count %d\n", fid, pages_[fid].GetPinCount());
      return false;
    }
    // if (pages_[fid].pin_count_ == 0) {
    //   replacer_->SetEvictable(fid, true);
    //   return false;
    // }
    pages_[fid].pin_count_--;
    // LOG_DEBUG("unpin page %d,pin_count %d",fid,pages_[fid].pin_count_);
    if (pages_[fid].pin_count_ == 0) {
      // LOG_DEBUG("page %d", fid);
      // replacer_->RecordAccess(fid);
      replacer_->SetEvictable(fid, true);
    }
    if (is_dirty) {
      pages_[fid].is_dirty_ = true;
    }
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_->Find(page_id, fid)) {
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[fid].GetData());
      pages_[fid].is_dirty_ = false;
    }
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return true;
  }
  if (pages_[fid].GetPinCount() != 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(fid);
  free_list_.push_back(fid);
  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
