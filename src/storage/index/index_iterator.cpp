/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *buffer_pool_manager)
    : index_(index), leaf_(leaf), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return index_ == leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // throw std::runtime_error("unimplemented");
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // throw std::runtime_error("unimplemented");
  if (index_ == leaf_->GetSize() - 1 && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page_id = leaf_->GetNextPageId();
    index_ = 0;
    // unlock leaf
    auto page = buffer_pool_manager_->FetchPage(leaf_->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    page->RUnlatch();
    // LOG_DEBUG("page %d is unlocked",page->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);

    auto next_page = buffer_pool_manager_->FetchPage(next_page_id);
    // lock next leaf
    next_page->RLatch();
    // LOG_DEBUG("page %d is locked",next_page_id);
    leaf_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(next_page->GetData());
  } 
  else if (index_ == leaf_->GetSize() - 1 && leaf_->GetNextPageId() == INVALID_PAGE_ID) {
    index_++;
    auto page = buffer_pool_manager_->FetchPage(leaf_->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    page->RUnlatch();
    // LOG_DEBUG("page %d is unlocked",page->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  } 
  else {
    index_++;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
