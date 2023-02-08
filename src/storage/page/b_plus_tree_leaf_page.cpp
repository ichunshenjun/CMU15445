//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetParentPageId(parent_id);
  SetSize(0);
  SetPageId(page_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
  // KeyType key{};
  // return key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
  // KeyType key{};
  // return key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, KeyComparator comparator) -> bool {
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      return true;
    }
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ReturnValue(const KeyType &key, ValueType *value, KeyComparator comparator) -> bool {
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      *value = ValueAt(mid);
      return true;
    }
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Move(BPlusTreeLeafPage *page_data) {
  int maxsize = GetMaxSize();
  int minsize = GetMinSize();
  for (auto i = minsize; i < maxsize; i++) {
    SetKeyAt(i - minsize, page_data->KeyAt(i));
    SetValueAt(i - minsize, page_data->ValueAt(i));
  }
  IncreaseSize(maxsize - minsize);
  page_data->IncreaseSize(minsize - maxsize);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator comparator) -> bool {
  if (GetSize() == 0) {
    SetKeyAt(0, key);
    SetValueAt(0, value);
    IncreaseSize(1);
    return true;
  }
  int left = 0;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      return false;
    }
  }
  if (comparator(key, KeyAt(left)) == 0) {
    return false;
  }
  if (comparator(key, KeyAt(left)) > 0) {
    for (int i = left + 1; i < GetSize(); i++) {
      SetKeyAt(i + 1, KeyAt(i));
      SetValueAt(i + 1, ValueAt(i));
    }
    SetKeyAt(left + 1, key);
    SetValueAt(left + 1, value);
  }
  if (comparator(key, KeyAt(left)) < 0) {
    for (int i = left; i < GetSize(); i++) {
      SetKeyAt(i + 1, KeyAt(i));
      SetValueAt(i + 1, ValueAt(i));
    }
    SetKeyAt(left, key);
    SetValueAt(left, value);
  }
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key,KeyComparator comparator){
  int left=0;
  int right=GetSize()-1;
  while (left<=right){
    int mid=(left+right)/2;
    if(comparator(key,KeyAt(mid))<0){
      right=mid-1;
    }else if(comparator(key,KeyAt(mid))>0){
      left=mid+1;
    }
    else{
      for(int i=mid;i<GetSize()-1;i++){
        SetKeyAt(i,KeyAt(i+1));
        SetValueAt(i,ValueAt(i+1));
      }
      IncreaseSize(-1);
      return;
    }
  }
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
