//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKey(const KeyType &key, KeyComparator comparator) -> ValueType {
  if (comparator(key, KeyAt(1)) < 0) {
    return ValueAt(0);
  }
  int left = 1;
  int right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right + 1) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else {
      left = mid;
    }
  }
  return ValueAt(left);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Move(BPlusTreeInternalPage *page_data, BufferPoolManager *buffer_pool_manager) {
  int maxsize = GetMaxSize();
  int minsize = GetMinSize();
  // after insert internalpage have maxsize+1 size then spilt
  for (auto i = minsize; i <= maxsize; i++) {
    SetKeyAt(i - minsize, page_data->KeyAt(i));
    SetValueAt(i - minsize, page_data->ValueAt(i));
    auto child_page_id = page_data->ValueAt(i);
    auto child_page = buffer_pool_manager->FetchPage(child_page_id);
    auto child_page_data = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
    child_page_data->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  IncreaseSize(maxsize - minsize + 1);
  page_data->IncreaseSize(minsize - maxsize - 1);
}
/*当中间节点只有一个key时进行插入会出错（remove时可能出现这种情况先删除了key再进行合并或者borrow，这时候直接用setkey和setvalue*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator comparator) -> bool {
  if (comparator(key, KeyAt(1)) < 0) {
    for (int i = GetSize() - 1; i >= 1; i--) {
      SetKeyAt(i + 1, KeyAt(i));
      SetValueAt(i + 1, ValueAt(i));
    }
    SetKeyAt(1, key);
    SetValueAt(1, value);
    IncreaseSize(1);
    return true;
  }
  int left = 1;
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
    for (int i = GetSize() - 1; i >= left + 1; i--) {
      SetKeyAt(i + 1, KeyAt(i));
      SetValueAt(i + 1, ValueAt(i));
    }
    SetKeyAt(left + 1, key);
    SetValueAt(left + 1, value);
  } else if (comparator(key, KeyAt(left)) < 0) {
    for (int i = GetSize() - 1; i >= left; i--) {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) {
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      for (int i = mid; i < GetSize() - 1; i++) {
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
      }
      IncreaseSize(-1);
      return;
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetLeftPage(const ValueType &value) -> ValueType {
  int index = ValueIndex(value);
  if (index == 0) {
    return INVALID_PAGE_ID;
  }
  return array_[index - 1].second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetRightPage(const ValueType &value) -> ValueType {
  int index = ValueIndex(value);
  if (index == GetSize() - 1) {
    return INVALID_PAGE_ID;
  }
  return array_[index + 1].second;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::AppendFirst(KeyType key, ValueType value) {
  for (int i = GetSize() - 1; i >= 0; i--) {
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFirst() {
  for (int i = 1; i <= GetSize() - 1; i++) {
    SetKeyAt(i - 1, KeyAt(i));
    SetValueAt(i - 1, ValueAt(i));
  }
  IncreaseSize(-1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
