#include "storage/index/b_plus_tree.h"
#include <cassert>
#include <cstddef>
#include <exception>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::root_locked_cnt = 0;
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto leaf_page = FindLeafPage(key, OpType::READ, transaction);
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType value;
  if (leaf_page_data->ReturnValue(key, &value, comparator_)) {
    result->push_back(value);
    FreePagesInTransaction(false, transaction, leaf_page_data->GetPageId());
    return true;
  }
  // buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false);
  FreePagesInTransaction(false, transaction, leaf_page_data->GetPageId());
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  LockRootPageId(true);
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (root_page == nullptr) {
      throw std::exception();
    }
    UpdateRootPageId(1);
    auto root_page_data = reinterpret_cast<LeafPage *>(root_page->GetData());
    root_page_data->SetPageType(IndexPageType::LEAF_PAGE);
    root_page_data->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    root_page_data->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    TryUnlockRootPageId(true);
    return true;
  }
  TryUnlockRootPageId(true);
  return InsertIntoLeaf(key, value, transaction);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto leaf_page = FindLeafPage(key, OpType::INSERT, transaction);
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (leaf_page_data->FindKey(key, comparator_)) {
    FreePagesInTransaction(true, transaction, -1);
    return false;
  }
  bool res = leaf_page_data->Insert(key, value, comparator_);
  if (leaf_page_data->GetSize() >= leaf_page_data->GetMaxSize()) {
    auto new_leaf_page_data = Split(leaf_page_data, transaction);
    auto first_key = new_leaf_page_data->KeyAt(0);
    InsertIntoParent(leaf_page_data, first_key, new_leaf_page_data, transaction);
    // buffer_pool_manager_->UnpinPage(new_leaf_page_data->GetPageId(), true);
  }
  // buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), true);
  FreePagesInTransaction(true, transaction, -1);
  return res;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, OpType op, Transaction *transaction, bool mostLeft) -> Page * {
  bool exclusive = (op != OpType::READ);
  LockRootPageId(exclusive);
  auto find_page_id = root_page_id_;
  auto new_page_id = INVALID_PAGE_ID;
  auto find_page = CrabingProtocalFetchPage(find_page_id, op, -1, transaction);
  if (find_page == nullptr) {
    TryUnlockRootPageId(exclusive);
    return nullptr;
  }
  auto find_page_data = reinterpret_cast<BPlusTreePage *>(find_page->GetData());
  while (!find_page_data->IsLeafPage()) {
    auto internal_page_data = reinterpret_cast<InternalPage *>(find_page_data);
    if (mostLeft) {
      new_page_id = internal_page_data->ValueAt(0);
    } else {
      new_page_id = internal_page_data->FindKey(key, comparator_);
    }
    // buffer_pool_manager_->UnpinPage(find_page_id, false);
    find_page = CrabingProtocalFetchPage(new_page_id, op, find_page_id, transaction);
    find_page_id = new_page_id;
    if (find_page == nullptr) {
      throw std::exception();
    }
    find_page_data = reinterpret_cast<BPlusTreePage *>(find_page->GetData());
  }
  return find_page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id, OpType op, page_id_t previous,
                                              Transaction *transaction) -> Page * {
  bool exclusive = (op != OpType::READ);
  auto page = buffer_pool_manager_->FetchPage(page_id);
  Lock(exclusive, page);
  auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // 判断是否需要释放前一个页面的访问权限
  if (previous > 0 && (!exclusive || page_data->IsSafe(op))) {
    FreePagesInTransaction(exclusive, transaction, previous);
  }

  // 将页面加入事务的页面集合
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }

  return page;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t previous) {
  TryUnlockRootPageId(exclusive);
  if (transaction == nullptr) {
    assert(!exclusive && previous >= 0);
    Unlock(exclusive, previous);
    buffer_pool_manager_->UnpinPage(previous, false);
    return;
  }
  for (Page *page : *transaction->GetPageSet()) {
    page_id_t pre_page_id = page->GetPageId();
    Unlock(exclusive, page);
    buffer_pool_manager_->UnpinPage(pre_page_id, exclusive);
    if (transaction->GetDeletedPageSet()->find(pre_page_id) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(pre_page_id);
      transaction->GetDeletedPageSet()->erase(pre_page_id);
    }
  }
  // assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}
INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
auto BPLUSTREE_TYPE::Split(ClassType *page_data, Transaction *transaction) -> ClassType * {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::exception();
  }
  new_page->WLatch();
  transaction->AddIntoPageSet(new_page);
  auto temp_page_data = reinterpret_cast<ClassType *>(new_page->GetData());  // 还要使temp_page_data转为另外两种类型
  temp_page_data->SetPageType(page_data->GetPageType());
  if (temp_page_data->IsLeafPage()) {
    auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
    auto new_page_data = reinterpret_cast<LeafPage *>(temp_page_data);
    new_page_data->Init(new_page_id, leaf_page_data->GetParentPageId(), leaf_page_data->GetMaxSize());
    new_page_data->Move(leaf_page_data);
    new_page_data->SetNextPageId(leaf_page_data->GetNextPageId());
    leaf_page_data->SetNextPageId(new_page_data->GetPageId());
  } else {
    auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
    auto new_page_data = reinterpret_cast<InternalPage *>(temp_page_data);
    new_page_data->Init(new_page_id, internal_page_data->GetParentPageId(), internal_page_data->GetMaxSize());
    new_page_data->Move(internal_page_data, buffer_pool_manager_);
  }
  return temp_page_data;
}
INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
void BPLUSTREE_TYPE::InsertIntoParent(ClassType *page_data, KeyType key, ClassType *new_leaf_page_data,
                                      Transaction *transaction) {
  if (page_data->IsRootPage()) {
    page_id_t new_root_page_id;
    auto new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    auto new_root_page_data = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_page_data->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_page_data->SetValueAt(0, page_data->GetPageId());
    new_root_page_data->SetKeyAt(1, key);
    new_root_page_data->SetValueAt(1, new_leaf_page_data->GetPageId());
    new_root_page_data->IncreaseSize(2);
    page_data->SetParentPageId(new_root_page_id);
    new_leaf_page_data->SetParentPageId(new_root_page_id);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  auto parent_page_id = page_data->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_page_data = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_page_data->Insert(key, new_leaf_page_data->GetPageId(), comparator_);
  if (parent_page_data->GetSize() > parent_page_data->GetMaxSize()) {
    // parent_page_data->Insert(key,new_leaf_page_data->ValueAt(0),comparator_);
    auto new_parent_page_data = Split(parent_page_data, transaction);
    auto parent_first_key = new_parent_page_data->KeyAt(0);
    InsertIntoParent(parent_page_data, parent_first_key, new_parent_page_data, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto leaf_page = FindLeafPage(key, OpType::DELETE, transaction);
  // LOG_DEBUG("find success");
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (!leaf_page_data->FindKey(key, comparator_)) {
    // buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false);
    FreePagesInTransaction(true, transaction, -1);
    return;
  }
  DeleteKey(leaf_page_data, key, transaction);
  // buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), true);
  FreePagesInTransaction(true, transaction, -1);
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteKey(BPlusTreePage *page_data, const KeyType &key, Transaction *transaction) {
  if (page_data->IsLeafPage()) {
    auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
    leaf_page_data->Remove(key, comparator_);
  } else {
    auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
    internal_page_data->Remove(key, comparator_);
  }
  if (page_data->IsRootPage() && page_data->GetSize() == 0){
    root_page_id_ = INVALID_PAGE_ID;
  }
  if (page_data->IsRootPage() && page_data->GetSize() == 1 && !page_data->IsLeafPage()) {
    auto root_page_data = reinterpret_cast<InternalPage *>(page_data);
    auto root_page_id = root_page_data->GetPageId();
    auto new_root_page_id = root_page_data->ValueAt(0);
    auto new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
    assert(new_root_page);
    auto new_root_page_data = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_page_data->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_=new_root_page_id;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    // buffer_pool_manager_->DeletePage(root_page_id);
    transaction->AddIntoDeletedPageSet(root_page_id);
    return;
  }
  if (!page_data->IsRootPage() && page_data->GetSize() < page_data->GetMinSize()) {
    auto parent_page = buffer_pool_manager_->FetchPage(page_data->GetParentPageId());
    assert(parent_page);
    auto parent_page_data = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // Get left page
    auto left_page_id = parent_page_data->GetLeftPage(page_data->GetPageId());
    Page *left_page = nullptr;
    BPlusTreePage *left_page_data = nullptr;
    if (left_page_id != INVALID_PAGE_ID) {
      // left_page = buffer_pool_manager_->FetchPage(left_page_id);
      left_page = CrabingProtocalFetchPage(left_page_id, OpType::DELETE, -1, transaction);
      left_page_data = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
    }
    // Get right page
    auto right_page_id = parent_page_data->GetRightPage(page_data->GetPageId());
    Page *right_page = nullptr;
    BPlusTreePage *right_page_data = nullptr;
    if (right_page_id != INVALID_PAGE_ID) {
      // right_page = buffer_pool_manager_->FetchPage(right_page_id);
      right_page = CrabingProtocalFetchPage(right_page_id, OpType::DELETE, -1, transaction);
      right_page_data = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    }
    buffer_pool_manager_->UnpinPage(parent_page_data->GetPageId(), false);
    int maxsize=0;
    if(page_data->IsLeafPage()){
      maxsize=page_data->GetMaxSize();
    }
    else{
      maxsize=page_data->GetMaxSize()+1;
    }
    if (left_page_data != nullptr && left_page_data->GetSize() + page_data->GetSize() >= maxsize) {
      Borrow(left_page_data, page_data, parent_page_data->ValueIndex(page_data->GetPageId()));
    } else if (right_page_data != nullptr &&
               right_page_data->GetSize() + page_data->GetSize() >= maxsize) {
      Borrow(right_page_data, page_data, parent_page_data->ValueIndex(right_page_data->GetPageId()));
    } else if (left_page_data != nullptr &&
               left_page_data->GetSize() + page_data->GetSize() < maxsize) {
      Merge(left_page_data, page_data, parent_page_data->ValueIndex(page_data->GetPageId()), transaction);
    } else if (right_page_data != nullptr &&
               right_page_data->GetSize() + page_data->GetSize() < maxsize) {
      Merge(right_page_data, page_data, parent_page_data->ValueIndex(right_page_data->GetPageId()), transaction);
    }
    // if (left_page_id != INVALID_PAGE_ID) {
    //   buffer_pool_manager_->UnpinPage(left_page_id, true);
    // }
    // if (right_page_id != INVALID_PAGE_ID) {
    //   buffer_pool_manager_->UnpinPage(right_page_id, true);
    // }
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Borrow(BPlusTreePage *sibling_page_data, BPlusTreePage *page_data, int index) {
  auto parent_page_id = page_data->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_page_data = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // sibling node|node
  if (page_data->GetPageId() == parent_page_data->ValueAt(index)) {
    if (page_data->IsLeafPage()) {
      auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
      auto sibling_leaf_data = reinterpret_cast<LeafPage *>(sibling_page_data);
      auto last_key = sibling_leaf_data->KeyAt(sibling_leaf_data->GetSize() - 1);
      auto last_value = sibling_leaf_data->ValueAt(sibling_leaf_data->GetSize() - 1);
      sibling_leaf_data->Remove(last_key, comparator_);
      leaf_page_data->AppendFirst(last_key, last_value);
      // update parent key
      parent_page_data->SetKeyAt(index, last_key);
    } else {
      auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
      auto sibling_internal_data = reinterpret_cast<InternalPage *>(sibling_page_data);
      auto last_key = sibling_internal_data->KeyAt(sibling_internal_data->GetSize() - 1);
      auto last_value = sibling_internal_data->ValueAt(sibling_internal_data->GetSize() - 1);
      sibling_internal_data->Remove(last_key, comparator_);
      internal_page_data->AppendFirst(last_key, last_value);
      // update child
      auto child_page = buffer_pool_manager_->FetchPage(last_value);
      auto child_page_data = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_page_data->SetParentPageId(internal_page_data->GetPageId());
      buffer_pool_manager_->UnpinPage(last_value, true);
      // update parent key
      parent_page_data->SetKeyAt(index, last_key);
    }
  } else {  // node|sibling node
    if (page_data->IsLeafPage()) {
      auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
      auto sibling_leaf_data = reinterpret_cast<LeafPage *>(sibling_page_data);
      auto first_key = sibling_leaf_data->KeyAt(0);
      auto first_value = sibling_leaf_data->ValueAt(0);
      sibling_leaf_data->Remove(first_key, comparator_);
      leaf_page_data->SetKeyAt(leaf_page_data->GetSize(), first_key);
      leaf_page_data->SetValueAt(leaf_page_data->GetSize(), first_value);
      // update parent key
      parent_page_data->SetKeyAt(index, sibling_leaf_data->KeyAt(0));
    } else {
      auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
      auto sibling_internal_data = reinterpret_cast<InternalPage *>(sibling_page_data);
      auto first_key = sibling_internal_data->KeyAt(1);
      auto first_value = sibling_internal_data->ValueAt(0);
      sibling_internal_data->PopFirst();
      // internal_page_data->Insert(parent_page_data->KeyAt(index), first_value, comparator_); 不使用插入因为有可能中间节点这时只有一个key
      internal_page_data->SetKeyAt(internal_page_data->GetSize(),parent_page_data->KeyAt(index));
      internal_page_data->SetValueAt(internal_page_data->GetSize(),first_value);
      internal_page_data->IncreaseSize(1);
      // update child
      auto child_page_id = first_value;
      auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
      auto child_page_data = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_page_data->SetParentPageId(internal_page_data->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page_id, true);
      // update parent key
      parent_page_data->SetKeyAt(index, first_key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_data->GetPageId(), false);  //?
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *sibling_page_data, BPlusTreePage *page_data, int index,
                           Transaction *transaction) {
  auto parent_page_id = page_data->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_page_data = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // sibling node <- node
  if (page_data->GetPageId() == parent_page_data->ValueAt(index)) {
    if (page_data->IsLeafPage()) {
      auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
      auto sibling_leaf_data = reinterpret_cast<LeafPage *>(sibling_page_data);
      for (int i = 0; i <= leaf_page_data->GetSize() - 1; i++) {
        sibling_leaf_data->SetKeyAt(sibling_leaf_data->GetSize() + i, leaf_page_data->KeyAt(i));
        sibling_leaf_data->SetValueAt(sibling_leaf_data->GetSize() + i, leaf_page_data->ValueAt(i));
        sibling_leaf_data->IncreaseSize(1);
      }
      leaf_page_data->IncreaseSize(-leaf_page_data->GetSize());
      sibling_leaf_data->SetNextPageId(leaf_page_data->GetNextPageId());
    } else {
      auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
      auto sibling_internal_data = reinterpret_cast<InternalPage *>(sibling_page_data);
      for (int i = 0; i <= internal_page_data->GetSize() - 1; i++) {
        if (i == 0) {
          sibling_internal_data->SetKeyAt(sibling_internal_data->GetSize() + i, parent_page_data->KeyAt(index));
        } else {
          sibling_internal_data->SetKeyAt(sibling_internal_data->GetSize() + i, internal_page_data->KeyAt(i));
        }
        sibling_internal_data->SetValueAt(sibling_internal_data->GetSize() + i, internal_page_data->ValueAt(i));
        // update child
        auto child_page_id = internal_page_data->ValueAt(i);
        auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
        if (child_page == nullptr) {
          throw std::runtime_error("valid page id do not exist");
        }
        auto child_page_data = reinterpret_cast<InternalPage *>(child_page->GetData());
        child_page_data->SetParentPageId(sibling_internal_data->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page_id, true);
      }
      sibling_internal_data->IncreaseSize(internal_page_data->GetSize());
      internal_page_data->IncreaseSize(-internal_page_data->GetSize());
    }
    // buffer_pool_manager_->DeletePage(page_data->GetPageId());
    transaction->AddIntoDeletedPageSet(page_data->GetPageId());
    DeleteKey(parent_page_data, parent_page_data->KeyAt(index), transaction);
  } else {  // node <- sibling node (just change sibling node and node)
    if (page_data->IsLeafPage()) {
      auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
      auto sibling_leaf_data = reinterpret_cast<LeafPage *>(sibling_page_data);
      for (int i = 0; i <= sibling_leaf_data->GetSize() - 1; i++) {
        leaf_page_data->SetKeyAt(leaf_page_data->GetSize() + i, sibling_leaf_data->KeyAt(i));
        leaf_page_data->SetValueAt(leaf_page_data->GetSize() + i, sibling_leaf_data->ValueAt(i));
      }
      leaf_page_data->IncreaseSize(sibling_leaf_data->GetSize());
      sibling_leaf_data->IncreaseSize(-sibling_leaf_data->GetSize());
      leaf_page_data->SetNextPageId(sibling_leaf_data->GetNextPageId());
    } else {
      auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
      auto sibling_internal_data = reinterpret_cast<InternalPage *>(sibling_page_data);
      for (int i = 0; i <= sibling_internal_data->GetSize() - 1; i++) {
        if (i == 0) {
          internal_page_data->SetKeyAt(internal_page_data->GetSize() + i, parent_page_data->KeyAt(index));
        } else {
          internal_page_data->SetKeyAt(internal_page_data->GetSize() + i, sibling_internal_data->KeyAt(i));
        }
        internal_page_data->SetValueAt(internal_page_data->GetSize() + i, sibling_internal_data->ValueAt(i));
        // update child
        auto child_page_id = sibling_internal_data->ValueAt(i);
        auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
        if (child_page == nullptr) {
          throw std::runtime_error("valid page id do not exist");
        }       
        auto child_page_data = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_page_data->SetParentPageId(internal_page_data->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page_id, true);
      }
      internal_page_data->IncreaseSize(sibling_internal_data->GetSize());
      sibling_internal_data->IncreaseSize(-sibling_internal_data->GetSize());
    }
    // buffer_pool_manager_->DeletePage(page_data->GetPageId());
    transaction->AddIntoDeletedPageSet(sibling_page_data->GetPageId());
    DeleteKey(parent_page_data, parent_page_data->KeyAt(index), transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_data->GetPageId(), true);
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  KeyType useless;
  auto find_page_data = FindLeafPage(useless, OpType::READ, nullptr, true);
  TryUnlockRootPageId(false);
  return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(find_page_data), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  auto leaf_page = FindLeafPage(key, OpType::READ, nullptr);
  TryUnlockRootPageId(false);
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(leaf_page_data, leaf_page_data->KeyIndex(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  bool exclusive = false;
  LockRootPageId(exclusive);
  auto find_page_id = root_page_id_;
  auto new_page_id = INVALID_PAGE_ID;
  auto find_page = CrabingProtocalFetchPage(find_page_id, OpType::READ, -1, nullptr);
  if (find_page == nullptr) {
    TryUnlockRootPageId(exclusive);
    return INDEXITERATOR_TYPE();
  }
  auto find_page_data = reinterpret_cast<BPlusTreePage *>(find_page->GetData());
  while (!find_page_data->IsLeafPage()) {
    auto internal_page_data = reinterpret_cast<InternalPage *>(find_page_data);
    new_page_id = internal_page_data->ValueAt(internal_page_data->GetSize() - 1);
    // buffer_pool_manager_->UnpinPage(find_page_id, false);
    find_page = CrabingProtocalFetchPage(new_page_id, OpType::READ, find_page_id, nullptr);
    find_page_id = new_page_id;
    if (find_page == nullptr) {
      throw std::exception();
    }
    find_page_data = reinterpret_cast<BPlusTreePage *>(find_page->GetData());
  }
  return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(find_page_data), find_page_data->GetSize(),
                            buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
