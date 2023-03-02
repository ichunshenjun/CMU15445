//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/rwlatch.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;
  auto InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool;
  auto FindLeafPage(const KeyType &key, OpType op, Transaction *transaction=nullptr,bool mostLeft=false) -> Page *;
  auto CrabingProtocalFetchPage(page_id_t page_id, OpType op, page_id_t previous, Transaction *transaction) -> Page *;
  void FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t previous);
  // auto Split(ClassType *page_data,const KeyType &key,const ValueType &value) -> ClassType*;
  template <typename ClassType>
  auto Split(ClassType *page_data, Transaction *transaction) -> ClassType *;
  template <typename ClassType>
  void InsertIntoParent(ClassType *page_data, KeyType key, ClassType *new_leaf_page_data, Transaction *transaction);
  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);
  void DeleteKey(BPlusTreePage *page_data, const KeyType &key, Transaction *transaction);
  void Borrow(BPlusTreePage *sibling_page_data, BPlusTreePage *page_data, int index);
  void Merge(BPlusTreePage *sibling_page_data, BPlusTreePage *page_data, int index, Transaction *transaction);
  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  inline void Lock(bool exclusive, Page *page) {
    if (exclusive) {
      page->WLatch();
    } else {
      page->RLatch();
    }
  }

  inline void Unlock(bool exclusive, Page *page) {
    if (exclusive) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
  }

  inline void Unlock(bool exclusive, page_id_t page_id) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    Unlock(exclusive, page);
    buffer_pool_manager_->UnpinPage(page_id, exclusive);
  }

  inline void LockRootPageId(bool exclusive) {
    if (exclusive) {
      mutex_.WLock();
    } else {
      mutex_.RLock();
    }
    root_locked_cnt++;
  }

  inline void TryUnlockRootPageId(bool exclusive) {
    if (root_locked_cnt > 0) {
      if (exclusive) {
        mutex_.WUnlock();
      } else {
        mutex_.RUnlock();
      }
      root_locked_cnt--;
    }
  }

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  ReaderWriterLatch mutex_;
  static thread_local int root_locked_cnt;
};

}  // namespace bustub
