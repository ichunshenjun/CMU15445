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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }
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
  auto leaf_page = FindLeafPage(key, transaction);
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType value;
  if(leaf_page_data->ReturnValue(key, &value, comparator_)){
    result->push_back(value);
    buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false);
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
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (root_page == nullptr) {
      throw std::exception();
    }
    UpdateRootPageId(0);
    auto new_root = reinterpret_cast<LeafPage *>(root_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    new_root->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto leaf_page = FindLeafPage(key, transaction);
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (leaf_page_data->FindKey(key, comparator_)) {
    return false;
  }
  bool res = leaf_page_data->Insert(key, value, comparator_);
  if (leaf_page_data->GetSize() == leaf_page_data->GetMaxSize()) {
    auto new_leaf_page_data = LeafPageSplit(leaf_page_data);
    auto first_key = new_leaf_page_data->KeyAt(0);
    InsertIntoParent(leaf_page_data, first_key, new_leaf_page_data);
    buffer_pool_manager_->UnpinPage(new_leaf_page_data->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), true);
  return res;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction) -> Page * {
  auto find_page_id = root_page_id_;
  auto find_page = buffer_pool_manager_->FetchPage(find_page_id);
  if (find_page == nullptr) {
    throw std::exception();
  }
  auto find_page_data = reinterpret_cast<BPlusTreePage *>(find_page->GetData());
  while (!find_page_data->IsLeafPage()) {
    auto internal_page_data = reinterpret_cast<InternalPage *>(find_page_data);
    auto new_page_id = internal_page_data->FindKey(key, comparator_);
    buffer_pool_manager_->UnpinPage(find_page_id, false);
    find_page_id = new_page_id;
    find_page = buffer_pool_manager_->FetchPage(find_page_id);
    if (find_page == nullptr) {
      throw std::exception();
    }
    find_page_data = reinterpret_cast<BPlusTreePage *>(find_page->GetData());
  }
  return find_page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafPageSplit(LeafPage *page_data) -> LeafPage * {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::exception();
  }
  auto temp_page_data = reinterpret_cast<LeafPage *>(new_page->GetData());  // 还要使temp_page_data转为另外两种类型
  temp_page_data->SetPageType(page_data->GetPageType());
  if (temp_page_data->IsLeafPage()) {
    auto leaf_page_data = reinterpret_cast<LeafPage *>(page_data);
    auto new_page_data = reinterpret_cast<LeafPage *>(temp_page_data);
    new_page_data->Init(new_page_id, leaf_page_data->GetParentPageId(), leaf_page_data->GetMaxSize());
    new_page_data->Move(leaf_page_data);
    new_page_data->SetNextPageId(leaf_page_data->GetNextPageId());
    leaf_page_data->SetNextPageId(new_page_data->GetPageId());
  }
  return temp_page_data;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalPageSplit(InternalPage *page_data, const KeyType &key, const page_id_t &value)
    -> InternalPage * {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::exception();
  }
  auto temp_page_data = reinterpret_cast<InternalPage *>(new_page->GetData());
  temp_page_data->SetPageType(page_data->GetPageType());
  if (!temp_page_data->IsLeafPage()) {
    auto internal_page_data = reinterpret_cast<InternalPage *>(page_data);
    auto new_page_data = reinterpret_cast<InternalPage *>(temp_page_data);
    std::map<KeyType, page_id_t, KeyComparator> temp;
    for (int i = 0; i < internal_page_data->GetSize(); i++) {
      temp[internal_page_data->KeyAt(i)] = internal_page_data->ValueAt(i);
    }
    temp[key] = value;
    new_page_data->Init(new_page_id, internal_page_data->GetParentPageId(), internal_page_data->GetMaxSize());
    int count = 0;
    auto minsize = internal_page_data->GetMinSize();
    auto maxsize = internal_page_data->GetMaxSize();
    for (auto iter = temp.begin(); iter != temp.end(); iter++) {
      if (count >= minsize && count < maxsize) {
        new_page_data->SetKeyAt(count - minsize, internal_page_data->KeyAt(count));
        new_page_data->SetValueAt(count - minsize, internal_page_data->ValueAt(count));
      }
      count++;
    }
    internal_page_data->IncreaseSize(minsize - maxsize);
    new_page_data->IncreaseSize(minsize);
  }
  return temp_page_data;
}
INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
void BPLUSTREE_TYPE::InsertIntoParent(ClassType *page_data, KeyType key, ClassType *new_leaf_page_data) {
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
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  auto parent_page_id = page_data->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_page_data = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_page_data->GetSize() < parent_page_data->GetMaxSize()) {
    parent_page_data->Insert(key, new_leaf_page_data->GetPageId(), comparator_);
    return;
  }
  // parent_page_data->Insert(key,new_leaf_page_data->ValueAt(0),comparator_);
  auto new_parent_page_data = InternalPageSplit(parent_page_data, key, new_leaf_page_data->GetPageId());
  auto parent_first_key = new_parent_page_data->KeyAt(0);
  InsertIntoParent(parent_page_data, parent_first_key, new_parent_page_data);
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
//   auto leaf_page=FindLeafPage(key, transaction);
//   auto leaf_page_data=reinterpret_cast<LeafPage*>(leaf_page->GetData());
//   if(!leaf_page_data->FindKey(key,comparator_)){
//     buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false);
//     return;
//   }
//   DeleteKey(leaf_page_data,key,transaction);
//   buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), true);
// }
// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::DeleteKey(BPlusTreePage* page_data,const KeyType &key, Transaction *transaction) {
//   if(page_data->IsLeafPage()){
//     auto leaf_page_data=reinterpret_cast<LeafPage*>(page_data);
//     leaf_page_data->Remove(key,comparator_);
//   }else{
//     auto internal_page_data=reinterpret_cast<InternalPage*>(page_data);
//     internal_page_data->Remove(key,comparator_);
//   }
//   if(page_data->IsRootPage()&&page_data->GetSize()==1&&!page_data->IsLeafPage()){
//     auto root_page_data=reinterpret_cast<InternalPage*>(page_data);
//     auto root_page_id=root_page_data->GetPageId();
//     auto new_root_page_id=root_page_data->ValueAt(0);
//     auto new_root_page=buffer_pool_manager_->FetchPage(new_root_page_id);
//     assert(new_root_page);
//     auto new_root_page_data=reinterpret_cast<BPlusTreePage*>(new_root_page->GetData());
//     new_root_page_data->SetParentPageId(INVALID_PAGE_ID);
//     UpdateRootPageId(1);
//     buffer_pool_manager_->UnpinPage(new_root_page_id, true);
//     buffer_pool_manager_->DeletePage(root_page_id);
//     return;
//   }
//   if(!page_data->IsRootPage()&&page_data->GetSize()<page_data->GetMinSize()){
//     auto parent_page=buffer_pool_manager_->FetchPage(page_data->GetParentPageId());
//     assert(parent_page);
//     auto parent_page_data=reinterpret_cast<InternalPage*>(parent_page->GetData());
//     auto left_page_id=parent_page_data->GetLeftPage(page_data->GetPageId());
//     if(left_page_id!=INVALID_PAGE_ID){
//       auto left_page=buffer_pool_manager_->FetchPage(left_page_id);
//       auto left_page_data=reinterpret_cast<BPlusTreePage*>(left_page->GetData());
//       if(left_page_data->GetSize()+page_data->GetSize()>=page_data->GetMaxSize()){
//         Borrow();
//       }
//     }
//   }
}
// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::Borrow(const KeyType &key, Transaction *transaction){

// } 
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

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
