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


#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"


namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
  //  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
  //KeyType key{};
  //return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {

  return array_[index].second;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(KeyComparator comparator, const KeyType &key) {
  int i=-1;
  for(i=0; i<GetSize(); i++){
    if(comparator(array_[i].first, key) == 0){

      break;
    }
  }

  std::cout << "=====i: " << i <<", "<< GetSize() << ", " << array_[0].first << std::endl;
  if(i != -1 && i != GetSize()){
    for(i=i+1; i<GetSize(); i++){
      array_[i-1] =array_[i];
    }

    SetSize(GetSize()-1);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(KeyComparator comparator, const KeyType& key, const ValueType& value){
 // auto key_schema = ParseCreateStatement("a bigint");
  //KeyComparator comparator(key_schema.get());
  std::cout << "insert key: " << key << ", value: " << value  << ", size: " << GetSize() << std::endl;

  for(int k=0; k<GetSize(); k++){
    if(comparator(array_[k].first, key) == 0){
      return ;
    }
  }
  int i;
  for(i=GetSize(); i>=1; i--){
    if(comparator(array_[i-1].first, key) == 1){
      array_[i].first = array_[i-1].first;
      array_[i].second = array_[i-1].second;
    }

    if(comparator(array_[i-1].first, key) == 0){
      break ;
    }
    if(comparator(array_[i-1].first, key) == -1){
      array_[i].first = key;
      array_[i].second = value;
      SetSize(GetSize()+1);
      break ;
    }
  }

  //如果key小于array_中所有的元素
  if(i == 0){
    array_[0].first = key;
    array_[0].second = value;
    SetSize(GetSize()+1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index,const ValueType& value) {
  array_[index].second = value;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
