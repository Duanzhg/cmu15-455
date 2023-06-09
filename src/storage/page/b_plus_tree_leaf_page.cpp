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
    SetNextPageId(-1);
    SetMaxSize(max_size);

}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {

    return next_page_id_;

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

  //KeyType key{};
  //return key;
    return array_[index].first;

}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
    return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType& key) {
    array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
    array_[index].second = value;
}

/*
 * 使用二分查找查找array_中键为key的元素对应的下标，若不存在，返回-1
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndex(KeyComparator &comparator, const KeyType &key) const -> int {
    int left = 0;
    int right = GetSize();

    while(left < right){
      int middle = left + (right - left)/2;
      if(comparator(array_[middle].first, key) == 0){
          return middle;
      }else if(comparator(array_[middle].first, key) == 1){
          right = middle;
      }else{
          left = middle+1;
      }
    }
    return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(KeyComparator &comparator, const KeyType &key, const ValueType &value) {
    //array_中已经存在键为key的元素
    if(GetIndex(comparator, key) != -1){
        return;
    }

    int i;
    for(i=GetSize()-1; i>=0; i--){
        if(comparator(array_[i].first, key) == 1){
            array_[i+1] = array_[i];
        }else if(comparator(array_[i].first, key) == -1){
            array_[i+1].first = key;
            array_[i+1].second = value;
            break ;
        }
    }

    if(i == -1){
        array_[0].first = key;
        array_[0].second = value;
    }

    SetSize(GetSize()+1);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(KeyComparator &comparator, const KeyType &key) {
    int index = GetIndex(comparator,key);
    if(index == -1){
        return;
    }


    for(int i=index; i<GetSize()-1; i++){
        array_[i] = array_[i+1];
    }

    SetSize(GetSize()-1);
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
