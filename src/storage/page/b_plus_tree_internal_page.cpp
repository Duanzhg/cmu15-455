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

//#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"


namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetMaxSize(max_size);
}


/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // KeyType key{};
  //return key;

    return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
    return array_[index].second;
    //return 0;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyComparator comparator, const KeyType &key, const ValueType &value) {
    //auto key_schema = ParseCreateStatement("a bigint");
    //KeyComparator comparator(key_schema.get());



    for(int k=1; k<GetSize(); k++){
      if(comparator(array_[k].first, key) == 0){
        throw Exception(".....");
        return;
      }
    }

    int i;
    for(i=GetSize(); i>=2; i--){
      if(comparator(array_[i-1].first, key) == 1){
        array_[i].first = array_[i-1].first;
        array_[i].second = array_[i-1].second;
      }

      if(comparator(array_[i-1].first, key) == 0){
        break ;
      }
      if (comparator(array_[i-1].first, key) == -1){
        array_[i].first = key;
        array_[i].second = value;
        SetSize(GetSize()+1);
        break ;
      }
    }

    if(i == 1){
      array_[i].first = key;
      array_[i].second = value;
      SetSize(GetSize()+1);
    }


}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(KeyComparator comparator, const KeyType& key) {


    int i=-1;
    for( i=1; i<GetSize(); i++){
      if(comparator(array_[i].first, key) == 0){
        break ;
      }
    }

    /*
    int i = -1;
    for(i=1; i<GetSize(); i++){
      if(array_[i].second == value){
        break ;
      }
    }
*/

    std::cout << "#i: " << i << std::endl;
    if(i != -1 && i != GetSize()){
      for(i=i+1; i<GetSize(); i++){
        array_[i-1] = array_[i];
        std::cout << array_[i].first << std::endl;
      }
      SetSize(GetSize()-1);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst() {
    for(int i=1; i<GetSize(); i++){
      array_[i-1] = array_[i];
    }
    SetSize((GetSize()-1));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType& key, const ValueType& value) {
    for(int i=GetSize(); i>=1; i--){
      std::cout << "insert first key: " << array_[i-1].first << std::endl;
      array_[i] = array_[i-1];
    }

    array_[0].first = key;
    array_[0].second = value;
    SetSize((GetSize()+1));
    std::cout << GetSize() << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
    array_[index].second = value;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
