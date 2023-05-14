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

#include "common/exception.h"
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
  //KeyType key{};
  //return key;
    return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
    array_[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
    //return 0;
    return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndex(KeyComparator &comparator, const KeyType &key) const -> int {
    int left = 1;
    int right = GetSize();
    int larger_flag = 0;
    int smaller_flag = 0;
    int start_index = -1;

    while(left < right){
        int middle = left + (right - left)/2;
        if(comparator(array_[middle].first, key) == 0){
            return middle;
        }else if(comparator(array_[middle].first, key) == 1){

            if(smaller_flag == 1){
                start_index = left;
                break ;
            }
            larger_flag = 1;
            right = middle;
        }else if(comparator(array_[middle].first, key) == -1){

            if(larger_flag == 1){
                start_index = middle+1;
                break;
            }
            smaller_flag = 1;
            left = middle+1;
        }
    }

    if(start_index == -1 && smaller_flag == 1){
        return GetSize()-1;
    }else if(start_index == -1 && larger_flag == 1){
        return 0;
    }else{
        for(int i=start_index; i< GetSize(); i++){
            if(comparator(array_[i].first, key) == 0){
                return i;
            }else if(comparator(array_[i].first, key) == 1){
                return i-1;
            }
        }
    }

    return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyComparator &comparator, const KeyType &key, const ValueType &value) {



    int i;
    for(i=GetSize()-1; i>=1; i--){
        if(comparator(array_[i].first, key) == 1){
            array_[i+1] = array_[i];
        }else if(comparator(array_[i].first, key) == -1){
            array_[i+1].first = key;
            array_[i+1].second = value;
            break ;
        }
    }

    if(i == 0){
        array_[1].first = key;
        array_[1].second = value;
    }

    SetSize(GetSize()+1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(KeyComparator &comparator, const KeyType &key) {
    int left = 1;
    int right = GetSize();

    int index = -1;
    while(left < right){
        int middle = left + (right - left)/2;
        if(comparator(array_[middle].first, key) == 0){
            index = middle;
            break;
        }else if(comparator(array_[middle].first, key) == 1){
            right = middle;
        }else{
            left = middle+1;
        }
    }

    if(index == -1){
        return;
    }

    for(int i=index; i<GetSize()-1; i++){
        array_[i] = array_[i+1];
    }

    SetSize(GetSize()-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertIntoHead(const KeyType key, const ValueType &value) {
    for(int i=GetSize()-1; i>=0; i--){
        array_[i+1] = array_[i];
    }

    array_[0].first = key;
    array_[0].second = value;

    SetSize(GetSize()+1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteHead() {
    for(int i=0; i<GetSize()-1; i++){
        array_[i] = array_[i+1];
    }

    SetSize(GetSize()-1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
