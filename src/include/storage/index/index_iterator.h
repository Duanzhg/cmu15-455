//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT

  IndexIterator(BufferPoolManager* bpm, const page_id_t page_id, const int index);
  IndexIterator(bool is_end);
  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto is_equal(const IndexIterator &itr) ->bool {
    if(bpm_ != nullptr && itr.bpm_ != nullptr){
      if(page_id_ == itr.page_id_ && index_ == itr.index_){
        return true;
      }
    }

    if(is_end_ && itr.is_end_){
      return true;
    }
    return false;
  }

  auto operator==(const IndexIterator &itr) const -> bool {
  //  throw std::runtime_error("unimplemented");
    //return is_equal(itr);
    if(bpm_ != nullptr && itr.bpm_ != nullptr){
      if(page_id_ == itr.page_id_ && index_ == itr.index_){
        return true;
      }
    }

    if(is_end_ && itr.is_end_){
      return true;
    }

    return false;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    //throw std::runtime_error("unimplemented");
    //std::cout << is_end_ << " " << itr.is_end_ << std::endl;
    return !(*this == itr);
  }

 private:
  // add your own private member variables here
  BufferPoolManager* bpm_;
  ReadPageGuard guard_;
  page_id_t page_id_;
  int index_;
  MappingType value_;
  bool is_end_;
};

}  // namespace bustub
