/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(bool is_end): is_end_(is_end) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, const page_id_t page_id, const int index):bpm_(bpm), page_id_(page_id),index_(index), is_end_(false){
    guard_ = bpm->FetchPageRead(page_id);
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    //throw std::runtime_error("unimplemented");
    return is_end_ == true;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    //throw std::runtime_error("unimplemented");
    const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    value_.first = page->KeyAt(index_);
    value_.second = page->ValueAt(index_);

    return value_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    //throw std::runtime_error("unimplemented");
    const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

    if(page->GetSize()-1>index_){
        index_++;
    }else{
        int next_page_index = page->GetNextPageId();
        if(next_page_index == -1){
            is_end_ = true;
        }else{
            guard_ = bpm_->FetchPageRead(next_page_index);
            index_ = 0;
        }
    }

    return *this;

}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
