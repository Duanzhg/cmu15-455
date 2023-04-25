//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  /*
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");
*/
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetPageByFrameId(bustub::frame_id_t frameId) -> Page * {
  page_id_t page_id = -1;
  Page* page = nullptr;

  //查找frameId对应的pageId
  for(auto iter=page_table_.begin(); iter != page_table_.end(); iter++){

    if(iter->second == frameId){
      page_id = iter->first;
      break;
    }
  }

  //查找pageId对应的Page
  for(size_t i=0; i<pool_size_; i++){
    if(pages_[i].page_id_ == page_id){
      page = &pages_[i];
      break ;
    }
  }
  return page;
}

auto BufferPoolManager::GetPageFromFreeList() -> Page * {
  Page* page = nullptr;
  for(size_t i=0; i<pool_size_; i++){
    if(pages_[i].page_id_ == INVALID_PAGE_ID){
      page = &pages_[i];
      break ;
    }
  }
  return page;
}

/*
auto BufferPoolManager::GetPageId(bustub::frame_id_t frameId) ->page_id_t {
  page_id_t  page_id = -1;
  for(auto iter = page_table_.begin(); iter != page_table_.end(); iter++){
    if(iter->second == frameId){
      page_id = iter->first;
      break ;
    }
  }
  return page_id;
}
 */

auto BufferPoolManager::GetPageByPageId(bustub::page_id_t pageId) -> Page * {
  Page* page = nullptr;
  for(size_t i=0; i<pool_size_; i++){
    if(pages_[i].page_id_ == pageId){
      page = &pages_[i];
      break ;
    }
  }
  return page;
}
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "create New page..." << std::endl;
  Page* page = nullptr;
  frame_id_t  frame_id = -1;
  //先查找空闲列表
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = GetPageFromFreeList();
  }

  //若空闲列表为空，再查询buffer pool中evictable的帧
  if(frame_id == -1 && replacer_->Evict(&frame_id)){
    page = GetPageByFrameId(frame_id);
  //  page_id_t old_page_id = GetPageId(frame_id);
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
    replacer_->Remove(frame_id);
    page_table_.erase(page->page_id_);
  }

  if(frame_id == -1){
    std::cout << "Fail to create new page" << std::endl;
    return nullptr;
  }

  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
 // page->data_ = nullptr;

  replacer_->RecordAccess(frame_id);
  std::cout << "Success to create a new page " << *page_id << std::endl;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "Fetch page " << page_id << std::endl;
  Page* page = nullptr;
  frame_id_t frame_id = -1;
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = GetPageFromFreeList();
  }

  if(frame_id == -1 && replacer_->Evict(&frame_id)){
    page = GetPageByFrameId(frame_id);
 //   page_id_t old_page_id = GetPageId(frame_id);
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
    replacer_->Remove(frame_id);
    page_table_.erase(page->page_id_);
  }

  if(frame_id == -1){
    std::cout << "Fail to fetch page " << page_id << std::endl;
    return nullptr;
  }

  page_table_[page_id] = frame_id;
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, page->data_);
  page->pin_count_++;

  replacer_->RecordAccess(frame_id);
  std::cout << "success to fetch page " << page_id << std::endl;
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "Unpin page " << page_id << std::endl;
  if(page_table_.find(page_id) == page_table_.end()){
    std::cout << "Page " << page_id << " is not in buffer pool" << std::endl;
    return false;
  }

  Page* page = GetPageByPageId(page_id);
  if(page->pin_count_ == 0){
    std::cout << "pin count of page" << page_id << " is 0" << std::endl;
    return false;
  }

  page->pin_count_--;
  if(page->pin_count_ == 0){
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  std::cout << "Success to unpin page "<< page_id << std::endl;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "Flush page "<< page_id << std::endl;
  if(page_table_.find(page_id) == page_table_.end()){
    std::cout << "page " << page_id << "is not in buffer pool" << std::endl;
    return false;
  }

  Page* page = GetPageByPageId(page_id);
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;

  std::cout << "Success to flush page " << page_id << std::endl;
  return true;

}

void BufferPoolManager::FlushAllPages() {
    std::unique_lock<std::mutex> lock(latch_);
    std::cout << "Flush all page" << std::endl;
    for(size_t i=0; i<pool_size_; i++){
      if(pages_[i].page_id_ == INVALID_PAGE_ID){
        continue ;
      }
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
    std::cout << "Success to flush all page" << std::endl;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "Delete page " << page_id << std::endl;
  if(page_table_.find(page_id) == page_table_.end()){
      std::cout << "page " << page_id << "is not in buffer pool" << std::endl;
      return false;
  }

  Page* page = GetPageByPageId(page_id);
  if(page->pin_count_ > 0){
      std::cout << "pin count of page " << page_id << " is lager than 0" << std::endl;
      return false;
  }

  replacer_->Remove(page_table_[page_id]);
  page->page_id_ = INVALID_PAGE_ID;
  page_table_.erase(page->page_id_);
  std::cout << "Success to delete page " << page_id << std::endl;
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {

  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {


  return {this, NewPage(page_id)};
}

}  // namespace bustub
