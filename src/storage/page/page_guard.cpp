#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if(page_ != nullptr){
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  //如果that等于this
  if(page_ == that.page_){
    bpm_->UnpinPage(page_->GetPageId(), that.is_dirty_);
    that.page_ = nullptr;
  }else{
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard(){
    if(page_ != nullptr){
      bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
      page_ = nullptr;
    }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
    guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
  if(guard_.page_ != nullptr){
      guard_.page_->RUnlatch();
  }
}

ReadPageGuard::~ReadPageGuard() {
  if(guard_.page_ != nullptr){
      guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept{
    guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    if(guard_.page_ != nullptr){
      guard_.page_->WUnlatch();
    }
}

WritePageGuard::~WritePageGuard() {
    if(guard_.page_ != nullptr){
      guard_.page_->WUnlatch();
    }
}  // NOLINT
}  // namespace bustub
