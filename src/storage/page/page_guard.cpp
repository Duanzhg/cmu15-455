#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {

  Drop();

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if(bpm_ != nullptr && page_ != nullptr){
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
    is_dirty_ = false;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
 // std::cout << "******" << std::endl;
  //如果that等于this

  /*
  if(page_ == that.page_){
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

    is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
  }else{
   if(page_ != nullptr){
     bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
   }
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
  }
*/
  if(this == &that){
    return *this;
  }
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard(){
  /*
    if(bpm_ == nullptr) {
      std::cout << "!!!!!" << std::endl;
    }

    */
  /*
    if( bpm_ != nullptr && page_ != nullptr){

      bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
      page_ = nullptr;
    }
    */
    Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_))  {
    //  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {

    guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
  if(guard_.page_ != nullptr){
      //guard_.page_->RUnlatch();
  }
}

ReadPageGuard::~ReadPageGuard() {
  if(guard_.page_ != nullptr){
     // guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  //  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
//  std::cout << "******" << std::endl;
  guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    if(guard_.page_ != nullptr){
      //guard_.page_->WUnlatch();
    }
}

WritePageGuard::~WritePageGuard() {
    if(guard_.page_ != nullptr){
      //guard_.page_->WUnlatch();
    }
}  // NOLINT
}  // namespace bustub
