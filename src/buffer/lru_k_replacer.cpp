//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {

  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "evict..." <<  std::endl;

  bool is_inf = false;
  size_t max_k_distance = UINT64_MAX;
  *frame_id = -1;


  //使用lruk算法淘汰一个节点
  for(auto iter = node_store_.begin(); iter != node_store_.end(); iter++){
    if(!iter->second.is_evictable_){

      continue ;
    }

    if(!is_inf && iter->second.history_.size() < k_){
        is_inf = true;
      }

      if(is_inf && iter->second.history_.size() >=k_){
        continue ;
      }

      if(max_k_distance > iter->second.history_.front()){
          max_k_distance = iter->second.history_.front();
          *frame_id = iter->second.fid_;
      }
  }

  if(*frame_id == -1){
      std::cout << "failed to evict a frame..." << std::endl;
      return false;
  }

  //移除frame_id;
  node_store_.erase(*frame_id);
  curr_size_--;
  std::cout << "success to  evict frame " << *frame_id << std::endl;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "access frame " << frame_id <<" at timestamp " << current_timestamp_ << std::endl;
  if(frame_id > frame_id_t (replacer_size_)){
    throw Exception(fmt::format("frame_id[{}] is invalid", frame_id));
  }

  //如果frame_id不存在, 添加到node_store中
  if(node_store_.find(frame_id) == node_store_.end()){
    auto node = LRUKNode();
    node.fid_ = frame_id;
    node.k_ = k_;
    node.is_evictable_ = false;
    node_store_[frame_id] = node ;

  }
  node_store_[frame_id].is_evictable_ = false;
  node_store_[frame_id].history_.push_front(current_timestamp_);
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "set frame " << frame_id << " to " << set_evictable << std::endl;
  if(frame_id > (frame_id_t)replacer_size_){
    throw Exception(fmt::format("frame_id[{}] is invalid", frame_id));
  }

  //如果该frame是evictable, 但是set_evictable值为fasle,则需要将该frame设置为non-evictable
  if(node_store_[frame_id].is_evictable_ && !set_evictable){
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_--;
  }

  //如果该frame是not-evictable，但是set-evictable值为false，则需要将该frame设置为non-evictable
  if(!node_store_[frame_id].is_evictable_ && set_evictable){
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "remove frame " << frame_id << std::endl;
  if(frame_id > (frame_id_t)replacer_size_){
    throw Exception(fmt::format("frame_id[{}] is invalid", frame_id));
  }

  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
