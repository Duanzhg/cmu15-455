#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <iostream>
#include <stack>
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
 //throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

 std::cout << "get key: " << key << std::endl;
 auto temp = root_;
 for(size_t i = 0; i<key.size(); i++){
   if(temp->children_.find(key[i]) == temp->children_.end()){
      return nullptr;
   }

   temp = temp->children_[key[i]];
 }

 auto p_value = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(temp);

 if(p_value == nullptr){
   return nullptr;
 }

 return p_value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
//  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  /**
   * 创建一个新的根节点，其值为旧根节点的一份拷贝。然后遍历key，若其字符已经在树中，
   * 则将对应节点的一份拷贝加入到新树中，若发现一个字符不在树中，则说明明树中目前没有
   * 这个key，则需要将这个key加入到树中，停止遍历。若成功遍历完key，则说明树中已经包含了
   * 这个key，则需要再创建一个TrieNodeWithValue节点，并插入到树中。
   *
   */

  std::cout << "put key: " << key << std::endl;
  std::shared_ptr<const TrieNode> new_root(std::move(root_->Clone()));
  //std::shared_ptr<const TrieNode> ne_root(std::move(root_->Clone()));
  auto new_temp = new_root;
  //auto pre_new_temp = new_temp;
  std::shared_ptr<const TrieNode> pre_new_temp = nullptr;

  auto temp = root_;
  bool is_exist = true;

  size_t i = 0;
  for(i=0; i<key.size(); i++){
    if(temp->children_.find(key[i]) == temp->children_.end()){
        is_exist = false;
        break;
    }

    pre_new_temp = new_temp;
    auto new_node = std::shared_ptr<const TrieNode>(temp->children_[key[i]]->Clone());
    new_temp->children_[key[i]] = new_node;
    new_temp = new_node;
    temp = temp->children_[key[i]];
  }

  if(is_exist){
    auto new_node = std::make_shared<const TrieNodeWithValue<T>>(new_temp->children_, std::make_shared<T>(std::move(value)));
    if(key.size() == 0){
        new_root = new_node;
    }else{
        pre_new_temp->children_[key[i-1]] = new_node;
    }
  }

  if(!is_exist){
    for(; i<key.size()-1; i++){
        auto new_node = std::make_shared<const TrieNode>();
        new_temp->children_[key[i]] = new_node;
        new_temp = new_node;
    }

    auto new_node = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    new_temp->children_[key[i]] = new_node;
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
 // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::cout << "remove key: " << key<< std::endl;
  auto new_root = std::shared_ptr<const TrieNode>(std::move(root_->Clone()));
  std::shared_ptr<const TrieNode> new_temp = new_root;
  auto temp = root_;
  std::stack<std::shared_ptr<const TrieNode>> node_stack;
  node_stack.push(nullptr);
  size_t i;
  for(i=0; i<key.size(); i++){
    if(temp->children_.find(key[i]) == temp->children_.end()){
        return Trie(new_root);
    }

    node_stack.push(new_temp);
    auto new_node = std::shared_ptr<const TrieNode>(std::move(temp->children_[key[i]]->Clone()));
    new_temp->children_[key[i]] = new_node;
    new_temp = new_node;
    temp = temp->children_[key[i]];
  }

  if(new_temp->is_value_node_){
    std::cout << key << std::endl;
    while(i>0){
        auto pre_new_temp = node_stack.top();
        node_stack.pop();

        if(new_temp->children_.size() != 0){
          auto new_node = std::make_shared<const TrieNode>(new_temp->children_);
          if(pre_new_temp == nullptr){
            new_root = new_node;
          }else{
            pre_new_temp->children_[key[i-1]] = new_node;
          }
          break;
        }

        pre_new_temp->children_.erase(key[i-1]);


        if(pre_new_temp->is_value_node_){
          break;
        }
        new_temp = pre_new_temp;
        i--;
    }


  }

  return Trie(new_root);

}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.


using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;


}  // namespace bustub
