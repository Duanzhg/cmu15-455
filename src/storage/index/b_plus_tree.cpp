#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include <iostream>
namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();


  return header_page->root_page_id_ == INVALID_PAGE_ID;
 // return true;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  std::cout << "get value of key: " << key << std::endl;
  page_id_t root_page_id = GetRootPageId();
  auto curr_guard = bpm_->FetchPageBasic(root_page_id);
  auto curr_page = curr_guard.As<BPlusTreePage>();

  while(!curr_page->IsLeafPage()){
    const InternalPage * temp_page = curr_guard.As<InternalPage >();
    int i;
    for(i=1; i<temp_page->GetSize(); i++){
      if(comparator_(temp_page->KeyAt(i), key) == 1){
        curr_guard = bpm_->FetchPageBasic(temp_page->ValueAt(i-1));
        curr_page = curr_guard.As<BPlusTreePage>();
        break ;
      }

      if(comparator_(temp_page->KeyAt(i), key) == 0){
        curr_guard = bpm_->FetchPageBasic(temp_page->ValueAt(i));
        curr_page = curr_guard.As<BPlusTreePage>();
        break ;
      }
    }

    //temp_page中所有的元素的key都小于要查询的key
    if(temp_page->GetSize() == i){
      curr_guard = bpm_->FetchPageBasic(temp_page->ValueAt(i-1));
      curr_page = curr_guard.As<BPlusTreePage>();
    }
  }

  //找到叶节点后，遍历找到key对应的元素
  const LeafPage* leaf_page = curr_guard.As<LeafPage>();
  for(int i=0; i<leaf_page->GetSize(); i++){
    if(comparator_(leaf_page->KeyAt(i), key) == 0 ){
      result->push_back(leaf_page->ValueAt(i));
      std::cout << "success get value: " << result->back() << std::endl;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
 std::cout << "insert key " << key << ", value: " << value << std::endl;

  // Declaration of context instance.
  Context ctx;
  //(void)ctx;

  //树为空，创建一个叶子节点作为根节点
  if(IsEmpty()){
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

    //可能发送竞争？
    auto root_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    std::cout << "root_page_id: " << header_page->root_page_id_ << std::endl;
    LeafPage *root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->SetKeyAt(0, key);
    root_page->SetValueAt(0, value);
    root_page->SetSize(1);
   // root_page->Insert(comparator_, key, value);
    root_page->SetNextPageId(-1);
    return true;

  }

  page_id_t root_page_id = GetRootPageId();
  auto curr_guard = bpm_->FetchPageWrite(root_page_id);
  BPlusTreePage* curr_page = curr_guard.AsMut<BPlusTreePage>();

  while(!curr_page->IsLeafPage()){
    InternalPage *temp_page = curr_guard.AsMut<InternalPage>();
    //记录curr_guard;
    ctx.write_set_.push_back(std::move(curr_guard));
    int i;
    for(i=1; i<temp_page->GetSize(); i++){
      if(comparator_(temp_page->KeyAt(i), key) == 1){
        curr_guard = bpm_->FetchPageWrite(temp_page->ValueAt(i-1));
        curr_page = curr_guard.AsMut<BPlusTreePage>();
        break ;
      }

      if(comparator_(temp_page->KeyAt(i), key) == 0){
        curr_guard = bpm_->FetchPageWrite(temp_page->ValueAt(i));
        curr_page = curr_guard.AsMut<BPlusTreePage>();
        break ;
      }
    }

    if(temp_page->GetSize() == i){
      curr_guard = bpm_->FetchPageWrite(temp_page->ValueAt(i-1));
      curr_page = curr_guard.AsMut<BPlusTreePage>();
    }
  }



  //将值插入叶节点
  LeafPage* leaf_page = curr_guard.AsMut<LeafPage>();
  std::cout << "size of leaf: " << leaf_page->GetSize() << std::endl;
  if(leaf_page->GetSize() < leaf_max_size_){
    leaf_page->Insert(comparator_,key, value);
  }else{
    MappingType* temp_array_ = new MappingType[leaf_max_size_+1];
    for(int i=0; i<leaf_page->GetSize(); i++){
      temp_array_[i].first = leaf_page->KeyAt(i);
      temp_array_[i].second = leaf_page->ValueAt(i);
    }



    //判断是否存在

    for(int k=0; k<leaf_page->GetSize(); k++){
      if(comparator_(temp_array_[k].first, key) == 0){
        delete [] temp_array_;
        return false;
      }
    }
    //将kv插入temp_array;

    int i;
    for(i=leaf_page->GetSize(); i>=1; i--){
      if(comparator_(temp_array_[i-1].first, key) == 1){
        temp_array_[i].first = temp_array_[i-1].first;
        temp_array_[i].second = temp_array_[i-1].second;
      }

      if(comparator_(temp_array_[i-1].first, key) == -1){
        temp_array_[i].first = key;
        temp_array_[i].second = value;
        break ;
      }
    }

    if(i == 0){
      temp_array_[0].first = key;
      temp_array_[0].second = value;
    }

    for( i=0; i<=leaf_page->GetSize();i++){
      std::cout << temp_array_[i].first << std::endl;
    }


    //创建一个新leaf_page
    page_id_t  new_page_id;
    auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);



    LeafPage* new_leaf_page = new_page_guard.AsMut<LeafPage>();
    new_leaf_page->Init(leaf_max_size_);

    for(i=0; i< leaf_max_size_/2+1; i++){
      leaf_page->SetKeyAt(i, temp_array_[i].first);
      leaf_page->SetValueAt(i, temp_array_[i].second);
    }


    for(i=leaf_max_size_/2+1; i<=leaf_page->GetSize(); i++){
      new_leaf_page->SetKeyAt(i-leaf_max_size_/2-1, temp_array_[i].first);
      new_leaf_page->SetValueAt(i-leaf_max_size_/2-1, temp_array_[i].second);
    }

    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_page_id);
    //设置大小
    leaf_page->SetSize(leaf_max_size_/2+1);
    new_leaf_page->SetSize((leaf_max_size_+1)/2);


    /*
    for(int k=0; k<new_leaf_page->GetSize(); k++){
      std::cout << leaf_page->KeyAt(k) << std::endl;
    }
    if(new_page_id == 8){
      throw Exception("new_page_id");
    }
*/


    delete [] temp_array_;
    //将new_leaf_page在parent中记录(key: array_[0], value new_page_id)
    InsertIntoParent(ctx, curr_guard.PageId(), new_leaf_page->KeyAt(0), new_page_id);
    curr_guard.Drop();
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(Context &context, const page_id_t& curr_page_id,const KeyType &key, const page_id_t &value) {


  std::cout << "insert into pareant: " << curr_page_id << ", " << key << ", " << value << std::endl;
  if(curr_page_id == GetRootPageId()){
    //新建一个根节点
    page_id_t  new_page_id;
    auto new_root_guard = bpm_->NewPageGuarded(&new_page_id);
    InternalPage * new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);

    //将curr_page_id, key, value写入new_root_page中
    new_root_page->SetValueAt(0, curr_page_id);

    /*
    auto page_guard = bpm_->FetchPageWrite(curr_page_id);
    auto page = page_guard.AsMut<BPlusTreePage>();


    if(page->IsLeafPage()){
      LeafPage* temp_page = page_guard.AsMut<LeafPage>();
      new_root_page->SetKeyAt(0, temp_page->KeyAt(0));
    }else{
      InternalPage* temp_page = page_guard.AsMut<InternalPage>();
      new_root_page->SetKeyAt(0, temp_page->KeyAt(0));
    }
     */
    new_root_page->SetKeyAt(1, key);
    new_root_page->SetValueAt(1, value);
    new_root_page->SetSize(2);

    /*
    auto page_guard = bpm_->FetchPageWrite(curr_page_id);
    auto page = page_guard.AsMut<BPlusTreePage>();
    if(page->IsLeafPage()){
      LeafPage* temp_page = page_guard.AsMut<LeafPage>();
      new_root_page->Insert(comparator_, temp_page->KeyAt(0), curr_page_id);
    }else{
      InternalPage* temp_page = page_guard.AsMut<InternalPage>();
      new_root_page->Insert(comparator_, temp_page->KeyAt(0), curr_page_id);
    }

    new_root_page->Insert(comparator_, key, value);

*/    //修改header_page
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_page_id;
    return;
  }

  auto parent_guard = std::move(context.write_set_.back());
  context.write_set_.pop_back();
  InternalPage* parent_page = parent_guard.AsMut<InternalPage>();

  if(parent_page->GetSize() < internal_max_size_){
     parent_page->Insert(comparator_,key, value);
  }else{
     std::pair<KeyType, page_id_t>* temp_array = new std::pair<KeyType, page_id_t>[internal_max_size_+1];
     for(int i=0; i<parent_page->GetSize(); i++){
        temp_array[i].first = parent_page->KeyAt(i);
        temp_array[i].second = parent_page->ValueAt(i);
     }

     //将kv插入到temp_array中

     int i;
     for(i= parent_page->GetSize(); i>=2; i--){
        if(comparator_(temp_array[i-1].first, key) == 1){
          temp_array[i].first = temp_array[i-1].first;
          temp_array[i].second = temp_array[i-1].second;
        }

        if(comparator_(temp_array[i-1].first, key) == -1){
          temp_array[i].first = key;
          temp_array[i].second = value;
          break ;
        }
     }

     if(i == 1){
        temp_array[i].first = key;
        temp_array[i].second = value;
     }

     std::cout << "parent_page: "<< std::endl;
     for(i=0; i<= parent_page->GetSize(); i++){
       // std::cout << parent_page->KeyAt(i) << std::endl;
        std::cout << temp_array[i].first << std::endl;
     }

     //创建一个新的internal_page
     page_id_t  new_page_id;
     auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
     InternalPage* new_internal_page = new_page_guard.AsMut<InternalPage>();
     new_internal_page->Init(internal_max_size_);

     for(i=0; i<internal_max_size_/2+1; i++){
        parent_page->SetKeyAt(i, temp_array[i].first);
        parent_page->SetValueAt(i, temp_array[i].second);
     }


     for(i=internal_max_size_/2+1; i<=parent_page->GetSize(); i++){
        new_internal_page->SetKeyAt(i-internal_max_size_/2-1, temp_array[i].first);
        new_internal_page->SetValueAt(i-internal_max_size_/2-1, temp_array[i].second);
     }
     parent_page->SetSize(internal_max_size_/2+1);
     new_internal_page->SetSize((internal_max_size_+1)/2);
     delete [] temp_array;
    //将kv插入到其parent中
     InsertIntoParent(context,parent_guard.PageId(),new_internal_page->KeyAt(0), new_page_id);
    parent_guard.Drop();
  }
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
 std::cout << "==remove key: " << key << std::endl;
  Context ctx;
  (void)ctx;

  //找到key所在的叶子节点
  page_id_t root_page_id =GetRootPageId();
  auto curr_guard = bpm_->FetchPageWrite(root_page_id);

  auto curr_page = curr_guard.AsMut<BPlusTreePage>();

//  ctx.write_set_.push_back(std::move(curr_guard));
  while(!curr_page->IsLeafPage()){
    std::cout << "hello" << std::endl;
     const InternalPage *temp_page = curr_guard.As<InternalPage>();
     ctx.write_set_.push_back(std::move(curr_guard));
     int i;
     for(i=1; i<temp_page->GetSize(); i++){
        if(comparator_(temp_page->KeyAt(i), key) == 1){
          curr_guard = bpm_->FetchPageWrite(temp_page->ValueAt(i-1));
          curr_page = curr_guard.AsMut<BPlusTreePage>();
          break ;
        }

        if(comparator_(temp_page->KeyAt(i), key) == 0){
          curr_guard = bpm_->FetchPageWrite(temp_page->ValueAt(i));
          curr_page = curr_guard.AsMut<BPlusTreePage>();
          break ;
        }
     }

     if(i == temp_page->GetSize()){
        curr_guard = bpm_->FetchPageWrite(temp_page->ValueAt(i-1));
        curr_page = curr_guard.AsMut<BPlusTreePage>();
     }
  }

  //LeafPage * leaf_page = curr_guard.AsMut<LeafPage>();
  //leaf_page->Delete(comparator_, key);
  std::cout << "!!!!!" << std::endl;
  DeleteEntry(ctx, curr_guard.PageId(), key);
  std::cout << "==success remove" << std::endl;

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(bustub::Context &context, const bustub::page_id_t &curr_page_id, const KeyType& key) {
  std::cout << "****delete key: " << key << std::endl;
  auto curr_page_guard = bpm_->FetchPageWrite(curr_page_id);
  auto curr_page = curr_page_guard.AsMut<BPlusTreePage>();
  page_id_t child_page_id = -1;

  if(curr_page->IsLeafPage()){
     LeafPage*  temp_leaf_page = curr_page_guard.AsMut<LeafPage>();

     temp_leaf_page->Delete(comparator_, key);
     std::cout << "leaf delete key " << temp_leaf_page->GetSize() <<std::endl;
  }else{
     InternalPage* temp_internal_page = curr_page_guard.AsMut<InternalPage>();
     temp_internal_page->Delete(comparator_, key);
     child_page_id = temp_internal_page->ValueAt(0);
  }

  if(curr_page_id == GetRootPageId()){
     if(child_page_id !=-1  && curr_page->GetSize() == 1){
       auto header_guard = bpm_->FetchPageWrite(header_page_id_);
       auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
       header_page->root_page_id_ = child_page_id;
     }

     if(curr_page->IsLeafPage() && curr_page->GetSize() == 0){
       auto header_guard = bpm_->FetchPageWrite(header_page_id_);
       auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
       header_page->root_page_id_ = INVALID_PAGE_ID;
     }
     return;
  }

  auto parent_page_guard = std::move(context.write_set_.back());
  context.write_set_.pop_back();

  InternalPage *parent_page =  parent_page_guard.AsMut<InternalPage>();

  std::cout << "====" <<curr_page->GetSize() << ", " << curr_page->GetMinSize() <<std::endl;
  if(curr_page->GetSize() < curr_page->GetMinSize()){

     //通过curr_page_id找到此parent_page中左右兄弟的位置
     int left_sibling_index = -1;
     int right_sibling_index = -1;
     page_id_t left_sibling_page_id = -1;
     page_id_t right_sibling_page_id = -1;
     int curr_index=-1;
     for(int i=0; i<parent_page->GetSize(); i++){

       /*
       if(comparator_(parent_page->KeyAt(i), key) == 0){
          curr_index = i;
          break ;
       }

*/

       /*
       if(comparator_(parent_page->KeyAt(i), key) == 0){
          curr_index = i;
          break ;
       }

       if(comparator_(parent_page->KeyAt(i), key) == -1){
          curr_index = i-1;
          break ;
       }
        */
       if(parent_page->ValueAt(i) == curr_page_id){
          curr_index = i;
          break ;
       }
     }

    // KeyType delete_key = key;
     //int right_index = -1;
     if(curr_index == 0){
        right_sibling_index = curr_index+1;
        right_sibling_page_id = parent_page->ValueAt(right_sibling_index);
        //delete_key = parent_page->KeyAt(right_sibling_index);
     }else if(curr_index == parent_page->GetSize()-1){
        left_sibling_index = curr_index-1;
        left_sibling_page_id = parent_page->ValueAt(left_sibling_index);
     } else{
        left_sibling_index = curr_index-1;
        right_sibling_index = curr_index+1;
        right_sibling_page_id = parent_page->ValueAt(right_sibling_index);
        left_sibling_page_id = parent_page->ValueAt(left_sibling_index);
     }

     WritePageGuard right_sibling_guard;
     WritePageGuard left_sibling_guard;
     BPlusTreePage* left_sibling_page = nullptr;
     BPlusTreePage* right_sibling_page = nullptr;

     if(right_sibling_page_id != -1){
        right_sibling_guard = bpm_->FetchPageWrite(right_sibling_page_id);
        right_sibling_page = right_sibling_guard.AsMut<BPlusTreePage>();
     }

     if(left_sibling_page_id != -1){
        left_sibling_guard = bpm_->FetchPageWrite(left_sibling_page_id);
        left_sibling_page = left_sibling_guard.AsMut<BPlusTreePage>();
     }

     bool left_can_coalesce =(left_sibling_page != nullptr) && (left_sibling_page->GetSize() + curr_page->GetSize() <= curr_page->GetMaxSize());
     bool right_can_coalesce = (right_sibling_page != nullptr) && (right_sibling_page->GetSize() + curr_page->GetSize() <= curr_page->GetMaxSize());

    if(left_can_coalesce || right_can_coalesce){
        KeyType delete_key;
        if(curr_page->IsLeafPage()){
          LeafPage *temp_left_page = nullptr;
          LeafPage *temp_curr_page = nullptr;
          if(left_can_coalesce){
            temp_left_page = left_sibling_guard.AsMut<LeafPage>();
            temp_curr_page = curr_page_guard.AsMut<LeafPage>();
            delete_key = parent_page->KeyAt(curr_index);
            std::cout << "====" << std::endl;
          }else if(!left_can_coalesce && right_can_coalesce){
            temp_left_page = curr_page_guard.AsMut<LeafPage>();
            temp_curr_page = right_sibling_guard.AsMut<LeafPage>();
            delete_key = parent_page->KeyAt(right_sibling_index);
            std::cout << "###" << std::endl;
          }

          for(int i=0; i<temp_curr_page->GetSize(); i++){
            temp_left_page->Insert(comparator_, temp_curr_page->KeyAt(i), temp_curr_page->ValueAt(i));
          }
          std::cout << "###delete key: " << delete_key  << ", " << temp_curr_page->GetNextPageId() << std::endl;
         temp_left_page->SetNextPageId(temp_curr_page->GetNextPageId());
        // temp_curr_page->SetNextPageId(-1);

        }else{
          InternalPage *temp_left_page = nullptr;
          InternalPage *temp_curr_page = nullptr;

          if(left_can_coalesce){
            temp_left_page = left_sibling_guard.AsMut<InternalPage>();
            temp_curr_page = curr_page_guard.AsMut<InternalPage>();
            delete_key = parent_page->KeyAt(curr_index);
          } else if(!left_can_coalesce && right_can_coalesce){
            temp_left_page = curr_page_guard.AsMut<InternalPage>();
            temp_curr_page = right_sibling_guard.AsMut<InternalPage>();
            delete_key =  parent_page->KeyAt(right_sibling_index);
          }
          temp_left_page->Insert(comparator_, delete_key, temp_curr_page->ValueAt(0));
          for(int i=1; i<temp_curr_page->GetSize(); i++){
            temp_left_page->Insert(comparator_, temp_curr_page->KeyAt(i), temp_curr_page->ValueAt(i));
          }


        }

         DeleteEntry(context, parent_page_guard.PageId(), delete_key);
    }else{
        //从左兄弟中偷一个元素
        if(left_sibling_page != nullptr){
            if(left_sibling_page->IsLeafPage()){
                LeafPage* temp_left_page = left_sibling_guard.AsMut<LeafPage>();
                LeafPage* temp_curr_page =  curr_page_guard.AsMut<LeafPage>();
                temp_curr_page->Insert(comparator_, temp_left_page->KeyAt(temp_left_page->GetSize()-1), temp_left_page->ValueAt(temp_left_page->GetSize()-1));
                temp_left_page->Delete(comparator_, temp_left_page->KeyAt(temp_left_page->GetSize()-1));

                //替换parent中相应key
                parent_page->SetKeyAt(curr_index, temp_curr_page->KeyAt(0));
            }else{
                InternalPage * temp_left_page = left_sibling_guard.AsMut<InternalPage>();
                InternalPage * temp_curr_page =  curr_page_guard.AsMut<InternalPage>();

                temp_curr_page->SetKeyAt(0, parent_page->KeyAt(curr_index));
                temp_curr_page->InsertFirst(temp_left_page->KeyAt(temp_left_page->GetSize()-1), temp_left_page->ValueAt(temp_left_page->GetSize()-1));
                temp_left_page->Delete(comparator_, temp_left_page->KeyAt(temp_left_page->GetSize()-1));
                //temp_left_page->Delete(comparator_, temp_left_page->ValueAt(temp_left_page->GetSize()-1));
                //替换parent中相应key
                parent_page->SetKeyAt(curr_index, temp_curr_page->KeyAt(0));
             }
        }else{
            if(right_sibling_page != nullptr){
                if(right_sibling_page->IsLeafPage()){
                  LeafPage* temp_curr_page = curr_page_guard.AsMut<LeafPage>();
                  LeafPage* temp_right_page = right_sibling_guard.AsMut<LeafPage>();
                  temp_curr_page->Insert(comparator_, temp_right_page->KeyAt(0), temp_right_page->ValueAt(0));
                  temp_right_page->Delete(comparator_, temp_right_page->KeyAt(0));

                  //替换parent中相应key
                  parent_page->SetKeyAt(right_sibling_index, temp_curr_page->KeyAt(temp_curr_page->GetSize()-1));
                }else{
                  InternalPage* temp_curr_page = curr_page_guard.AsMut<InternalPage>();
                  InternalPage* temp_right_page = right_sibling_guard.AsMut<InternalPage>();

                  temp_curr_page->Insert(comparator_, parent_page->KeyAt(right_sibling_index), temp_right_page->ValueAt(0));
                  temp_right_page->DeleteFirst();
                  //替换parent中相应key
                  parent_page->SetKeyAt(right_sibling_index, temp_right_page->KeyAt(0));
                }

            }
        }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {

  page_id_t  root_page_id = GetRootPageId();
  auto curr_page_guard = bpm_->FetchPageWrite(root_page_id);
  auto curr_page = curr_page_guard.AsMut<BPlusTreePage>();

  while(!curr_page->IsLeafPage()){
    InternalPage *temp_page = curr_page_guard.AsMut<InternalPage>();
    curr_page_guard = bpm_->FetchPageWrite(temp_page->ValueAt(0));
  }


  return INDEXITERATOR_TYPE(bpm_, curr_page_guard.PageId(), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {

  std::cout << "find begin of key: " << key << std::endl;
  std::cout << "#########" << std::endl;
  page_id_t root_page_id = GetRootPageId();
  auto curr_guard = bpm_->FetchPageRead(root_page_id);
  auto curr_page = curr_guard.As<BPlusTreePage>();


  while(!curr_page->IsLeafPage()){
     const InternalPage* temp_page = curr_guard.As<InternalPage>();
     int i;
     for(i=1; i<temp_page->GetSize(); i++){
        if(comparator_(temp_page->KeyAt(i), key) == 1){
          curr_guard = bpm_->FetchPageRead(temp_page->ValueAt(i-1));
          curr_page = curr_guard.As<BPlusTreePage>();
          break ;
        }

        if(comparator_(temp_page->KeyAt(i), key) == 0){
          curr_guard = bpm_->FetchPageRead(temp_page->ValueAt(i));
          curr_page = curr_guard.As<BPlusTreePage>();
          break ;
        }
     }

     if(i == temp_page->GetSize()){
        curr_guard = bpm_->FetchPageRead(temp_page->ValueAt(i-1));
        curr_page = curr_guard.As<BPlusTreePage>();
     }
  }


  const LeafPage * leaf_page = curr_guard.As<LeafPage>();
  int index=-1;
  for(int i=0; i<leaf_page->GetSize(); i++){
     if(comparator_(leaf_page->KeyAt(i), key) >=0){
        index = i;
        break ;
     }
  }
  return INDEXITERATOR_TYPE(bpm_, curr_guard.PageId(), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  //std::cout << "end" << std::endl;
  return INDEXITERATOR_TYPE(true);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  std::cout << "#root_page_id: " << header_page->root_page_id_ << std::endl;
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
