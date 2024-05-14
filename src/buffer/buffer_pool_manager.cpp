#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // page_id 有范围
  if (!(page_id >= 0 && page_id <= MAX_VALID_PAGE_ID)) return nullptr;

  frame_id_t frame_id;  // page_id对应的buffer frame标号

  // 1.1 page_id已在buffer中
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  // 1.2
  // get frame_id
  if (!free_list_.empty()) {  // buffer还有空间
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {                                              // buffer已经没有空间
    if (!replacer_->Victim(&frame_id)) return nullptr;  // 所有数据页都被固定
    // dirty处理
    if (pages_[frame_id].is_dirty_) FlushPage(pages_[frame_id].page_id_);
    page_table_.erase(pages_[frame_id].page_id_);
  }
  page_table_[page_id] = frame_id;                           // page_id与frame_id关联
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);  // 从disk加载数据
  pages_[frame_id].pin_count_ = 1;                           // 第一次加入到buffer，所以pin_count为1
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  replacer_->Pin(frame_id);

  return &pages_[frame_id];
}

/**
 * 分配一个新的数据页，并将逻辑页号于page_id中返回；
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t frame_id;
  if (!free_list_.empty()) {  // buffer还有空间
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {                                              // buffer已经没有空间
    if (!replacer_->Victim(&frame_id)) return nullptr;  // 所有数据页都被固定
    // dirty处理
    if (pages_[frame_id].is_dirty_) FlushPage(pages_[frame_id].page_id_);
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // 修改metadata
  page_id = AllocatePage();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  page_table_[page_id] = frame_id;  // 将page_id与frame_id关联

  return &pages_[frame_id];
}

/**
 * 释放一个数据页
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end()) return true;
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) return false;
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end())
    return false;  // 此page_id对应的数据页没有加载到buffer中，unpin无从谈起

  frame_id_t frame_id = page_table_[page_id];

  if (pages_[frame_id].pin_count_ == 0) return false;  // 按理来说初始不为0
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  pages_[frame_id].is_dirty_ = is_dirty;

  return true;
}

/**
 * 将page_id对应的buffer中的数据写回disk
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) return false;

  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;

  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}