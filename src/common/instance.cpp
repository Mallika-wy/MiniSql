//
// Created by njz on 2023/1/15.
//
#include "common/instance.h"

DBStorageEngine::DBStorageEngine(std::string db_name, bool init, uint32_t buffer_pool_size)
    : db_file_name_(std::move(db_name)), init_(init) {
  // Init database file if needed
  db_file_name_ = "./databases/" + db_file_name_;
  // 如果init_为true,那么需要删除原来数据库文件,重新建立这个数据库
  if (init_) {
    // remove用于删除一个文件,参数是文件名
    // 如果删除失败,返回非0值
    remove(db_file_name_.c_str());
  }
  // Initialize components
  disk_mgr_ = new DiskManager(db_file_name_);
  bpm_ = new BufferPoolManager(buffer_pool_size, disk_mgr_);

  // Allocate static page for db storage engine
  // 如果需要新建数据库文件的话,需要......
  if (init) {
    page_id_t id;
    // 检查CATALOG_META catalog元数据是否空闲
    if (!bpm_->IsPageFree(CATALOG_META_PAGE_ID)) {
      throw logic_error("Catalog meta page not free.");
    }
    // 检查INDEX_ROOTS index元数据是否空闲
    if (!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
      throw logic_error("Header page not free.");
    }
    // 新New一个Page,那么逻辑编号一定是按顺序的.
    // CATALOG_META_PAGE_ID = 0
    if (bpm_->NewPage(id) == nullptr || id != CATALOG_META_PAGE_ID) {
      throw logic_error("Failed to allocate catalog meta page.");
    }
    // INDEX_ROOTS_PAGE_ID = 1
    if (bpm_->NewPage(id) == nullptr || id != INDEX_ROOTS_PAGE_ID) {
      throw logic_error("Failed to allocate header page.");
    }
    // 保证逻辑编号为0和1的两个Page被占用,否则肯定失败
    if (bpm_->IsPageFree(CATALOG_META_PAGE_ID) || bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
      exit(1);
    }
    // newpage会pin这个page,需要手动解除
    bpm_->UnpinPage(CATALOG_META_PAGE_ID, false);
    bpm_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  } else {
    // 如果说不需要init,那么CATALOG_META_PAGE_ID和INDEX_ROOTS_PAGE_ID的两个page一定被占用
    ASSERT(!bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Invalid catalog meta page.");
    ASSERT(!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Invalid header page.");
  }
  // 创建catalog_manager
  catalog_mgr_ = new CatalogManager(bpm_, nullptr, nullptr, init);
}

// DBStorageEngine的析构函数, 释放掉对象中的new来的变量,catalog,bufferpool,disk.
DBStorageEngine::~DBStorageEngine() {
  delete catalog_mgr_;
  delete bpm_;
  delete disk_mgr_;
}

std::unique_ptr<ExecuteContext> DBStorageEngine::MakeExecuteContext(Txn *txn) {
  // unique_ptr一种智能指针
  // 自动管理它所指向的对象的生命周期
  // 即在 std::unique_ptr 对象被销毁时，它所指向的对象的内存也会被自动释放
  // 此外，std::unique_ptr 还有一个特性，那就是每个 std::unique_ptr
  // 对象都是独占它所指向的对象的所有权，即同一时间只能有一个 std::unique_ptr 指向同一个对象。
  return std::make_unique<ExecuteContext>(txn, catalog_mgr_, bpm_);
}
