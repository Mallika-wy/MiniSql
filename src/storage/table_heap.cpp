#include "storage/table_heap.h"

/**
 * InsertTuple
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
	// 获得row序列化所需要的内存空间
	uint32_t row_size = row.GetSerializedSize(schema_);
	// 如果空间大于row类型支持的最大空间，一定不符合要求
	if (row_size > SIZE_MAX_ROW) return false;

	bool is_success_insert = false;
	// 定义两个page_id_t，用于循环迭代找到能存入row的page
	page_id_t cur_page_id = first_page_id_, prev_page_id = INVALID_PAGE_ID;
	TablePage *page = nullptr;
	while (1) {
		page = reinterpret_cast<TablePage *>buffer_pool_manager_->FetchPage(cur_page_id);
		// 想必对于page进行操作的时候都需要加上一个锁 ？？？
		page->WLatch();
		bool is_success_insert = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
		page->WUnlatch();
		// 如果当前page有足够的空间，则一定会成功插入
		if (is_success_insert) {
			// fatch一个page的时候就会将这个页面暂时pin，但是我们对于这个page的使用已经结束
			// 所以需要unpin，同时这个page已经被修改，所以is_dirty = true
			buffer_pool_manager_->UnpinPage(cur_page_id, true);
			return true;
		}
		buffer_pool_manager_->UnpinPage(cur_page_id, false);
		prev_page_id = cur_page_id;
		cur_page_id = page->GetNextPageId();
		// 循环出口：table_heap中的当前所有page都无法满足空间需求
		if (cur_page_id == INVALID_PAGE_ID) 
			break;
	}

	// 申请一个新page，用于存放tuple
	page_id_t new_page_id = INVALID_PAGE_ID;
	TablePage *new_page = reinterpret_cast<TablePage *>buffer_pool_manager_->NewPage(new_page_id);
	new_page->WLatch();
	// 新申请的page需要初始化
	new_page->Init(new_page_id, INVALID_PAGE_ID, log_manager_, txn);
	// 直接insert，无需担心失败
	new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
	new_page->WUnlatch();
	page->WLatch();
	// 将new_page加到堆表中
	page->SetNextPageId(new_page_id);
	page->WUnlatch();
	// new的新的page初始时，pincount就是1，所以需要unpin一下
	buffer_pool_manager_->UnpinPage(new_page_id, true);
	return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Txn *txn) {
	// 获得待更新的tuple所在的page
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
	Row *old_row = new Row(rid);
	this->GetTuple(old_row, txn);
  page->WLatch();
	int state = page->UpdateTuple(row,, old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
	delete old_row;
	if (state == 2) return false;
	else if (state == 0) {
		row.SetRowId(rid);
		return true;
	}
	else { // 当前page存不下
		ApplyDelete(rid, txn); // 先delete
    InsertTuple(row, txn); // 再insert
		return true;
	}

}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr, "Page is nullptr");
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
	// 修改page，是脏页
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * GetTuple
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
	// 获得tuple所在的page
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
	// 加上读锁
	page->RLatch();
  bool is_success = page->GetTuple(row, schema_, txn, lock_manager_);
	page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return is_success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * 获得这个堆表中的第一个tuple的迭代器
 */
TableIterator TableHeap::Begin(Txn *txn) { 
	if (first_page_id_ != INVALID_PAGE_ID) {
		TablePage *page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
		if (page == nullptr) return End(); // 如果page无效，就返回一个无效迭代器
		RowId *first_row_id = new RowId();
		buffer_pool_manager_->UnpinPage(first_page_id_, false);
		if (page->GetFirstTupleRid(first_page_id_)) {
			return TableIterator(this, *first_row_id, txn);
		} else {
			return End(); // 如果获得失败，就返回一个无效迭代器
		}
	}
	return End(); // 如果first_page_id无效，就返回一个无效迭代器
}

/**
 * 结束迭代器，可以有很多种方式，只需要清楚地表达这个是一个无效的即可
 */
TableIterator TableHeap::End() { 
	return TableIterator(nullptr, INVALID_ROWID, nullptr); 
}
