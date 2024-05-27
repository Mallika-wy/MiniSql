#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"


/*
TableIterator是什么 : 堆表的迭代器
什么是堆表,堆表用于组织一连串的tuple(row)
迭代器的目的在于方便的获得当前row的下一个row
*/
/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
	this->table_heap = table_heap;
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    this->row=new Row(rid);
    this->table_heap->GetTuple(this->row, nullptr);
  } else {
		this->row=new Row(INVALID_ROWID);
	} 
	this->txn = txn;
}

// 构造函数，参数是另一个迭代器
TableIterator::TableIterator(const TableIterator &other) {
	this->row = new Row(*(other.row)); // deep copy
	this->table_heap = other.table_heap;
	this->txn = other.txn;
}

TableIterator::~TableIterator() {
	if(row != nullptr)
		delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(this->table_heap == itr.table_heap && this->row->GetRowId() == itr.row->GetRowId())
    return true;
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
	return !(*this == itr);
}

const Row &TableIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
	return *row;
}

Row *TableIterator::operator->() {
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
//  ASSERT(false, "Not implemented yet.");
  this->table_heap = itr.table_heap;
  this->row= new Row(*itr.row);
	this->txn = itr.txn;
	return *this;
}


// ++iter
TableIterator &TableIterator::operator++() {
	// ++iter：iter变成下一个，并返回下一个
	BufferPoolManager* buffer_pool_manager = this->table_heap->buffer_pool_manager_;
	TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(this->row->GetRowId().GetPageId()));
	RowId *next_row_id = new RowId();
	// 尝试从当前page获得下一个tuple的id
	bool flag = page->GetNextTupleRid(row->GetRowId(), next_row_id);
	delete row; // 即时释放不需要的空间
  if (flag) { // 下一个tuple就在当前page
    this->row = new Row(*next_row_id);
    page->GetTuple(this->row, this->table_heap->schema_, this->txn, this->table_heap->lock_manager_);
  } else { // 当前page没有下一个tuple
		// 如果不在当前page，就在下一个page
    page_id_t next_page_id = page->GetNextPageId();
    page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(next_page_id));
		if (page == nullptr) {
			next_row_id->Set(INVALID_PAGE_ID, 0);
			this->row = new Row(*next_row_id);
			this->table_heap = nullptr;
		} else {
			page->GetFirstTupleRid(next_row_id);
			this->row = new Row(*next_row_id);
			page->GetTuple(this->row, this->table_heap->schema_, this->txn, this->table_heap->lock_manager_);
		}
  }
	delete next_row_id;
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
	// iter++：iter变成下一个，返回当前 
	// 调用构造函数，保存一份当前的
	RowId row_id = this->row->GetRowId();
	TableHeap* tmp_table_heap = this->table_heap;
	++(*this);
	return TableIterator(tmp_table_heap, row_id, this->txn); 
}
