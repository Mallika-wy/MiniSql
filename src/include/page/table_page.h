#ifndef MINISQL_TUPLE_H
#define MINISQL_TUPLE_H
/**
 * Basic Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  ----------------------------------------------------------------
 **/

#include <cstring>

#include "common/macros.h"
#include "common/rowid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/txn.h"
#include "page/page.h"
#include "record/row.h"
#include "recovery/log_manager.h"

class TablePage : public Page {
 public:
  void Init(page_id_t page_id, page_id_t prev_id, LogManager *log_mgr, Txn *txn);

	// 根据TablePage的定义，TablePage最前面的部分为Header，存储TablePage的相关说明，数据
	// 最前面的是TablePageId，通过类型转换，获得数据
  page_id_t GetTablePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

	// 其次是PrevPageId，通过类型转换，获得数据
  page_id_t GetPrevPageId() { return *reinterpret_cast<page_id_t *>(GetData() + OFFSET_PREV_PAGE_ID); }

	// 然后是NextPageId，通过类型转换，获得数据
  page_id_t GetNextPageId() { return *reinterpret_cast<page_id_t *>(GetData() + OFFSET_NEXT_PAGE_ID); }

	// 修改PrevPageId，
  void SetPrevPageId(page_id_t prev_page_id) {
    memcpy(GetData() + OFFSET_PREV_PAGE_ID, &prev_page_id, sizeof(page_id_t));
  }

	// 修改NextPageId
  void SetNextPageId(page_id_t next_page_id) {
    memcpy(GetData() + OFFSET_NEXT_PAGE_ID, &next_page_id, sizeof(page_id_t));
  }

  bool InsertTuple(Row &row, Schema *schema, Txn *txn, LockManager *lock_manager, LogManager *log_manager);

  bool MarkDelete(const RowId &rid, Txn *txn, LockManager *lock_manager, LogManager *log_manager);

  int UpdateTuple(const Row &new_row, Row *old_row, Schema *schema, Txn *txn, LockManager *lock_manager,
                   LogManager *log_manager);

  void ApplyDelete(const RowId &rid, Txn *txn, LogManager *log_manager);

  void RollbackDelete(const RowId &rid, Txn *txn, LogManager *log_manager);

  bool GetTuple(Row *row, Schema *schema, Txn *txn, LockManager *lock_manager);

  bool GetFirstTupleRid(RowId *first_rid);

  bool GetNextTupleRid(const RowId &cur_rid, RowId *next_rid);

 private:
  uint32_t GetFreeSpacePointer() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE); }

	// 修改FreeSpacePointer
  void SetFreeSpacePointer(uint32_t free_space_pointer) {
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space_pointer, sizeof(uint32_t));
  }

	// 从data_中获得TupleCount
  uint32_t GetTupleCount() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_COUNT); }

	// 修改TupleCount
  void SetTupleCount(uint32_t tuple_count) { memcpy(GetData() + OFFSET_TUPLE_COUNT, &tuple_count, sizeof(uint32_t)); }

	// 计算可以被利用的空间
	// FreeSpacePointer之后的内存用于实际数据存储，在一页Page前部，有Header，同时还有Tuple的指针
	// 在计算的时候需要减去
  uint32_t GetFreeSpaceRemaining() {
    return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER - SIZE_TUPLE * GetTupleCount();
  }

  uint32_t GetTupleOffsetAtSlot(uint32_t slot_num) {
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_OFFSET + SIZE_TUPLE * slot_num);
  }

  void SetTupleOffsetAtSlot(uint32_t slot_num, uint32_t offset) {
    memcpy(GetData() + OFFSET_TUPLE_OFFSET + SIZE_TUPLE * slot_num, &offset, sizeof(uint32_t));
  }

  uint32_t GetTupleSize(uint32_t slot_num) {
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_SIZE + SIZE_TUPLE * slot_num);
  }

  void SetTupleSize(uint32_t slot_num, uint32_t size) {
    memcpy(GetData() + OFFSET_TUPLE_SIZE + SIZE_TUPLE * slot_num, &size, sizeof(uint32_t));
  }

	// 如果标记DELETE_MASK，或者size为0，就可以认识被删除
  static bool IsDeleted(uint32_t tuple_size) { return static_cast<bool>(tuple_size & DELETE_MASK) || tuple_size == 0; }

  static uint32_t SetDeletedFlag(uint32_t tuple_size) { return static_cast<uint32_t>(tuple_size | DELETE_MASK); }

  static uint32_t UnsetDeletedFlag(uint32_t tuple_size) { return static_cast<uint32_t>(tuple_size & (~DELETE_MASK)); }

 private:
  static_assert(sizeof(page_id_t) == 4);
	// DELETE_MASK是一个位掩码，让1左移31位至32位，用于标记一个Tuple被删除
  static constexpr uint64_t DELETE_MASK = (1U << (8 * sizeof(uint32_t) - 1));
  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 24;
  static constexpr size_t SIZE_TUPLE = 8;
  static constexpr size_t OFFSET_PREV_PAGE_ID = 8; // PrevPageId的偏移量
  static constexpr size_t OFFSET_NEXT_PAGE_ID = 12; // NextPageId的偏移量
  static constexpr size_t OFFSET_FREE_SPACE = 16; // FreeSpacePointer的偏移量
  static constexpr size_t OFFSET_TUPLE_COUNT = 20; // TupleCount的偏移量
  static constexpr size_t OFFSET_TUPLE_OFFSET = 24; // TupleOffset的偏移量
  static constexpr size_t OFFSET_TUPLE_SIZE = 28; // TupleSize的偏移量

 public:
  static constexpr size_t SIZE_MAX_ROW = PAGE_SIZE - SIZE_TABLE_PAGE_HEADER - SIZE_TUPLE;
};

#endif