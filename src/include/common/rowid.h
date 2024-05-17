#ifndef MINISQL_RID_H
#define MINISQL_RID_H

#include <cstdint>
#include <functional> // 定义函数对象及其相关操作

#include "common/config.h"

/**
 * | page_id(32bit) | slot_num(32bit) |
 */
class RowId {
 public:
  RowId() = default; 

  RowId(page_id_t page_id, uint32_t slot_num) : page_id_(page_id), slot_num_(slot_num) {}

	// 讲一个64位整数的高32位赋值给page_id_，将低32位赋值给slot_num_
  explicit RowId(int64_t rid) : page_id_(static_cast<page_id_t>(rid >> 32)), slot_num_(static_cast<uint32_t>(rid)) {}

	// 合并成一个64位整数
  inline int64_t Get() const { return (static_cast<int64_t>(page_id_)) << 32 | slot_num_; }

  inline page_id_t GetPageId() const { return page_id_; }

  inline uint32_t GetSlotNum() const { return slot_num_; }

  inline void Set(page_id_t page_id, uint32_t slot_num) {
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

	// ==运算符重载，当page_id_和slot_num_相同就可以认为两个对象相同
  bool operator==(const RowId &other) const { return page_id_ == other.page_id_ && slot_num_ == other.slot_num_; }

 private:
  page_id_t page_id_{INVALID_PAGE_ID};
  uint32_t slot_num_{0};  // logical offset of the record in page, starts from 0. eg:0, 1, 2...
};

// 提供hash特化版本,将RowId作为键
// RowId-->int64,经过一层哈希,得到size_t类型的哈希值
namespace std {
template <>
struct hash<RowId> {
  size_t operator()(const RowId &rid) const { return hash<int64_t>()(rid.Get()); }
};
}  // namespace std

static const RowId INVALID_ROWID = RowId(INVALID_PAGE_ID, 0);

#endif  // MINISQL_RID_H
