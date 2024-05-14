#ifndef MINISQL_COLUMN_H
#define MINISQL_COLUMN_H

#include <string>

#include "common/macros.h"
#include "record/types.h"

class Column {
  friend class Schema;

 public:
	// 构造函数：int、float类型数据字段
  Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique);

	// 构造函数：char类型数据字段
  Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique);

	// 构造函数，实现复制
  Column(const Column *other);

  std::string GetName() const { return name_; }

  uint32_t GetLength() const { return len_; }

  void SetTableInd(uint32_t ind) { table_ind_ = ind; }

  uint32_t GetTableInd() const { return table_ind_; }

  bool IsNullable() const { return nullable_; }

  bool IsUnique() const { return unique_; }

  TypeId GetType() const { return type_; }

	// 序列化
  uint32_t SerializeTo(char *buf) const;

	// 获得长度
  uint32_t GetSerializedSize() const;

	// 反序列化
	// Column *&column是一个指针引用
	// Column* &column这样写或许更好理解
  static uint32_t DeserializeFrom(char *buf, Column *&column);

 private:
  static constexpr uint32_t COLUMN_MAGIC_NUM = 210928;
  std::string name_;
  TypeId type_;
  uint32_t len_{0};  // for char type this is the maximum byte length of the string data,
  // otherwise is the fixed size
  uint32_t table_ind_{0};  // column position in table
  bool nullable_{false};   // whether the column can be null
  bool unique_{false};     // whether the column is unique
};

#endif  // MINISQL_COLUMN_H
