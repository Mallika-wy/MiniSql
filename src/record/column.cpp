#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* Column序列化
*/
uint32_t Column::SerializeTo(char *buf) const {
	// 定义偏移
	uint32_t offset = 0;
	// COLUMN_MAGIC_NUM序列化，用于校验
	MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
	offset += sizeof(uint32_t);
	// name_序列化
  size_t name_len = name_.length();
	MACH_WRITE_TO(size_t, buf+offset, name_len);
	offset += sizeof(size_t);
	memcpy(buf+offset, &name_, name_len);
	offset += name_len;
	// type_序列化
	MACH_WRITE_TO(TypeId, buf+offset, type_);
	offset += sizeof(TypeId);
	// len_序列化，如果不是char类型，反序列化的时候会忽略
	MACH_WRITE_TO(uint32_t, buf+offset, len_);
	offset += sizeof(uint32_t);
	// table_ind_序列化
	MACH_WRITE_TO(uint32_t, buf+offset, table_ind_);
	offset += sizeof(uint32_t);
	// nullable_序列化
	MACH_WRITE_TO(bool, buf+offset, nullable_);
	offset += sizeof(bool);
	// unique_序列化
	MACH_WRITE_TO(bool, buf+offset, unique_);
	offset += sizeof(bool);

	// 返回这个对象占用的所有内存大小
  return offset;
}

/**
 * 序列化大小
 */
uint32_t Column::GetSerializedSize() const {
  return 2*sizeof(bool) +  3*sizeof(uint32_t) +  sizeof(TypeId) + name_.length() + sizeof(size_t);
}

/**
 * Column反序列化
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  
	if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
  }
	
	uint32_t offset = 0;
	uint32_t size = 0;

	// column判断
	uint32_t column_magic_num = MACH_READ_UINT32(buf+offset);
	ASSERT(column_magic_num == COLUMN_MAGIC_NUM, "This is not a column");
	offset += sizeof(uint32_t);

	// 反序列化name长度
	size_t name_len = MACH_READ_FROM(size_t, buf+offset);
	size += sizeof(size_t);

	// 反序列化name_
	char* name_char = new char[name_len];
	memcpy(name_char, buf+offset, name_len);
	std::string name(name_char);
	offset += name_len;
	
	// 反序列化type_
	TypeId type = MACH_READ_FROM(TypeId, buf+offset);
	offset += sizeof(TypeId);

	// 反序列化len_
	uint32_t len = MACH_READ_UINT32(buf+offset);
	offset += sizeof(uint32_t);

	// 反序列化table_ind
	uint32_t table_ind = MACH_READ_UINT32(buf+offset);
	offset += sizeof(uint32_t);

	// 反序列化nullable
	bool nullable = MACH_READ_FROM(bool, buf+offset);
	offset += sizeof(bool);

	// 反序列化unique
	bool unique = MACH_READ_FROM(bool, buf+offset);
	offset += sizeof(bool);

	// 根据类型新建column对象
	if (type == kTypeChar) {
		column = new Column(name, type, len, table_ind, nullable, unique);
	} else {
		column = new Column(name, type, table_ind, nullable, unique);
	}

	delete[] name_char;

  return offset;
}
