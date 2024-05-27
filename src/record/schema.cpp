#include "record/schema.h"

/**
 * schema序列化
 */
uint32_t Schema::SerializeTo(char *buf) const {
	uint32_t offset = 0;

	// schema检测
	MACH_WRITE_UINT32(buf+offset, SCHEMA_MAGIC_NUM);
	offset += sizeof(uint32_t);

	// 先将column数目存入内存
  uint32_t column_count = GetColumnCount();
	MACH_WRITE_UINT32(buf+offset, column_count);
	offset += sizeof(uint32_t);

	// 依次将column序列化
	for (uint32_t i = 0; i < column_count; ++i) {
		// 调用column的serializeto函数, 并更新offset
		offset += columns_[i]->SerializeTo(buf+offset); 
	}

	// 将is_manage序列化
	MACH_WRITE_TO(bool, buf+offset, is_manage_);
	offset += sizeof(bool);

  return offset;
}


/**
 * schema序列化占用内存大小
 */
uint32_t Schema::GetSerializedSize() const {
	uint32_t size = 0;
	size += 2*sizeof(uint32_t);
  for (uint32_t i = 0; i < columns_.size(); ++i) {
		size += columns_[i]->GetSerializedSize();
	}
	size += sizeof(bool);
  return size;
}

/**
 * schema反序列化
 */
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
	if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in schema deserialize." 									 << std::endl;
  }
	
	uint32_t offset = 0;

	// schema 检测
	uint32_t schema_magic_num = MACH_READ_UINT32(buf+offset);
	ASSERT(schema_magic_num == SCHEMA_MAGIC_NUM, "This is not a schema");
	offset += sizeof(uint32_t);

	// column数目反序列化
	uint32_t column_num = MACH_READ_UINT32(buf+offset);
	offset += sizeof(uint32_t);

	// columns反序列化
	std::vector<Column*> columns;
	columns.resize(column_num);
	for (uint32_t i=0; i < column_num; ++i) {
		offset += Column::DeserializeFrom(buf+offset, columns[i]);
	}

	// is_manage_序列化
	bool is_manage = MACH_READ_FROM(bool, buf+offset);
	offset += sizeof(bool);

	schema = new Schema(columns, is_manage);

  return offset;
}