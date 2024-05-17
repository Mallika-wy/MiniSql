#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t offset = 0;

	// 序列化field的数目
	uint32_t field_count = GetFieldCount();
	MACH_WRITE_UINT32(buf+offset, field_count);
	offset += sizeof(uint32_t);

	// null bit map
	size_t byte_size = ceil(field_count*1.0/8);
	char *bit_map = new char[byte_size];
	memset(bit_map, 0, byte_size);
	for (int i = 0; i < field_count; i++) {
		if (fields_[i]->IsNull()) {
			bit_map[i/8] |= (1 << ( 7-i%8 ));
		}
	}
	// 将bit map拷贝到内存中,其也是序列化的一部分
	memcpy(buf+offset, bit_map, byte_size);
	// 偏移bit map的长度
	offset += byte_size;
	// 依次序列化field
	for (int i = 0; i < field_count; i++) {
		offset += fields_[i]->SerializeTo(buf+offset);
	}

	delete[] bit_map;

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
	uint32_t offset = 0;
	fields_.clear();

	// 反序列化field数目
	uint32_t field_count = MACH_READ_UINT32(buf);
	offset += sizeof(uint32_t);

	// 反序列化bit_map 
	size_t byte_size = ceil(field_count*1.0/8);
	char *bit_map = new char[byte_size];
	memcpy(bit_map, buf+offset, byte_size);
	offset += byte_size;

	// 反序列化field
	for (int i = 0; i < field_count; i++) {
		Field * field;
		bool is_null = bit_map[i] & (1 << (7-i%8)) != 0;
		offset += Field::DeserializeFrom(buf+offset, schema->GetColumn(i)->GetType(), &field, is_null);
		fields_.push_back(field);
	}

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
	uint32_t field_count = GetFieldCount();
	size_t byte_size = ceil(field_count*1.0/8);
	size += sizeof(uint32_t) + byte_size;
	for (int i = 0; i < field_count; i++) {
		size += fields_[i]->GetSerializedSize();
	}
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
