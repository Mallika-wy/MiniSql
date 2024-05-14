#include "page/header_page.h"

#include <cassert>
#include <iostream>

/**
 * Record related
 */
bool HeaderPage::InsertRecord(const std::string &name, const page_id_t root_id) {
  assert(name.length() < 32);
  assert(root_id > INVALID_PAGE_ID);

  int record_num = GetRecordCount();
  int offset = 4 + record_num * 36;
  // check for duplicate name
  if (FindRecord(name) != -1) {
    return false;
  }
  // copy record content
	// c_str()保证末尾加上一个'\0', 所以copy的长度需要加1
  memcpy(GetData() + offset, name.c_str(), (name.length() + 1));
	// 把root_id放置于36字节的最后4个字节，root_id是int32_t类型
  memcpy((GetData() + offset + 32), &root_id, 4);

	// record count加1
  SetRecordCount(record_num + 1);
  return true;
}

bool HeaderPage::DeleteRecord(const std::string &name) {
  int record_num = GetRecordCount();
  assert(record_num > 0);

  int index = FindRecord(name);
  // record does not exist
  if (index == -1) {
    return false;
  }
  int offset = index * 36 + 4;
	// 将以‘GetData() + offset + 36’为起点的长度为‘(record_num - index - 1) * 36’的内存拷贝到
	// 以‘GetData() + offset’为起点的长度为‘(record_num - index - 1) * 36’的内存
	// 如果用memcpy的话这两块内存有重叠，会发生错误
  memmove(GetData() + offset, GetData() + offset + 36, (record_num - index - 1) * 36);

  SetRecordCount(record_num - 1);
  return true;
}

bool HeaderPage::UpdateRecord(const std::string &name, const page_id_t root_id) {
  assert(name.length() < 32);

  int index = FindRecord(name);
  // record does not exist
  if (index == -1) {
    return false;
  }
  int offset = index * 36 + 4;
  // update record content, only root_id
  memcpy((GetData() + offset + 32), &root_id, 4);

  return true;
}

bool HeaderPage::GetRootId(const std::string &name, page_id_t *root_id) {
  assert(name.length() < 32);

  int index = FindRecord(name);
  // record does not exist
  if (index == -1) {
    return false;
  }
  int offset = (index + 1) * 36;
  *root_id = *reinterpret_cast<page_id_t *>(GetData() + offset);

  return true;
}

// 获得Record count数目，data_的前四个字节
int HeaderPage::GetRecordCount() {
  return *reinterpret_cast<int *>(GetData());
}

// 设置Record count
// record count为uint32_t类型的变量，占据data_的前4个字节
void HeaderPage::SetRecordCount(int record_count) {
  memcpy(GetData(), &record_count, 4);
}

int HeaderPage::FindRecord(const std::string &name) {
  int record_num = GetRecordCount();

  for (int i = 0; i < record_num; i++) {
		// 一个数据占据36个字节，所以每次偏移是36
    char *raw_name = reinterpret_cast<char *>(GetData() + (4 + i * 36));
    if (strcmp(raw_name, name.c_str()) == 0) {
      return i;
    }
  }
  return -1;
}