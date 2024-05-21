#ifndef MINISQL_DBERR_H
#define MINISQL_DBERR_H

enum dberr_t {
  DB_SUCCESS = 0, // 数据库操作成功
  DB_FAILED, // 数据库操作失败
  DB_ALREADY_EXIST, // 数据库已经存在
  DB_NOT_EXIST, // 数据库不存在
  DB_TABLE_ALREADY_EXIST, // 表已存在
  DB_TABLE_NOT_EXIST, // 表不存在
  DB_INDEX_ALREADY_EXIST, // 索引已存在
  DB_INDEX_NOT_FOUND, // 索引未找到
  DB_COLUMN_NAME_NOT_EXIST, // 列名不存在
  DB_KEY_NOT_FOUND, // 键未找到
  DB_QUIT // 数据库退出
};

#endif  // MINISQL_DBERR_H
