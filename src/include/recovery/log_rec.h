#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility> //工具类:提供了一组用于处理各种任务的实用模板类和函数

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert, // 数据的插入操作，表示某条记录已被插入数据库
  kDelete, // 数据的删除操作，表示某条记录已被从数据库中删除
  kUpdate, // 数据的更新操作，表示数据库中某条记录已被修改
  kBegin,  // 事务开始，表示一个新的数据库事务已开始。
  kCommit, // 事务提交，表示事务已成功完成，并且已经被永久写入数据库。
  kAbort,  // 事务终止，表示一个数据库事务由于某种原因无法完成，所进行的任何操作都需要回滚到事务开始之前的状态。
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

struct LogRec {
  LogRec() = default;

  LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn)
      : type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}

  LogRecType type_{LogRecType::kInvalid}; // 记录日志项类型
  lsn_t lsn_{INVALID_LSN}; // 日志序列号(Log Sequence Number),每个操作的唯一标识符
  txn_id_t txn_id_{INVALID_TXN_ID}; // 事务的唯一标识符
  lsn_t prev_lsn_{INVALID_LSN}; // 该日志上一次操作的序列号
  /* used for insert only */
  KeyType ins_key_{};
  ValType ins_val_{};
  /* used for delete only */
  KeyType del_key_{};
  ValType del_val_{};
  /* used for update only */
  KeyType old_key_{};
  ValType old_val_{};
  KeyType new_key_{};
  ValType new_val_{};

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;

  static lsn_t GetAndUpdatePrevLSN(txn_id_t txn_id, lsn_t cur_lsn) {
    auto iter = prev_lsn_map_.find(txn_id);
    auto prev_lsn = INVALID_LSN;
    if (iter != prev_lsn_map_.end()) {
      prev_lsn = iter->second;
      iter->second = cur_lsn;
    } else {
      prev_lsn_map_.emplace(txn_id, cur_lsn);
    }
    return prev_lsn;
  }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

// 智能指针,易于管理,避免内存相关的错误,自动释放内存
typedef std::shared_ptr<LogRec> LogRecPtr;

// 每一种日志的书写,都是新建一个LogRec对象,根据日志的类型,想日志对象中的数据添值
// 比较关键的是日志所在事务的标号,该日志的标号,对数据库修改前后的变化
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  auto log = std::make_shared<LogRec>(LogRecType::kInsert, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->ins_key_ = std::move(ins_key);
  log->ins_val_ = ins_val;
  return log;
}

static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  auto log = std::make_shared<LogRec>(LogRecType::kDelete, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->del_key_ = std::move(del_key);
  log->del_val_ = del_val;
  return log;
}

static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  auto log = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->old_key_ = std::move(old_key);
  log->old_val_ = old_val;
  log->new_key_ = std::move(new_key);
  log->new_val_ = new_val;
  return log;
}

// 开始也是一种日志
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kBegin, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kCommit, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kAbort, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

#endif  // MINISQL_LOG_REC_H
