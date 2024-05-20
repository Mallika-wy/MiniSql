#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};	// 检查点对应的日志序列号
  ATT active_txns_{}; // 映射表，表示在检查点时刻活跃事务的集合。键是事务ID，值是这个事务最后一个日志记录的LSN
  KvDatabase persist_data_{}; // 映射表，表示在检查点时刻已经被持久化到数据库的数据。键是数据项的键，值是数据项的值

	// 一个活跃事务添加到活跃事务集合
  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }
	// 把一个数据项添加到持久化数据集合
  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  void Init(CheckPoint &last_checkpoint) {
		// 检查点包含了已经持久化的数据,检查点所在的日志序列号,在检查点时刻还活跃的事务
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
		// 使用std::move的原因是set,map类似的大型的数据结构,如果使用简单的=赋值,在内存拷贝的时候效率较低
		// std::move通过直接将源对象的资源（通常是堆上的资源）“移动”到目标对象，避免了不必要的拷贝
    active_txns_ = std::move(last_checkpoint.active_txns_);
    data_ = std::move(last_checkpoint.persist_data_);
  }

	// 用于恢复过程的重做阶段
	// 由于幂等性,可以通过日志来将数据库系统从上一个检查点恢复到宕机前的状态
	// 不是幂等性的语句,我们实验不考虑
  void RedoPhase() {
		// 正常情况下,log_recs_如何获得:
		// 在执行一个操作,比如insert,delete,commit,abort等,都会生成一条日志,
		// 这个时候,就会像log_recs中插入一个新信息
		// 正常数据库中都会有很多条日志,所以我想设计的时候应该按时将log_recs中在检查点之前的都删掉
    auto iter = log_recs_.begin();
		// 在检查点之前的,即日志编号在persist_lsn_之前的,都认为正常
    for (; iter != log_recs_.end() && iter->first < persist_lsn_; ++iter) {
    }
		// 从检查点开始,根据日志信息进行重做,本次实验重做仅仅实现了简单内存上的重做,并不和其他模块相关联
    for (; iter != log_recs_.end(); ++iter) {
      auto &log_rec = *(iter->second);
      active_txns_[log_rec.txn_id_] = log_rec.lsn_;
			if (log_rec.type_ == LogRecType::kInvalid) {
				continue;
			} else if (log_rec.type_ == LogRecType::kInsert) {
				data_[log_rec.ins_key_] = log_rec.ins_val_;
			} else if (log_rec.type_ == LogRecType::kDelete) {
				data_.erase(log_rec.del_key_);
			} else if (log_rec.type_ == LogRecType::kUpdate) {
				data_.erase(log_rec.old_key_);
				data_[log_rec.new_key_] = log_rec.new_val_;
			} else if (log_rec.type_ == LogRecType::kBegin) {
				if (active_txns_.find(log_rec.txn_id_) == active_txns_.end()) {
					active_txns_[log_rec.txn_id_] = log_rec.lsn_;
				}
			} else if (log_rec.type_ == LogRecType::kCommit) {
				active_txns_.erase(log_rec.txn_id_);
			} else if (log_rec.type_ == LogRecType::kAbort) {
				Rollback(log_rec.txn_id_);
			} else {
				assert(false); //invalid area
			}
    }
  }

  void UndoPhase() {
		std::vector<txn_id_t> vec_txn;
    for (const auto &[txn_id, _] : active_txns_) {
      vec_txn.push_back(txn_id);
    }
		for (const auto txn_id : vec_txn) {
			Rollback(txn_id);
		}
  }

	// 对未完成的事务进行回滚
  void Rollback(txn_id_t txn_id) {
		// 获得这一事务已经进行到的日志标号
    auto last_log_lsn = active_txns_[txn_id];
    while (last_log_lsn != INVALID_LSN) {
      auto log_rec = log_recs_[last_log_lsn];
			// 根据日志类型进行撤销
			if (log_rec->type_ == LogRecType::kInsert) {
				data_.erase(log_rec->ins_key_);
			} else if (log_rec->type_ == LogRecType::kDelete) {
				data_[log_rec->del_key_] = log_rec->del_val_;
			} else if (log_rec->type_ == LogRecType::kUpdate) {
				data_.erase(log_rec->new_key_);
				data_[log_rec->old_key_] = log_rec->old_val_;	
			}
      last_log_lsn = log_rec->prev_lsn_;
    }
		active_txns_.erase(txn_id);
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
