#ifndef MINISQL_EXECUTE_ENGINE_H
#define MINISQL_EXECUTE_ENGINE_H

#include <memory>
#include <string>
#include <unordered_map>

#include "common/dberr.h"
#include "common/instance.h"
#include "concurrency/txn.h"
#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/abstract_plan.h"
#include "record/row.h"

extern "C" {
#include "parser/parser.h"
};

/**
 * ExecuteEngine
 */
class ExecuteEngine {
 public:
  ExecuteEngine();

	// ExecuteEngine的析构函数,需要delete所有DBStorageEngine
  ~ExecuteEngine() {
    for (auto it : dbs_) {
      delete it.second;
    }
  }

  /**
   * executor interface
   */
  dberr_t Execute(pSyntaxNode ast);

  dberr_t ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                      ExecuteContext *exec_ctx);

	// 接受dberr，输出错误信息
  void ExecuteInformation(dberr_t result);

 private:
  static std::unique_ptr<AbstractExecutor> CreateExecutor(ExecuteContext *exec_ctx, const AbstractPlanNodeRef &plan);

	// 新建一个Database
	// CREATE DATABASE 数据库名;
  dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);

	// 删除某个数据库
	// DROP DATABASE 数据库名称;
  dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);

	// 打印所有的database
	// SHOW DATABASES;
  dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);

	// 切换数据库
	// USE 数据库名称;
  dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);

	// 打印当前数据库中所有表
	// SHOW TABLES;
  dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);

	// 新建表
	/*
		CREATE TABLE 表名称 (
				列名称1 数据类型,
				列名称2 数据类型,
				...
		);
	*/ 
  dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);

	// 打印所有的indexs
	// show indexes;
  dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);

	// 删除某个索引
	// drop index index_name;
  dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);

	// 可以暂时不实现
  dberr_t ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context);

	// 可以暂时不实现
  dberr_t ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context);

	// 可以暂时不实现
  dberr_t ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);

 private:
	// 只要databases文件夹下有某个数据库文件,在execute_engine初始化的时候就会加载带dbs_中
  std::unordered_map<std::string, DBStorageEngine *> dbs_; /** all opened databases */
  // 如果有多个数据库,可以使用using语句切换,所以说需要有一个变量指定当前有效的数据库
	// 数据库通过string来唯一标识
	std::string current_db_;                                 /** current database */
};

#endif  // MINISQL_EXECUTE_ENGINE_H
