#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstring>
#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
	// 数据库文件存在databases中, 在instance.cpp中被指定
  char path[] = "./databases";
	// DIR在dirent.h中被定义, 用于在进行文件系统操作时，表示被打开的目录
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
		// 0777 是一个八进制数字，用于设置新创建的目录的权限
		// 每一位数字都代表一种权限，这三个数字分别代表了文件拥有者、文件所属组的成员和其他用户的权限
		// 例如: 文件的所有者权限: 第一个 7，转为二进制为 111，表示文件的所有者有读（4）、写（2）、执行（1）的权限
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
		// 过滤掉上层文件夹,上上层文件夹,忽略文件三种类型
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
		// 已经存在这个数据库文件,所以init参数为false
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
	switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
			// dynamic_cast 是 C++ 中的一种类型转换操作符
			// 通常用于将基类指针或引用转换为派生类指针或引用
			// 与其他几种类型转换(static_cast、const_cast和reinterpret_cast)相比，dynamic_cast 是运行时（运行时）动态类型检查，因此也是最安全的。
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
		// 对于update，delete，insert plan都需要先执行孩子的，然后再执行自己的
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
		// Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
	// 如果该数据库名字已经存在，返回 derr : DB_ALREADY_EXIST
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
	// 如果数据库不存在的话，就无法删除
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
	// 从物理意义上删除数据库文件
  remove(db_name.c_str());
	// 释放DBStorageEngine内存
  delete dbs_[db_name];
	// 从map中删除键值对
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
	// 按照一定格式输出所有数据库名称
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
	// 首先需要选择好某个数据库
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
	// 从catalog中获取表
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    // 如果操作失败，一般可能就没有表
		cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
	// 按照一定的格式输出表的相关信息
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement .
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
	// 前置约束 current_db存在
	if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
	// 获取catalog_manager,用于create table and index
	auto catalog_manager = context->GetCatalog();
	// 获取table_name
	std::string table_name = ast->child_->val_;
	// 存储column name
	std::vector<std::string> columns_name;
	// 存储column type
	std::unordered_map<std::string, TypeId> columns_type;
	// 存储unique信息
	std::unordered_map<std::string, bool> is_uniques;
	// 存储是否是主键
	std::unordered_map<std::string, bool> is_primarys;
	// 存储len
	std::unordered_map<std::string, int> string_lens;
	// 存储primary_key
	std::vector<std::string> primary_keys;
	// 所有column
	vector<Column*> columns;

	pSyntaxNode column_node = ast->child_->next_->child_;
	while (column_node != nullptr) {
		if (column_node->type_ == kNodeColumnDefinition) {
			// 获取column name 并加入 columns_name
			std::string column_name = column_node->child_->val_;
			columns_name.push_back(column_name);
			// 获取unique信息
			bool is_unique = false;
			if (column_node->val_ != nullptr) {
 				is_unique = (strcmp(column_node->val_, "unique") == 0);
			}
			is_primarys[column_name] = false;
			is_uniques[column_name] = is_unique;
			// 获取type信息
			std::string type_string = column_node->child_->next_->val_;
			TypeId type;
			if (type_string == "int") {
				type = kTypeInt;
			} else if (type_string == "float") {
				type = kTypeFloat;
			} else if (type_string == "char") {
				type = kTypeChar;
				// 获取char类型字符串的长度
				std::string len = column_node->child_->next_->child_->val_;
				int string_len = stoi(len);
				if (string_len <= 0) {
					return DB_FAILED;
				}
				string_lens[column_name] = string_len;
			}
			columns_type[column_name] = type;
		} else {
			pSyntaxNode primary_key_node = column_node->child_;
			// 开始遍历所有的primary_key
			while (primary_key_node != nullptr) {
				string primary_key = primary_key_node->val_;
				// primary_key意味着这个字段是unique的
				is_primarys[primary_key] = true;
				// 添加到primary_keys中,用于index的生成
				primary_keys.push_back(primary_key);
				primary_key_node = primary_key_node->next_;
			}
    }
		column_node = column_node->next_;
	}

	for (int i = 0; i < columns_name.size(); i++) {
		std::string column_name = columns_name[i];
		Column* column;
    if (columns_type[column_name] == kTypeInt) {
      column = new Column(column_name, kTypeInt, i, true, is_uniques[column_name] || is_primarys[column_name]);
		} else if (columns_type[column_name] == kTypeFloat ) {
      column = new Column(column_name, kTypeFloat, i, true, is_uniques[column_name] || is_primarys[column_name]);
		} else if (columns_type[column_name] == kTypeChar) {
      column = new Column(column_name, kTypeChar, string_lens[column_name], i, true, is_uniques[column_name] || is_primarys[column_name]);
		} else {
      cout<<"unknown type"<<endl;
      return DB_FAILED;
    }
    columns.push_back(column);
	}

  Schema *schema = new Schema(columns);
  TableInfo *table_info;
  dberr_t result = catalog_manager->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if(result == DB_TABLE_ALREADY_EXIST){
    return DB_TABLE_ALREADY_EXIST;
  }

  string index_name = table_name+"_primary_key_index";
  IndexInfo *index_info;
  result =  catalog_manager->CreateIndex(table_name, index_name, primary_keys, context->GetTransaction(), index_info, "bptree");
	if (result != DB_SUCCESS) {
		return result;
	}

	for (int i = 0; i < columns_name.size(); i++) {
		if (!is_uniques[columns_name[i]]) continue;
		index_name = table_name + "_unique_index_" + columns_name[i];
		vector<std::string> index_key;
		index_key.push_back(columns_name[i]);
		result =  catalog_manager->CreateIndex(table_name, index_name, index_key, context->GetTransaction(), index_info, "bptree");
		if (result != DB_SUCCESS) 
			return result; 
	}
	return DB_SUCCESS;
}

/**
 * TODO: Student Implement .
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
	// 前置约束 current_db存在
	if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
	// 获取catalog_manager
	auto catalog_manager = context->GetCatalog();
	// 从语法树中获取table_name
	std::string table_name = ast->child_->val_;
	// 获得该数据库中的所有table
	vector<TableInfo*> tables;
	catalog_manager->GetTables(tables);
	// 遍历tables, 找到指定的table
	for (auto table : tables) {
		if (table->GetTableName() == table_name) {
			return catalog_manager->DropTable(table_name);
		}
	}
	return DB_FAILED;
}

/**
 * TODO: Student Implement .
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
	// 前置约束 current_db存在
	if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
	cout << "show all indexs in database " << current_db_ << " : "<< endl;
	// 获取catalog_manager
	auto catalog_manager = context->GetCatalog();
	// 获得该数据库中的所有table
	vector<TableInfo*> tables;
	catalog_manager->GetTables(tables);
	// 遍历tables, 输出index
	for (auto table : tables) {
		cout << "    show all indexs in table " << table->GetTableName() << " : " << endl;
		vector<IndexInfo*> indexs;
		catalog_manager->GetTableIndexes(table->GetTableName(), indexs);
		if (indexs.empty()) {
			cout << "        Table " << table->GetTableName() << " has not a index" << endl;
		}
		for (auto index : indexs) {
			cout << "         " << index->GetIndexName() << endl;
		}
	}

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement .
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
	// 前置约束 current_db存在
	if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
  // 根据语法树获得将要被创建的index的基本信息
	// 1. 获得index_name
	// 2. 获得table_name
	// 3. 获得index_key
	pSyntaxNode tmp = ast->child_;
	std::string index_name = tmp->val_;
	tmp = tmp->next_;
	std::string table_name = tmp->val_;
	tmp = tmp->next_->child_;
  vector<std::string> index_keys;
  while(tmp!= nullptr){
		index_keys.push_back(tmp->val_);
    tmp=tmp->next_;
  }
	// 执行createindex,返回结果
	auto catalog_manager = context->GetCatalog();
	IndexInfo* index_info;
	return catalog_manager->CreateIndex(table_name, index_name, index_keys, 
																			context->GetTransaction(), index_info, "bptree");
}

/**
 * TODO: Student Implement .
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
	// 前置约束 current_db存在
	if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
	// 获得需要删除的index_name
	string index_name = ast->child_->val_;
	// 获取catalog_manager
	auto catalog_manager = context->GetCatalog();
	// 获得该数据库中的所有table
	vector<TableInfo*> tables;
	catalog_manager->GetTables(tables);
	// 遍历tables,寻找有指定index的table
	for (auto table : tables) {
		IndexInfo* index;
		catalog_manager->GetIndex(table->GetTableName(), index_name, index);
		if (index != nullptr) {
			return catalog_manager->DropIndex(table->GetTableName(), index_name);
		}
	}
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
	// 获取文件名
	std::string file_name = ast->child_->val_;
	ifstream in_file(file_name);
	if (!in_file.is_open()) {
		cout << "file : " << file_name << " can't be opened."<<endl;
    return DB_FAILED;
	}

	std::vector<std::string> sql_commands;
	std::string command;
	char ch;
	while (in_file.get(ch)) {
			// 如果字符不是换行符和分号，就将它添加到当前的SQL命令中
			if (ch != '\n' || ch != '\r' || ch != ';') {
				command.push_back(ch);
			}
			
			// 如果字符是分号，那么一条完整的SQL命令已经读取完毕
			if (ch == ';') {
				sql_commands.push_back(std::move(command));
				command.clear();
			}
	}

	for (auto &command : sql_commands) {
		YY_BUFFER_STATE bp = yy_scan_string(command.c_str());
		if (bp == nullptr) {
			LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
			exit(1);
		}
		yy_switch_to_buffer(bp);
		// init parser module
		MinisqlParserInit();
		// parse
		yyparse();
		// parse result handle
		if (MinisqlParserGetError()) {
			// error
			printf("%s\n", MinisqlParserGetErrorMessage());
		}
		auto result = Execute(MinisqlGetParserRootNode());
		MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    // quit condition
    ExecuteInformation(result);
    if (result == DB_QUIT) {
      return DB_QUIT;
    }
	}

	return DB_SUCCESS;
}

/**
 * TODO: Student Implement .
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}
