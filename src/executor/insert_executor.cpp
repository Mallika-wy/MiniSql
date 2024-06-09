//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  string table_name = plan_->GetTableName();
  exec_ctx_->GetCatalog()->GetTable(table_name, table_info_);
  schema_ = table_info_->GetSchema();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_name, index_info_);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  static TableInfo* table_info = nullptr;
  static vector<IndexInfo*> indexes;
  static std::unordered_set<RowId> processed_rids;  // 使用unordered_set记录已处理的RowId
  // 如果表信息和索引信息还没有被获取，进行获取
  if (table_info == nullptr) {
    // 从执行上下文中获取表信息，如果表不存在，则输出错误信息并返回false
    if (exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info) != DB_SUCCESS) {
      cout << "Table not exist" << endl;
      return false;
    }
    // 获取与该表相关的所有索引
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes);
  }
  Row insert_row;
  RowId insert_rid;
  // 从子执行器获取下一行要插入的数据和对应的RowId
  if (child_executor_->Next(&insert_row, &insert_rid)) {
    // 检查是否已经处理过这个RowId
    if (processed_rids.find(insert_rid) != processed_rids.end()) {
      // 已处理，跳过
      return false;
    }
    // 标记这个RowId为已处理
    processed_rids.insert(insert_rid);
    // 遍历每个索引，检查是否存在重复的索引项
    for (auto index : indexes) {
      vector<uint32_t> column_ids;
      vector<Column*> index_columns = index->GetIndexKeySchema()->GetColumns();
      // 查找索引键在表模式中的列ID
      for (auto index_column : index_columns) {
        uint32_t column_id;
        if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), column_id) == DB_SUCCESS)
          column_ids.push_back(column_id);
      }
      // 构建索引行
      vector<Field> fields;
      for (auto column_id : column_ids)
        fields.push_back(*insert_row.GetField(column_id));
      Row index_row(fields);
      // 检查是否存在重复索引项
      vector<RowId> rids;
      if (index->GetIndex()->ScanKey(index_row, rids, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS) {
        if (!rids.empty()) {
          cout << "Duplicated Entry for key " << index->GetIndexName() << endl;
          return false;
        }
      }
    }
    // 插入数据行
    if (table_info->GetTableHeap()->InsertTuple(insert_row, exec_ctx_->GetTransaction())) {
      Row key_row;
      insert_rid.Set(insert_row.GetRowId().GetPageId(), insert_row.GetRowId().GetSlotNum());
      // 更新索引
      for (auto info : index_info_) {
        insert_row.GetKeyFromRow(schema_, info->GetIndexKeySchema(), key_row);
        info->GetIndex()->InsertEntry(key_row, insert_rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
  }

  return false;
}