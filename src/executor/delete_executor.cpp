//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  static TableInfo* table_info = nullptr;
  static vector<IndexInfo*> indexes;
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
  Row delete_row;
  RowId delete_rid;
  // 从子执行器获取下一行要删除的数据和对应的RowId
  if (child_executor_->Next(&delete_row, &delete_rid)) {
    // 遍历每个索引，删除对应的索引项
    for (auto index : indexes) {
      vector<uint32_t> column_ids;
      vector<Column*> columns = index->GetIndexKeySchema()->GetColumns();
      // 查找索引键在表模式中的列ID
      for (auto column : columns) {
        uint32_t column_id;
        if (table_info->GetSchema()->GetColumnIndex(column->GetName(), column_id) == DB_SUCCESS)
          column_ids.push_back(column_id);
      }
      // 构建索引行
      vector<Field> fields;
      for (auto column_id : column_ids)
        fields.push_back(*delete_row.GetField(column_id));
      Row index_row(fields);
      // 删除索引项
      index->GetIndex()->RemoveEntry(index_row, delete_rid, exec_ctx_->GetTransaction());
    }
    // 从表堆中删除行
    table_info->GetTableHeap()->ApplyDelete(delete_rid, exec_ctx_->GetTransaction());
    // 返回true表示删除成功
    return true;
  }
  // 如果没有要删除的行，则返回false
  return false;
}