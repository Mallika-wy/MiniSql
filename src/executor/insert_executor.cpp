//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  schema_ = table_info_->GetSchema();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row insert_row;
  RowId insert_rid;
  if (child_executor_->Next(&insert_row, &insert_rid)) {
    vector<IndexInfo *> indexes;
    if (exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes) == DB_SUCCESS) {
      for (auto index : indexes) {
        vector<uint32_t> column_ids;
        vector<Column *> index_columns = index->GetIndexKeySchema()->GetColumns();
        for (auto index_column : index_columns) {
          uint32_t column_id;
          if (table_info_->GetSchema()->GetColumnIndex(index_column->GetName(), column_id) == DB_SUCCESS)
            column_ids.push_back(column_id);
        }
        vector<Field> fields;
        for (auto column_id : column_ids) fields.push_back(*insert_row.GetField(column_id));
        Row index_row(fields);
        vector<RowId> rids;

        if (index->GetIndex()->ScanKey(index_row, rids, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS) {
          if (rids.empty()) break;
          cout << "Duplicated Entry for key " << index->GetIndexName() << endl;
          return false;
        }
      }
    }

    if (table_info_->GetTableHeap()->InsertTuple(insert_row, exec_ctx_->GetTransaction())) {
      Row key_row;
      insert_rid.Set(insert_row.GetRowId().GetPageId(), insert_row.GetRowId().GetSlotNum());
      for (auto info : index_info_) {  // 更新索引
        insert_row.GetKeyFromRow(schema_, info->GetIndexKeySchema(), key_row);
        info->GetIndex()->InsertEntry(key_row, insert_rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
  }
  return false;
}