//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
  txn_ = exec_ctx_->GetTransaction();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row src_row;
  RowId src_rid;
  if (child_executor_->Next(&src_row, &src_rid)) {
    Row dest_row = GenerateUpdatedTuple(src_row);
    // 更新数据行
    if (!table_info_->GetTableHeap()->UpdateTuple(dest_row, src_rid, txn_)) {
      return false;
    }
    Row src_key_row;
    Row dest_key_row;
    for (auto info : index_info_) {
      src_row.GetKeyFromRow(table_info_->GetSchema(), info->GetIndexKeySchema(), src_key_row);
      dest_row.GetKeyFromRow(table_info_->GetSchema(), info->GetIndexKeySchema(), dest_key_row);
      // 检查是否存在重复索引项
      vector<RowId> rids;
      if (info->GetIndex()->ScanKey(dest_key_row, rids, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS) {
        if (!rids.empty()) {
          cout << "Duplicated Entry for key " << info->GetIndexName() << endl;
          return false;
        }
      }
      // 删除旧的索引项
      info->GetIndex()->RemoveEntry(src_key_row, src_rid, txn_);
      // 插入新的索引项
      info->GetIndex()->InsertEntry(dest_key_row, src_rid, txn_);
    }
    return true;
  }
  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  const auto update_attrs = plan_->GetUpdateAttr();
  Schema *schema = table_info_->GetSchema();
  uint32_t col_count = schema->GetColumnCount();
  std::vector<Field> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(*src_row.GetField(idx));
    } else {
      auto expr = update_attrs.at(idx);
      values.emplace_back(expr->Evaluate(&src_row));
    }
  }
  return Row{values};
}