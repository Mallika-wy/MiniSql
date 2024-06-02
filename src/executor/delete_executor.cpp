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
  Row delete_row;
  RowId delete_rid;
  if(child_executor_->Next(&delete_row,&delete_rid)){
    TableInfo* table_info= nullptr;
    if(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info)!=DB_SUCCESS){
      cout<<"Table not exist"<<endl;
      return false;
    }
    //remove from indexes
    vector<IndexInfo*> indexes;
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes);//add
    for(auto index:indexes){
      vector<uint32_t>column_ids;
      vector<Column*> columns=index->GetIndexKeySchema()->GetColumns();
      //find id in the table schema
      for(auto column:columns){
        uint32_t column_id;
        if(table_info->GetSchema()->GetColumnIndex(column->GetName(),column_id)==DB_SUCCESS)
          column_ids.push_back(column_id);
      }
      vector<Field>fields;
      for(auto column_id:column_ids)
        fields.push_back(*delete_row.GetField(column_id));
      Row index_row(fields);
      RowId index_row_id;
      index->GetIndex()->RemoveEntry(index_row,delete_rid,exec_ctx_->GetTransaction());
    }
    //delete from table
    table_info->GetTableHeap()->ApplyDelete(delete_rid,exec_ctx_->GetTransaction());
    return true;
  }
  return false;
}