#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return (sizeof(uint32_t)*3 + table_meta_pages_.size()*(sizeof(table_id_t)+sizeof(page_id_t)) + index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(page_id_t)));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
      if(init){
        catalog_meta_ = CatalogMeta::NewInstance();
        next_table_id_ = 0;
        next_index_id_ = 0;
        Page * catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_->SerializeTo(catalog_meta_page->GetData());
        buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
      }else{
        Page * catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData());
        buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
        for(auto ite:catalog_meta_->table_meta_pages_){
          if(LoadTable(ite.first,ite.second) != DB_SUCCESS){
            LOG(WARNING)<<"table load failed"<<std::endl;
          }
        }
        for(auto ite:catalog_meta_->index_meta_pages_){
          if(LoadIndex(ite.first,ite.second) != DB_SUCCESS){
            LOG(WARNING)<<"index load failed"<<std::endl;
          }
        }
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
      }
//    ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) != table_names_.end()){
    return DB_TABLE_ALREADY_EXIST;
  }
  //if certain table name doesn't exist,create a new table
  page_id_t new_page_id;
  TableHeap* table = TableHeap::Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_);
  Page* table_meta_page = buffer_pool_manager_->NewPage(new_page_id);
  page_id_t root_page_id = table->GetFirstPageId();
  TableMetadata* table_meta_data = TableMetadata::Create(new_page_id,table_name,root_page_id,schema);
  table_meta_data->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(new_page_id,true);

  //load information of the table
  table_info = TableInfo::Create();
  table_info->Init(table_meta_data, table);

  //store information into tables
  table_names_[table_name]=next_table_id_;
  tables_[next_table_id_] = table_info;
  this->catalog_meta_->table_meta_pages_[next_table_id_]=new_page_id;
  next_table_id_++;

  Page* catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  //if can't find certain table, return DB_TABLE_NOT_EXIST  
  if(table_names_.find(table_name)==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.find(table_names_.find(table_name)->second)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto t:tables_){
    tables.push_back(t.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  dberr_t dberr= GetIndex(table_name,index_name,index_info);
  if(dberr!=DB_INDEX_NOT_FOUND) return dberr==DB_SUCCESS?DB_INDEX_ALREADY_EXIST:dberr;
  index_id_t index_id=this->next_index_id_++;
  table_id_t table_id=table_names_[table_name];
  TableInfo* table_info=tables_[table_id];
  uint32_t col_index;
  std::vector<uint32_t> key_map;
  for(auto key:index_keys){
    if(table_info->GetSchema()->GetColumnIndex(key,col_index)==DB_COLUMN_NAME_NOT_EXIST)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(col_index);
  }
  IndexMetadata* meta_data=IndexMetadata::Create(index_id,index_name,table_id,key_map);
  index_info=IndexInfo::Create();
  index_info->Init(meta_data,table_info,buffer_pool_manager_);
  //将table中原有的数据插入索引中
  auto itr=table_info->GetTableHeap()->Begin(txn);
  vector<uint32_t> column_ids;
  vector<Column *> columns = index_info->GetIndexKeySchema()->GetColumns();
  for (auto column : columns) {
    uint32_t column_id;
    if (table_info->GetSchema()->GetColumnIndex(column->GetName(), column_id) == DB_SUCCESS)
      column_ids.push_back(column_id);
  }
  for(;itr!=table_info->GetTableHeap()->End();itr++){
    Row tmp=*itr;
    vector<Field> fields;
    for (auto column_id : column_ids) fields.push_back(*tmp.GetField(column_id));
    Row index_row(fields);
    index_info->GetIndex()->InsertEntry(index_row,tmp.GetRowId(),txn);
  }
  //找个page写元数据
  page_id_t page_id;
  Page* page=buffer_pool_manager_->NewPage(page_id);
  meta_data->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id,true);
  //更新table_names&indexes
  index_names_[table_name][index_name]=index_id;
  indexes_[index_id]=index_info;
  //更新catalog_meta_data
  catalog_meta_->index_meta_pages_[index_id]=page_id;
  return DB_SUCCESS;
}
/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it=index_names_.find(table_name);
  if(it==index_names_.end()) return DB_INDEX_NOT_FOUND;
  if(it->second.find(index_name)==it->second.end()) return DB_INDEX_NOT_FOUND;
  index_info=indexes_.at(it->second.find(index_name)->second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto table_index_name=index_names_.find(table_name);
  if(table_index_name==index_names_.end()){
    return DB_INDEX_NOT_FOUND;
  }
  for(auto i:table_index_name->second){
    indexes.push_back(indexes_.at(i.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto tb=table_names_.find(table_name);
  if(tb==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  //delete index
  if(index_names_.find(table_name)!=index_names_.end()){
    vector<IndexInfo*> indexes;
    GetTableIndexes(table_name,indexes);
    for(int i=0;i<indexes.size();i++)
      DropIndex(table_name,indexes[i]->GetIndexName());
  }
  //delete certain table from data structure
  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_.erase(table_id);
  //delete page from the buffer
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto table_index_name=index_names_.find(table_name);
  if(table_index_name==index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto ind=index_names_[table_name].find(index_name);
  if(ind==index_names_[table_name].end()){
    return DB_INDEX_NOT_FOUND;
  }
  //get the id of the index with certain name, erase certain index
  index_id_t index_id = index_names_[table_name][index_name];
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  //delete the meta data from catalog meta 
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  //if the table already exists, the program will go wrong
  if(tables_.find(table_id) != tables_.end()){
      return DB_FAILED;
  }
  //get the page to store the table
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  TableMetadata* meta_data;
  TableMetadata::DeserializeFrom(page->GetData(),meta_data);
  TableHeap* table_heap=TableHeap::Create(buffer_pool_manager_,meta_data->GetFirstPageId(),meta_data->GetSchema(),log_manager_,lock_manager_);
  //create table info to be added into tables
  TableInfo* table_info=TableInfo::Create();
  table_info->Init(meta_data,table_heap);
  //add to table and table_name
  table_names_[meta_data->GetTableName()]=table_id;
  tables_[table_id]=table_info;
  
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //get the page from buffer
  if(indexes_.find(index_id) != indexes_.end()){
      return DB_FAILED;
  }
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  //meta_data
  IndexMetadata* meta_data;
  IndexMetadata::DeserializeFrom(page->GetData(),meta_data);
  if(index_id != meta_data->GetIndexId()){
    return DB_FAILED;
  }
  IndexInfo* index_info=IndexInfo::Create();
  index_info->Init(meta_data,tables_[meta_data->GetTableId()],buffer_pool_manager_);
  //table_name&index_name
  std::string index_name=meta_data->GetIndexName();
  std::string table_name=tables_[meta_data->GetTableId()]->GetTableName();
  index_names_[table_name][index_name]=index_id;
  indexes_[index_id]=index_info;
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(tables_.find(table_id)==tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info=tables_[table_id];
  return DB_SUCCESS;
}