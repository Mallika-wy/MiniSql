#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetKeySize(key_size);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
    return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
    *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
    for (int i = 0; i < GetSize(); ++i) {
        if (ValueAt(i) == value)
            return i;
    }
    return -1;
}

void *InternalPage::PairPtrAt(int index) {
    return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    //二分
    int left = 1;
    int right = GetSize() - 1;
    while (left <= right) {
        int mid = (left+right)/2;
        if (KM.CompareKeys(KeyAt(mid), key) > 0) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    int target_index = left;
    // 满足key(i-1) <= subtree(value(i)) < key(i)
    return ValueAt(target_index - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetSize(2);
    SetValueAt(0,old_value);
    SetKeyAt(1,new_key);
    SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    int insert_index = ValueIndex(old_value);  // 得到 =old_value 的下标
    // 下标存在
    insert_index++;
    for (int i = GetSize(); i > insert_index; i--) {
        PairCopy(PairPtrAt(i), PairPtrAt(i - 1), 1);
    }
    SetKeyAt(insert_index,new_key);
    SetValueAt(insert_index,new_value);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int start_index = (GetSize() + 1) / 2;
    int move_num = GetSize() - start_index;
    recipient->CopyNFrom(PairPtrAt(start_index), move_num, buffer_pool_manager);
    IncreaseSize(-move_num);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int size_ = GetSize();
    //复制到当前page的最后一个之后的空间
    memcpy(PairPtrAt(size_), src, size * pair_size);
    // 修改array中的value的parent page id，其中array范围为[GetSize(), GetSize() + size)
    for (int i = GetSize(); i < GetSize() + size; i++) {
        // ValueAt(i)得到指向的孩子结点的page_id
        Page *child_page = buffer_pool_manager->FetchPage(ValueAt(i));
        auto *child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
        // their parents page now changes to me.
        child_node->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    IncreaseSize(-1);
    for (int i = index; i < GetSize(); i++) {
        PairCopy(PairPtrAt(i), PairPtrAt(i + 1), 1);
    }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    SetSize(0);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(0, middle_key);  // 将分隔key设置在0的位置
    recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(0, middle_key);
    recipient->CopyLastFrom(KeyAt(0),ValueAt(0), buffer_pool_manager);
    Remove(0);  // 函数复用
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int current_size = GetSize();
    SetKeyAt(current_size, key);
    SetValueAt(current_size, value);
    // 更新被移动条目的父页ID
    Page *page = buffer_pool_manager->FetchPage(value);
    auto *child_page = reinterpret_cast<InternalPage *>(page->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);

    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
    IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    for (int i = GetSize() - 1; i >= 0; i--) {
        PairCopy(PairPtrAt(i + 1), PairPtrAt(i), 1);
    }

    SetValueAt(0, value);
    IncreaseSize(1);
    // update parent page id of child page
    Page *child_page = buffer_pool_manager->FetchPage(ValueAt(0));
    auto *child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}