#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
	this->num_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * 
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
	if (lru_list_.size() == 0) 
		return false;
	*frame_id = lru_list_.back();
	lru_list_.pop_back();
	unpined.erase(*frame_id);
	return true;
}

/**
 * 数据页固定
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
	// 如果已经被钉住，直接return
	if (unpined.find(frame_id) == unpined.end()) 
		return;
	// 此frame被固定，不可被替换
	lru_list_.erase(unpined[frame_id]);
	// 解除标记
	unpined.erase(frame_id);
}

/**
 * 将数据页解除固定，加入到lru_list_中等待被替换，并记录
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
	// 数据页必须已经被pin住
	if (unpined.find(frame_id) != unpined.end())
		return;
	// 根据lru原则，加入到list的最前面
	lru_list_.push_front(frame_id);
	// 记录
	unpined[frame_id] = lru_list_.begin();
}

/**
 * 返回当前LRUReplacer中能够被替换的数据页的数量
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}