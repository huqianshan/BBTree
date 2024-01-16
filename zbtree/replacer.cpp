

#include "replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages)
    : dummy_(static_cast<frame_id_t>(num_pages)) {
  lru_list_ = new Node[num_pages + 1];
  for (frame_id_t i = 0; i <= dummy_; ++i) {
    lru_list_[i].next_id_ = i;
    lru_list_[i].prev_id_ = i;
  }
  size_ = 0;
}

LRUReplacer::~LRUReplacer() { delete[] lru_list_; }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (size_ == 0) {
    return false;
  }
  *frame_id = lru_list_[dummy_].prev_id_;
  Invalidate(*frame_id);
  size_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (!IsValid(frame_id)) {
    return;
  }
  Invalidate(frame_id);
  size_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (IsValid(frame_id)) {
    return;
  }
  // insert node
  Add(frame_id);
  size_++;
}

size_t LRUReplacer::Size() { return size_; }
/**
 * @brief
 *
 * @param frame_id
 * @return true: frame_id is in already unpined and in replacer
 * @return false: frame_id is pinned and not in replacer
 */
bool LRUReplacer::IsValid(frame_id_t frame_id) const {
  return lru_list_[frame_id].next_id_ != frame_id;
}

void LRUReplacer::Add(frame_id_t frame_id) {
  lru_list_[frame_id].next_id_ = lru_list_[dummy_].next_id_;
  lru_list_[frame_id].prev_id_ = dummy_;
  lru_list_[lru_list_[dummy_].next_id_].prev_id_ = frame_id;
  lru_list_[dummy_].next_id_ = frame_id;
}

void LRUReplacer::Invalidate(frame_id_t frame_id) {
  lru_list_[lru_list_[frame_id].prev_id_].next_id_ =
      lru_list_[frame_id].next_id_;
  lru_list_[lru_list_[frame_id].next_id_].prev_id_ =
      lru_list_[frame_id].prev_id_;
  lru_list_[frame_id].prev_id_ = frame_id;
  lru_list_[frame_id].next_id_ = frame_id;
}

FIFOReplacer::FIFOReplacer(size_t num_pages) {
  fifo_list_ = new std::vector<frame_id_t>(num_pages);
  for (frame_id_t i = 0; i < num_pages; i++) {
    fifo_list_->at(i) = 0;
  }
  cur_size_ = fifo_head_ = fifo_tail_ = 0;
  max_size_ = num_pages;
}

FIFOReplacer::~FIFOReplacer() { delete fifo_list_; }

bool FIFOReplacer::Add(frame_id_t frame_id) {
  if (cur_size_ == max_size_) {
    return false;
  }
  fifo_list_->at(fifo_head_) = frame_id;
  fifo_head_ = (fifo_head_ + 1) % max_size_;
  cur_size_++;
  return true;
}

/**
 * @brief only return frame_id, not remove it from replacer
 *
 * @param frame_id
 * @return false if replacer is empty, and replacer is not full
 */
bool FIFOReplacer::Victim(frame_id_t *frame_id) {
  if (cur_size_ == 0) {
    return false;
  }
  *frame_id = fifo_list_->at(fifo_tail_);
  fifo_tail_ = (fifo_tail_ + 1) % max_size_;
  cur_size_--;
  return true;
}

bool FIFOReplacer::ToVictim(frame_id_t *frame_id) {
  if (cur_size_ == 0 || cur_size_ != max_size_) {
    return false;
  }
  *frame_id = fifo_list_->at(fifo_tail_);
  // fifo_tail_ = (fifo_tail_ + 1) % max_size_;
  // cur_size_--;
  return true;
}

bool FIFOReplacer::Remove(frame_id_t frame_id) {
  if (cur_size_ == 0) {
    return false;
  }
  auto to_remove = fifo_list_->at(fifo_tail_);
  if (to_remove != frame_id) {
    return false;
  }
  fifo_tail_ = (fifo_tail_ + 1) % max_size_;
  cur_size_--;
  return true;
}

void FIFOReplacer::Pin(frame_id_t frame_id) { return; }

void FIFOReplacer::Unpin(frame_id_t frame_id) {}

size_t FIFOReplacer::Size() { return cur_size_; }
/**
 * @brief
 *
 * @param frame_id
 * @return true: frame_id is in already unpined and in replacer
 * @return false: frame_id is pinned and not in replacer
 */
bool FIFOReplacer::IsValid(frame_id_t frame_id) const { return true; }

void FIFOReplacer::Invalidate(frame_id_t frame_id) { return; }

void FIFOReplacer::Print() {
  std::cout << "fifo_head_: " << fifo_head_ << std::endl;
  std::cout << "fifo_tail_: " << fifo_tail_ << std::endl;
  std::cout << "cur_size_: " << cur_size_ << std::endl;
  std::cout << "max_size_: " << max_size_ << std::endl;
  std::cout << "fifo_list_: ";
  for (auto i = 0; i < max_size_; i++) {
    std::cout << fifo_list_->at(i) << " ";
  }
  std::cout << std::endl;
}

FIFOBacthReplacer::FIFOBacthReplacer(size_t num_pages) {
  // fifo_list_ = new std::vector<frame_id_t>(num_pages);
  head_ = new Item(-1, -1, nullptr);
  tail_ = new Item(-1, -1, nullptr);

  head_->next_ = tail_;
  tail_->prev_ = head_;

  max_size_ = num_pages;
  cur_size_ = 0;
}

FIFOBacthReplacer::~FIFOBacthReplacer() {
  while (head_) {
    Item *temp = head_;
    head_ = head_->next_;
    delete temp;
  }
}

bool FIFOBacthReplacer::Add(frame_id_t frame_id, u32 length, char *data) {
  if (cur_size_ == max_size_) {
    return false;
  }
  Item *item = new Item(frame_id, length, data);
  item->prev_ = head_;

  item->next_ = head_->next_;
  head_->next_->prev_ = item;
  head_->next_ = item;

  cur_size_++;
  return true;
};

bool FIFOBacthReplacer::IsFull() { return cur_size_ == max_size_; }

bool FIFOBacthReplacer::Victim(Item **item) {
  if (cur_size_ == 0) {
    return false;
  }

  Item *temp = tail_->prev_;
  temp->prev_->next_ = tail_;
  tail_->prev_ = temp->prev_;
  *item = temp;

  cur_size_--;
  return true;
}

u64 FIFOBacthReplacer::Size() { return cur_size_; }

void FIFOBacthReplacer::Print() {
  Item *temp = head_;
  while (temp) {
    std::cout << temp->frame_id_ << " ";
    temp = temp->next_;
  }
  std::cout << std::endl;
}