#ifndef MINISQL_RWLATCH_H
#define MINISQL_RWLATCH_H

#include <climits>
#include <condition_variable>
#include <mutex>

#include "macros.h"

/**
 * Reader-Writer latch backed by std::mutex.
 */
class ReaderWriterLatch {
  // 为std::mutex其一个别名：mutex_t
  using mutex_t = std::mutex;
  // 为std::condition_variable其一个别名：cond_t
  // condition_variable是关于同步操作的一个类
  using cond_t = std::condition_variable;
  // UINT_MAX的最大值，最多允许MAX_READERS个读者
  static const uint32_t MAX_READERS = UINT_MAX;

 public:
  ReaderWriterLatch() = default;

  // std::lock_guard是一个模板类，被用于管理一个互斥元（mutex）。
  // 在构造时自动获取（锁定）一个mutex，并在析构时自动释放（解锁）这个mutex。
  // 确保在ReaderWriterLatch对象销毁的过程中，其他线程不能访问该对象的内部数据
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }

  DISALLOW_COPY(ReaderWriterLatch);

  /**
   * Acquire a write latch.
   * 如果有其他的读或者写操作正在进行，当前的写操作会等待
   * 只有当没有其他读写操作正在进行时，才会进行该写操作
   */
  void WLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_) {
      reader_.wait(latch);
    }
    writer_entered_ = true;
    while (reader_count_ > 0) {
      writer_.wait(latch);
    }
  }

  /**
   * Release a write latch.
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
	// 表明此时已经没有写线程
    writer_entered_ = false;
	// 唤醒所有读进程
    reader_.notify_all();
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_ || reader_count_ == MAX_READERS) {
	  // 容量达到上限或者写线程正在进行
	  // 这个线程需要释放latch，等待写线程结束或者读线程有容量
      reader_.wait(latch);
    }
    reader_count_++;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
	// 这样肯定是出现问题了，正常reader_count最大就是MAX_REANERS
	// 这说明一个未被跟踪的读线程尝试去释放一个已满的读锁
    if (reader_count_ == MAX_READERS) {
      ASSERT(false, "RUnlock failed.");
      reader_count_ = 0;
    }
	// writer_entered_为true，意味着有写进程在等待被唤醒
    if (writer_entered_) {
	  // 需要先执行完所有读进程，再执行写进程
      if (reader_count_ == 0) {
        writer_.notify_one();
      }
    } else {
	  // 如果reader_count == MAX_READERS - 1，意味着可能有读线程释放锁，等待被唤醒
      if (reader_count_ == MAX_READERS - 1) {
		// 具体哪一个线程被唤醒取决于线程调度策略，相当于随机唤醒
        reader_.notify_one();
      }
    }
  }

 private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_{0};
  bool writer_entered_{false};
};

#endif  // MINISQL_RWLATCH_H
