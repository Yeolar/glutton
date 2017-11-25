/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <algorithm>
#include <string>
#include <vector>
#include <deque>
#include <map>

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <dirent.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#include "QItem.h"
#include "Journal.h"

namespace rdd {

typedef int (*FUNC)(const std::string &, const int);

class PersistentQueue {
private:
  std::string persistencePath_;
  std::string name_;
  bool groupedFanout_;
  size_t queueSize_;
  long totalItems_;
  long totalExpired_;
  int64_t currentAge_;
  long totalDiscarded_;
  long queueLength_;
  std::deque<QItem *> queue_;
  size_t memoryBytes_;
  bool closed_;
  bool paused_;


  long maxItems_;
  size_t maxSize_;
  size_t maxItemSize_;
  int maxAge_;
  size_t maxJournalSize_;
  size_t maxMemorySize_;
  int maxJournalOverflow_;
  size_t maxJournalSizeAbsolute_;
  bool discardOldWhenFull_;
  bool keepJournal_;
  bool syncJournal_;

  int maxExpireSweep_;

  int xidCounter_;

  Journal *journal_;

  int waiters_;
  pthread_cond_t queue_cond_;
  pthread_mutex_t queue_mutex_;

  std::map<int, QItem*> openTransactions_;

  int  divisor_;
  int remainder_;
  FUNC filter_;

  void doAdd(QItem *item);

  QItem* doPeek() {
    discardExpired(maxExpireSweep_);

    return queue_.empty() ? NULL : queue_.front();
  }

  int nexXid() {
    do {
      xidCounter_ += 1;
    } while(openTransactions_.find(xidCounter_) != openTransactions_.end());

    return xidCounter_;
  }

  int64_t timeInMilliseconds() {
    struct timeval tm;
    gettimeofday(&tm, NULL);
    return tm.tv_sec * 1000 + tm.tv_usec/1000;
  }

  int64_t adjustExpiry(int64_t startingTime, int64_t expiry) {
    if (maxAge_ > 0) {
       int64_t maxExpiry = startingTime + maxAge_;
       if (expiry > 0) return expiry < maxExpiry ? expiry : maxExpiry;
    }
    return expiry;
  }

  int discardExpired(int max) {
    if(queue_.empty() || journal_->isReplaying() || max <= 0)
      return 0;

    int len, num = 0;
    int64_t realExpiry;
    std::deque<QItem *>::iterator it;
    for(it = queue_.begin(); it != queue_.end(); ++it) {
      realExpiry = adjustExpiry(queue_.front()->getAddTime(), queue_.front()->getExpiry());
      if(realExpiry != 0 && realExpiry < timeInMilliseconds()) {
        totalExpired_++;
        len = queue_.front()->getLength();
        queue_.pop_front();
        queueSize_ -= len;
        memoryBytes_ -= len;
        queueLength_--;
        fillReadBehind();
        if(keepJournal_)
          journal_->remove();
        num++;
      }
      else
        break;
    }

    return num;
  }

  QItem* doRemove(bool transaction);

  bool doUnremove(int xid);

  // just called by removeReact() and add()
  QItem* remove(bool transaction = false);

  void fillReadBehind();

  const std::deque<QItem *> openTransactionQItems();

public:
  PersistentQueue(const std::string& persistencePath,
          const std::string& name,
          size_t maxMemorySize = 64L*1024*1024,
          size_t maxJournalSize = 16L*1024*1024,
          long maxItems = LONG_MAX,
          size_t maxSize = ULONG_MAX,
          bool keepJournal = true,
          bool syncJournal = false
          ) :
    persistencePath_(persistencePath), name_(name), groupedFanout_(false), queueSize_(0),
    totalItems_(0), totalExpired_(0), currentAge_(0), totalDiscarded_(0), queueLength_(0),

    memoryBytes_(0), closed_(false), paused_(false),
    maxItems_(maxItems), maxSize_(maxSize), maxItemSize_(1024*1024), maxAge_(0),
    maxJournalSize_(maxJournalSize), maxMemorySize_(maxMemorySize),
    maxJournalOverflow_(10), maxJournalSizeAbsolute_(LONG_MAX),
    discardOldWhenFull_(false), keepJournal_(keepJournal), syncJournal_(syncJournal),

    maxExpireSweep_(INT_MAX), xidCounter_(0), waiters_(0) {

    std::string path = persistencePath_ + name_;
    journal_ = new Journal(path, syncJournal_);

    pthread_cond_init(&queue_cond_, NULL);
    pthread_mutex_init(&queue_mutex_, NULL);
  }

  PersistentQueue(const std::string& persistencePath,
          const std::string& name,
          int  divisor,
          int remainder,
          FUNC func,
          size_t maxMemorySize = 64L*1024*1024,
          size_t maxJournalSize = 16L*1024*1024,
          long maxItems = LONG_MAX,
          size_t maxSize = ULONG_MAX,
          bool keepJournal = true,
          bool syncJournal = false
          ) :
    persistencePath_(persistencePath), name_(name), groupedFanout_(true),
    queueSize_(0), totalItems_(0), totalExpired_(0), currentAge_(0), totalDiscarded_(0), queueLength_(0),

    memoryBytes_(0), closed_(false), paused_(false),
    maxItems_(maxItems), maxSize_(maxSize), maxItemSize_(1024*1024), maxAge_(0),
    maxJournalSize_(maxJournalSize), maxMemorySize_(maxMemorySize),
    maxJournalOverflow_(10), maxJournalSizeAbsolute_(LONG_MAX),
    discardOldWhenFull_(false), keepJournal_(keepJournal), syncJournal_(syncJournal),

    maxExpireSweep_(INT_MAX), xidCounter_(0), waiters_(0),
    divisor_(divisor), remainder_(remainder), filter_(func) {

    std::string path = persistencePath_ + name_;
    journal_ = new Journal(path, syncJournal_);

    pthread_cond_init(&queue_cond_, NULL);
    pthread_mutex_init(&queue_mutex_, NULL);
  }

  ~PersistentQueue() {
    delete journal_;
    pthread_cond_destroy(&queue_cond_);
    pthread_mutex_destroy(&queue_mutex_);
  }

  int openTransactionCount() const {
    return openTransactions_.size();
  }

  std::vector<int> openTransactionIds() {
    std::vector<int> v;

    for(std::map<int, QItem*>::const_iterator it = openTransactions_.begin();
        it != openTransactions_.end(); ++it) {
      v.push_back(it->first);
    }

    sort(v.begin(), v.end(), std::less<int>());

    return v;
  }

  bool access(const std::string &key) {
    return groupedFanout_ ? filter_(key, divisor_) == remainder_ : true;
  }

  long length() {
    long ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = queueLength_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  long totalItems() {
    long ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = totalItems_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  size_t bytes() {
    size_t ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = queueSize_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  size_t journalSize() {
    size_t ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = journal_->size();
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  long totalExpired() {
    long ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = totalExpired_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  int64_t currentAge() {
    int64_t ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = queueSize_ == 0 ? 0 : currentAge_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  long totalDiscarded() {
    long ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = totalDiscarded_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  int waiterCount() {
    int ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = waiters_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  bool isClosed() {
    bool ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = closed_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  size_t memoryLength() {
    size_t ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = queue_.size();
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  size_t memoryBytes() {
    size_t ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = memoryBytes_;
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  bool inReadBehind() {
    bool ret;
    pthread_mutex_lock(&queue_mutex_);
    ret = journal_->inReadBehind();
    pthread_mutex_unlock(&queue_mutex_);
    return ret;
  }

  bool add(char *value, int length, int64_t expiry = 0);

  QItem* peek() {
    QItem *item = NULL;
    pthread_mutex_lock(&queue_mutex_);
    if (closed_ || paused_ || queueLength_ == 0) {

    } else {
      item = doPeek();
    }
    pthread_mutex_unlock(&queue_mutex_);

    return item;
  }

  // for external call
  QItem* removeReact(bool transaction = false, int timeout = 0);

  bool unremove(int xid) {
    bool ret = false;

    pthread_mutex_lock(&queue_mutex_);
    if (!closed_) {
      if (keepJournal_)
        journal_->unremove(xid);
      ret = doUnremove(xid);
    }
    if (waiters_ > 0)
      pthread_cond_signal(&queue_cond_);
    pthread_mutex_unlock(&queue_mutex_);

    return ret;
  }

  bool confirmRemove(int xid) {
    bool ret = false;

    pthread_mutex_lock(&queue_mutex_);
    if (!closed_) {
      if (keepJournal_) journal_->confirmRemove(xid);
      std::map<int, QItem*>::iterator it = openTransactions_.find(xid);
      if (it != openTransactions_.end()) {
        delete(it->second);
        openTransactions_.erase(xid);
        ret = true;
      }
    }
    pthread_mutex_unlock(&queue_mutex_);

    return ret;
  }

  bool flush() {
    while(remove(false)) ;
    return true;
  }

  bool close(bool flush = false) ;

  bool setup() {
    pthread_mutex_lock(&queue_mutex_);
    queueSize_ = 0;
    replayJournal();
    pthread_mutex_unlock(&queue_mutex_);

    return true;
  }

  int flushExpired() {
    pthread_mutex_lock(&queue_mutex_);
    int num = discardExpired(maxExpireSweep_);
    pthread_mutex_unlock(&queue_mutex_);

    return num;
  }

  void destroyJournal() {
    pthread_mutex_lock(&queue_mutex_);

    if (keepJournal_) journal_->erase();

    pthread_mutex_unlock(&queue_mutex_);
  }

  bool replayJournal();

  std::vector<std::string> dumpStats();

  friend void fillReadBehind_callback(QItem *item, void *context);
  friend void replay_callback(char opcode, QItem *item, int xid, void *context);

};

void fillReadBehind_callback(QItem *item, void *context);
void replay_callback(char opcode, QItem *item, int xid, void *context);

}

