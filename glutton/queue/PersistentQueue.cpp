/*
 * Copyright (C) 2017, Yeolar
 */

#include "PersistentQueue.h"
#include <assert.h>

using namespace std;

namespace rdd {

void PersistentQueue::doAdd(QItem *item) {
  discardExpired(maxExpireSweep_);

  totalItems_ += 1;
  queueSize_ += item->getLength();
  queueLength_ += 1;
  if (!journal_->inReadBehind()) {
    queue_.push_back(item);
    memoryBytes_ += item->getLength();
  } else {
    delete(item);
  }

  //debug_out( "ADD# qsize/msize: (%lu / %lu)", queueSize_, maxMemorySize_);
}

QItem* PersistentQueue::doRemove(bool transaction) {
  discardExpired(maxExpireSweep_);

  if (queue_.empty()) return NULL;

  QItem *item = queue_.front();
  queue_.pop_front();

  int len = item->getLength();
  queueSize_ -= len;
  memoryBytes_ -= len;
  queueLength_ -= 1;
  int xid = transaction ? nexXid() : 0;

  fillReadBehind();
  currentAge_ = timeInMilliseconds() - item->getAddTime();
  if (transaction) {
    item->setXid(xid);
    openTransactions_[xid] = item;
  }

  //debug_out( "REMOVE# qsize/msize: (%lu / %lu)", queueSize_, maxMemorySize_);
  return item;
}

bool PersistentQueue::doUnremove(int xid) {
  std::map<int, QItem*>::iterator it = openTransactions_.find(xid);
  if (it != openTransactions_.end()) {
    queueLength_ += 1;
    queueSize_ += it->second->getLength();
    queue_.push_front(it->second);
    memoryBytes_ += it->second->getLength();
    openTransactions_.erase(it);
    return true;
  }
  return false;
}

void PersistentQueue::fillReadBehind() {
  while(keepJournal_ && journal_->inReadBehind() && memoryBytes_ < maxMemorySize_) {
    journal_->fillReadBehind(fillReadBehind_callback, this);
    if (!journal_->inReadBehind()) {
      //notice_out( "Coming out of read-behind for queue '%s'", name_.c_str());
    }
  }
}

const deque<QItem *> PersistentQueue::openTransactionQItems() {
  vector<int> v = openTransactionIds();
  deque<QItem *> q;
  for(vector<int>::const_iterator it = v.begin(); it != v.end(); ++it) {
    q.push_back(openTransactions_[*it]);
  }
  return q;
}

bool PersistentQueue::add(char *value, int length, int64_t expiry) {
  pthread_mutex_lock(&queue_mutex_);
  if (closed_ || (unsigned)length > maxItemSize_) {
    pthread_mutex_unlock(&queue_mutex_);
    return false;
  }

  while(queueLength_ >= maxItems_ || queueSize_ >= maxSize_) {
    if (!discardOldWhenFull_) {
      pthread_mutex_unlock(&queue_mutex_);
      return false;
    }
    remove(false);
    totalDiscarded_ += 1;
    if (keepJournal_) journal_->remove();
  }

  int64_t now = timeInMilliseconds();
  QItem *item = new QItem(now, adjustExpiry(now, expiry), value, length, 0);
  //debug_out( "#PersistentQueue::add# new(\033[31m%p\033[0m)\n", item);

  if (keepJournal_ && !journal_->inReadBehind()) {
    if (journal_->size() > maxJournalSize_ * maxJournalOverflow_ &&
      queueSize_ < maxJournalSize_) {
      //notice_out( "Rolling journal file for '%s' (qsize=%d)", name_.c_str(), queueSize_);
      deque<QItem *> q = openTransactionQItems();
      journal_->roll(xidCounter_, q, queue_);
    }

    if (queueSize_ >= maxMemorySize_) {
      //notice_out( "Dropping to read-behind for queue '%s' (%lu bytes)", name_.c_str(), queueSize_);
      journal_->startReadBehind();
    }
  }
  if (keepJournal_) journal_->add(*item);

  doAdd(item);

  if(waiters_ > 0)
    pthread_cond_signal(&queue_cond_);

  pthread_mutex_unlock(&queue_mutex_);
  return true;
}

QItem* PersistentQueue::removeReact(bool transaction, int timeout) {
  QItem *item = NULL;
  int rc = 0;
  struct timespec ts;

  clock_gettime(CLOCK_REALTIME, &ts);
  if(timeout) {
    ts.tv_sec += timeout/1000;
    ts.tv_nsec += (timeout % 1000) * 1000000;
  }

  pthread_mutex_lock(&queue_mutex_);
  waiters_++;
  while(timeout != 0 && !closed_ && queueLength_ == 0 && rc == 0)
      rc = pthread_cond_timedwait(&queue_cond_, &queue_mutex_, &ts);
  if(timeout == 0 || (!closed_ && rc == 0))
    item = remove(transaction);
  waiters_--;
  pthread_mutex_unlock(&queue_mutex_);

  return item;
}

// called by add() and removeReact(), cannot hold the mutex
QItem* PersistentQueue::remove(bool transaction) {
  if (closed_ || paused_ || queueLength_ == 0) {
    return NULL;
  }
  QItem *item = doRemove(transaction);

  if (keepJournal_) {
    if (transaction) journal_->removeTentative();
    else journal_->remove();

    if (queueLength_ == 0 && journal_->size() >= maxJournalSize_) {
      //notice_out( "Rolling journal file for '%s' (qsize=%d)", name_.c_str(), queueSize_);
      deque<QItem *> q = openTransactionQItems();
      journal_->roll(xidCounter_, q, queue_);
    }
  }

  return item;
}

bool PersistentQueue::replayJournal() {
  if (!keepJournal_)
    return false;

  //notice_out( "Replaying transaction journal for '%s'", name_.c_str());
  xidCounter_ = 0;
  journal_->replay(name_, replay_callback, this);

  //notice_out( "Finished transaction journal for '%s' (%d items, %lu bytes)", name_.c_str(), queueLength_, journal_->size());
  journal_->open();

  vector<int> v = openTransactionIds();
  for(vector<int>::const_iterator it = v.begin(); it != v.end(); ++it) {
    journal_->unremove(*it);
    doUnremove(*it);
  }

  return true;
}

vector<string> PersistentQueue::dumpStats() {
  std::vector<std::string> stats;
  char str[512];

  sprintf(str, "STAT queue_%s_items %ld", name_.c_str(), length());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_bytes %ld", name_.c_str(), bytes());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_total_items %ld", name_.c_str(), totalItems());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_logsize %ld", name_.c_str(), journalSize());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_expired_items %ld", name_.c_str(), totalExpired());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_mem_items %ld", name_.c_str(), memoryLength());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_mem_bytes %ld", name_.c_str(), memoryBytes());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_age %ld", name_.c_str(), currentAge());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_discarded %ld", name_.c_str(), totalDiscarded());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_waiters %d", name_.c_str(), waiterCount());
  stats.push_back(str);
  sprintf(str, "STAT queue_%s_open_transactions %d", name_.c_str(), openTransactionCount());
  stats.push_back(str);

  return stats;
}

bool PersistentQueue::close(bool flush) {
  pthread_mutex_lock(&queue_mutex_);
  closed_ = true;
  if (keepJournal_)
    journal_->close();
  if (waiters_ > 0)
    pthread_cond_broadcast(&queue_cond_);

  while (!queue_.empty()) {
    QItem *item = queue_.front();
    queue_.pop_front();
    delete(item);
    //debug_out( "#PQ# close_delete(\033[32m%p\033[0m)\n", item);
  }
  queueSize_ = memoryBytes_ = queueLength_ = 0;
  pthread_mutex_unlock(&queue_mutex_);

  return true;
}


void fillReadBehind_callback(QItem *item, void *context)
{
  assert(item);
  PersistentQueue *q = (PersistentQueue *)context;
  q->queue_.push_back(item);
  q->memoryBytes_ += item->getLength();
}

void replay_callback(char opcode, QItem *item, int xid, void *context)
{
  PersistentQueue *q = (PersistentQueue *)context;

  switch(opcode) {
  case Journal::CMD_ADD:
  case Journal::CMD_ADDX:
  case Journal::CMD_ADD_XID:
    assert(item);
    q->doAdd(item);
    if (!q->journal_->inReadBehind() && q->queueSize_ >= q->maxMemorySize_) {
      //notice_out( "Dropping to read-behind for queue '%s' (%lu bytes)", q->name_.c_str(), q->queueSize_);
      q->journal_->startReadBehind();
    }
    break;
  case Journal::CMD_REMOVE:
    assert(!item);
    item = q->doRemove(false);
    delete(item);
    //debug_out( "#replay:REMOVE# delete(\033[32m%p\033[0m)\n", item);
    break;
  case Journal::CMD_REMOVE_TENTATIVE:
    assert(!item);
    q->doRemove(true);
    //item holded by hash
    break;
  case Journal::CMD_SAVE_XID:
    assert(!item);
    q->xidCounter_ = xid;
    break;
  case Journal::CMD_UNREMOVE:
    assert(!item);
    q->doUnremove(xid);
    break;
  case Journal::CMD_CONFIRM_REMOVE:
  {
    assert(!item);
    std::map<int, QItem*>::iterator it = q->openTransactions_.find(xid);
    if (it != q->openTransactions_.end()) {
      delete(it->second);
      //debug_out( "#replay:CONFIRM_REMOVE# delete(\033[32m%p\033[0m)\n", item);
      q->openTransactions_.erase(xid);
    }
    break;
  }
  default:
    if (item) delete(item);
    //erro_out( "Unexpected item in journal: %d", opcode);
    break;
  }
}

}

