/*
 * Copyright (C) 2017, Yeolar
 */

#pragma once

#include <string>
#include <map>
#include <set>
#include <vector>
#include <utility>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>

#include "raster/util/Counter.h"
#include "PersistentQueue.h"
#include "QItem.h"

namespace rdd {

class QueueCollection {

friend int group(const std::string &key, const int divisor);
friend int part(const std::string &key, const int divisor);

public:
  QueueCollection (const std::string& queueFolder,
           const size_t maxMemorySize = 1L << 26,
           const size_t maxJournalSize = 1L << 24,
           const long maxItems = LONG_MAX,
           const size_t maxSize = ULONG_MAX,
           const bool keepJournal = false,
           const bool syncJournal = false
          ) :
    queueFolder_(queueFolder), shuttingDown_(false),
    maxMemorySize_(maxMemorySize), maxJournalSize_(maxJournalSize),
    maxItems_(maxItems), maxSize_(maxSize),
    keepJournal_(keepJournal), syncJournal_(syncJournal) {
    pthread_mutex_init(&collection_mutex_, NULL);

  }

  ~QueueCollection() {
    for(std::map<std::string, PersistentQueue*>::iterator it = queues_.begin();
        it != queues_.end(); ++it) {
      delete it->second;
    }
    pthread_mutex_destroy(&collection_mutex_);
  }

  int loadQueues();

  const std::vector<std::string> queueNames() {
    std::vector<std::string> v;

    for(std::map<std::string, PersistentQueue*>::const_iterator it = queues_.begin();
        it != queues_.end(); ++it) {
      v.push_back(it->first);
    }

    return v;
  }

  long currentItems() {
    long items = 0;

    for(std::map<std::string, PersistentQueue*>::const_iterator it = queues_.begin();
        it != queues_.end(); ++it) {
      items += it->second->length();
    }

    return items;
  }

  long totalItems() {
    long items = 0;

    for(std::map<std::string, PersistentQueue*>::const_iterator it = queues_.begin();
        it != queues_.end(); ++it) {
      items += it->second->totalItems();
    }

    return items;
  }

  size_t currentBytes() {
    size_t bytes = 0;

    for(std::map<std::string, PersistentQueue*>::const_iterator it = queues_.begin();
        it != queues_.end(); ++it) {
      bytes += it->second->bytes();
    }

    return bytes;
  }


  bool add(const std::string& key, char *item, int length, int expiry = 0);

  QItem* remove(const std::string& key, bool transaction, bool peek, int timeout = 0);

  bool unremove(const std::string& key, int xid) {
    PersistentQueue *q = queue(key);
    if (q) {
      return q->unremove(xid);
    }

    return false;
  }

  bool confirmRemove(const std::string& key, int xid) {
    PersistentQueue *q = queue(key);
    if (q)
      return q->confirmRemove(xid);

    return false;
  }

  bool flush(const std::string& key) {
    PersistentQueue *q = queue(key);
    if (q) return q->flush();

    return false;
  }

  bool deleteQueue(const std::string& name);

  int flushExpired(const std::string& name) {
    int ret = 0;

    pthread_mutex_lock(&collection_mutex_);
    if (!shuttingDown_) {
      PersistentQueue *q = queue(name);
      if (q) {
        ret = q->flushExpired();
      }
    }
    pthread_mutex_unlock(&collection_mutex_);

    return ret;
  }

  int flushAllExpired() {
    int ret = 0;
    std::vector<std::string> v = queueNames();
    for(std::vector<std::string>::iterator it = v.begin(); it != v.end(); ++it) {
      ret += flushExpired(*it);
    }

    return ret;
  }

  bool shutdown() {
    pthread_mutex_lock(&collection_mutex_);
    if (shuttingDown_)
      return true;

    shuttingDown_ = true;
    for(std::map<std::string, PersistentQueue*>::const_iterator it = queues_.begin();
        it != queues_.end(); ++it) {
      it->second->close();
      delete it->second;
    }
    queues_.clear();
    pthread_mutex_unlock(&collection_mutex_);

    return true;
  }

  std::string stats(const std::string &key);
  std::string stats();

private:
  void addFanoutQueue(const std::string& master, const std::string& name);
  void deleteFanoutQueue(const std::string& master, const std::string& name);
  PersistentQueue* queue(const std::string& name);

  std::string  queueFolder_;

  bool shuttingDown_;
  pthread_mutex_t  collection_mutex_;

  // total items added since the server started up
  Counter totalAdded_;

  // hits/misses on removing items from the queue
  Counter queueHits_;
  Counter queueMisses_;

  std::map<std::string, PersistentQueue*> queues_;
  std::map<std::string, std::set<std::string> > fanout_queues_;

  size_t maxMemorySize_;
  size_t maxJournalSize_;
  long maxItems_;
  size_t maxSize_;
  bool keepJournal_;
  bool syncJournal_;
};

extern int group(const std::string &key, const int divisor);
extern int part(const std::string &key, const int divisor);

}

