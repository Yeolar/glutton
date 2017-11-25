/*
 * Copyright (C) 2017, Yeolar
 */

#include <stdio.h>
#include <stdlib.h>
#include "QueueCollection.h"

using namespace std;

namespace rdd {

int QueueCollection::loadQueues()
{
  DIR *dir;
  struct dirent *de;

  if ((dir = opendir(queueFolder_.c_str())) == NULL) {
    return -1;
  }
  while ((de = readdir(dir)) != NULL) {
    if(!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..") ||
        strstr(de->d_name, "~~") != NULL)
      continue;
    queue(de->d_name);
  }
  closedir(dir);
  return 0;
}

void QueueCollection::addFanoutQueue(const string& master, const string& name) {
  map<string, set<string> >::iterator it = fanout_queues_.find(master);
  if (it != fanout_queues_.end()) {
    it->second.insert(name);
  } else {
    set<string> slaves;
    slaves.insert(name);
    fanout_queues_.insert(pair<string, set<string> >(master, slaves));
  }
}

void QueueCollection::deleteFanoutQueue(const string& master, const string& name) {
  map<string, set<string> >::iterator it = fanout_queues_.find(master);
  if (it != fanout_queues_.end()) {
    it->second.erase(name);

    if (it->second.empty()) {
      fanout_queues_.erase(it);
    }
  }
}

PersistentQueue* QueueCollection::queue(const string &name) {
  pthread_mutex_lock(&collection_mutex_);
  if (shuttingDown_) {
    pthread_mutex_unlock(&collection_mutex_);
    return NULL;
  }

  PersistentQueue *q;
  map<string, PersistentQueue*>::iterator it = queues_.find(name);
  if (it != queues_.end()) {
    // created queue
    q = it->second;
    pthread_mutex_unlock(&collection_mutex_);
    return q;
  }

  // creating queue
  //debug_out( "Queue %s is created", name.c_str());
  const int BUFLEN = 64;
  char master[BUFLEN], fname[BUFLEN];
  int divisor = 0, remainder = 0;
  bool grouped = false;

  string::size_type pos = name.find('+');
  if (pos != string::npos) {
    char div[BUFLEN], rem[BUFLEN];
    int result = sscanf(name.c_str(), "%[^+]+%*[^@]@%[^#]#%[0-9]#%[0-9]", master, fname, div, rem);
    if(result != 1 && result != 4)
    {
      //notice_out( "Illegal fanout queue %s", name.c_str());
      pthread_mutex_unlock(&collection_mutex_);
      return NULL;
    }

    addFanoutQueue(string(master), name);
    //notice_out( "Fanout queue %s added to %s", name.c_str(), master);
    if(result == 4)
    {
      grouped = true;
      divisor = atoi(div);
      remainder = atoi(rem);
    }
  }

  if(grouped) {
    FUNC func;
    if(strcmp(fname, "group") == 0)
      func = group;
    else
      func = part;

    q = new PersistentQueue(queueFolder_, name, divisor, remainder, func, maxMemorySize_,
                maxJournalSize_, maxItems_, maxSize_, keepJournal_, syncJournal_);
  }
  else
    q = new PersistentQueue(queueFolder_, name, maxMemorySize_, maxJournalSize_,
                maxItems_, maxSize_, keepJournal_, syncJournal_);

  q->setup();
  queues_[name] = q;
  pthread_mutex_unlock(&collection_mutex_);
  return q;
}

bool QueueCollection::add(const string& name, char *item, int length, int expiry) {
  string qname(name), key;
  string::size_type pos = qname.find('$');
  if(pos != string::npos) {
    key = qname.substr(pos + 1);
    qname.erase(pos);
  }

  PersistentQueue *q = queue(qname);
  if (q) {
    int64_t now = time(NULL) * 1000;
    int64_t normalizedExpiry = (expiry == 0 ? 0 : (expiry < 1000000 ? now + expiry : expiry));

    map<string, set<string> >::const_iterator it = fanout_queues_.find(qname);
    if (it != fanout_queues_.end()) {
      set<string>::const_iterator it2;
      for(it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
           map<string, PersistentQueue*>::iterator it3 = queues_.find(*it2);
           if (it3 != queues_.end() && it3->second->access(key)) {
          char *newitem = (char *)malloc(length);
          assert(newitem);
          memcpy(newitem, item, length);
             if (it3->second->add(newitem, length, normalizedExpiry))
               totalAdded_.incr();
           }
      }
    }

    bool result = q->add(item, length, normalizedExpiry);
    if (result) totalAdded_.incr();
    return result;
  }
  return false;
}

QItem* QueueCollection::remove(const string& key, bool transaction, bool peek, int timeout) {
  QItem *item = NULL;
  PersistentQueue *q = queue(key);
  if (q) {
    if (peek)
      return q->peek();
    else {
      if ((item = q->removeReact(transaction, timeout)) == NULL)
         queueMisses_.incr();
      else
         queueHits_.incr();
    }
  } else {
    queueMisses_.incr();
  }

  return item;
}

bool QueueCollection::deleteQueue(const string& name) {
  bool ret = false;
  pthread_mutex_lock(&collection_mutex_);
  if (!shuttingDown_) {
    map<string, PersistentQueue*>::iterator it = queues_.find(name);
    if (it != queues_.end()) {
      it->second->close(true);
      it->second->destroyJournal();
      queues_.erase(it);
      delete(it->second);
      //debug_out( "Queue %s has been deleted", name.c_str());
      ret = true;
    }
    string::size_type pos = name.find('+');
    if (pos != string::npos) {
      string master = name.substr(0, pos);
      deleteFanoutQueue(master, name);
      //notice_out( "Fanout queue %s dropped from %s", name.c_str(), master.c_str());
    }
  }
  pthread_mutex_unlock(&collection_mutex_);

  return ret;
}

string QueueCollection::stats(const string &key) {
  string status, child;
  vector<string> vec;

  pthread_mutex_lock(&collection_mutex_);
     map<string, PersistentQueue*>::const_iterator it1 = queues_.find(key);
  PersistentQueue *q;
     if (it1 != queues_.end() && (q = it1->second)) {
    vec = q->dumpStats();
    map<string, set<string> >::const_iterator it2 = fanout_queues_.find(key);
    if(it2 != fanout_queues_.end()) {
      child = "STAT queue_";
      child += key;
      child += "_children ";
      for(set<string>::const_iterator it3 = it2->second.begin(); it3 != it2->second.end(); ++it3) {
        child += *it3;
        child += ",";
      }
      string::size_type comma = child.rfind(',');
      if(comma != string::npos)
        child.erase(comma);
    }
    vec.push_back(child);
     }

  for(vector<string>::const_iterator it4 = vec.begin(); it4 != vec.end(); ++it4) {
    status += *it4;
    status += "\r\n";
  }
  pthread_mutex_unlock(&collection_mutex_);

  return status;
}

string QueueCollection::stats() {
  string status;
  vector<string> names = queueNames();
  for(vector<string>::const_iterator it = names.begin(); it != names.end(); ++it)
    status += stats(*it);

  return status;
}

int group(const std::string &key, const int divisor) {
  int size = key.size();
  if(size == 0 || divisor == 0)
    return -1;

  uint64_t h = 0;
  for(int i = 0; i < size; i++) {
    h = (h << 7) - h + static_cast<unsigned char>(key[i]) + 987654321L;
    h &= 0x0000ffffffffL;
  }

  return h % divisor;
}

int part(const std::string &key, const int divisor) {
  srand(time(NULL));

  return rand() % divisor;
}

}

