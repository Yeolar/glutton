/*
 * Copyright (C) 2017, Yeolar
 */


int main()
{
  string name("qu");
  string msg;
  char tm[128];

  QueueCollection qc("/data0/jianqing/queue");

  msg = "hi, I am coming at ";
  sprintf(tm, "%d", time(NULL));
  msg += tm;
  qc.add(name, const_cast<char *>(msg.c_str()), msg.size(), 0);

  QItem *item = NULL;
  while(item = qc.remove(name, false, false)) {
    if(item) {
      cout<<(*item);
      free(item);
    }
  }
  cout<<qc.stats("qu");

  return 0;
}
