#include "CKestrel.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/uio.h>

#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>

#include <malloc.h>

#include <string>
#include "QueueCollection.h"

namespace rdd {

/*
 * forward declarations
 */
static void drive_machine(conn *c);
static int new_socket(struct addrinfo *ai);
static int server_socket(const int port);
static int try_read_command(conn *c);
static int try_read_network(conn *c);

/* stats */
static void stats_init(void);

/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);
static void conn_close(conn *c);
static void conn_init(void);
static bool update_event(conn *c, const int new_flags);
static void complete_nread(conn *c);
static void process_command(conn *c, char *command);
static int transmit(conn *c);
static int ensure_iov_space(conn *c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msghdr(conn *c);

/* time handling */
static void set_current_time(void);  /* update the global variable holding
                    global 32-bit seconds-since-start time
                    (to avoid 64 bit time_t) */

static void conn_free(conn *c);

/** exported globals **/
struct stats stats;
struct settings settings;

/** file scope variables **/
static conn *listen_conn = NULL;
static struct event_base *main_base;
static QueueCollection *qc = NULL;

#define TRANSMIT_COMPLETE   0
#define TRANSMIT_INCOMPLETE 1
#define TRANSMIT_SOFT_ERROR 2
#define TRANSMIT_HARD_ERROR 3

static void stats_init(void) {
  stats.curr_conns = stats.total_conns = stats.conn_structs = 0;
  stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses = 0;
  stats.bytes_read = stats.bytes_written = stats.flush_cmds = 0;
  stats.listen_disabled_num = 0;
  stats.accepting_conns = 1; /* assuming we start in this state. */

  /* make the time we started always be 2 seconds before we really
     did, so time(0) - time.started is never zero.  if so, things
     like 'settings.oldest_live' which act as booleans as well as
     values are now false in boolean context... */
  stats.started = time(0) - 2;
}

static void settings_init(void) {
  settings.access=0755;
  settings.port = 11233;
  /* By default this string should be NULL for getaddrinfo() */
  settings.vardir = "./var";
  settings.inter = NULL;
  settings.maxmembytes = 1L << 26;   /* default is 64MB */
  settings.maxjnlbytes = 1L << 24;   /* default is 16MB */
  settings.maxbytes = ULONG_MAX;
  settings.maxitems = LONG_MAX;
  settings.maxconns = 1024;
  settings.keepjournal = true;
  settings.syncjournal = false;
  settings.verbose = 0;
  settings.socketpath = NULL;     /* by default, not using a unix socket */
#ifdef USE_THREADS
  settings.num_threads = 8;
#else
  settings.num_threads = 1;
#endif
  settings.num_threads++;  /* N workers + 1 dispatcher */
  settings.reqs_per_event = 20;
  settings.backlog = 1024;
}

static void settings_dump(void) {
  printf("\033[32m" PACKAGE " " VERSION "\033[0m\n");
  printf("\033[34m"
    "access:     0%o\n"
    "port:       %d\n"
    "vardir:     %s\n"
    "inter:      %s\n"
    "maxmembytes:  %lu\n"
    "maxjnlbytes:  %lu\n"
    "maxbytes:     %lu\n"
    "maxitems:     %ld\n"
    "maxconns:     %d\n"
    "keepjournal:  %d\n"
    "syncjournal:  %d\n"
    "verbose:    %d\n"
    "socketpath:   %s\n"
    "threads:    %d\n"
    "reqs/event:   %d\n"
    "backlog:    %d\n"
    "\033[0m\n",
    settings.access,
    settings.port,
    settings.vardir,
    settings.inter,
    settings.maxmembytes,
    settings.maxjnlbytes,
    settings.maxbytes,
    settings.maxitems,
    settings.maxconns,
    settings.keepjournal,
    settings.syncjournal,
    settings.verbose,
    settings.socketpath,
    settings.num_threads,
    settings.reqs_per_event,
    settings.backlog
    );
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(conn *c)
{
  struct msghdr *msg;

  assert(c != NULL);

  if (c->msgsize == c->msgused) {
    msg = (msghdr*)realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
    if (! msg)
      return -1;
    c->msglist = msg;
    c->msgsize *= 2;
  }

  msg = c->msglist + c->msgused;

  /* this wipes msg_iovlen, msg_control, msg_controllen, and
     msg_flags, the last 3 of which aren't defined on solaris: */
  memset(msg, 0, sizeof(struct msghdr));

  msg->msg_iov = &c->iov[c->iovused];

  c->msgbytes = 0;
  c->msgused++;

  return 0;
}


/*
 * Free list management for connections.
 */

static conn **freeconns;
static int freetotal;
static int freecurr;

static void conn_init(void) {
  freetotal = 200;
  freecurr = 0;
  if ((freeconns = (conn **)malloc(sizeof(conn *) * freetotal)) == NULL) {
    //erro_out( "malloc() %m\n");
  }
  return;
}

/*
 * Returns a connection from the freelist, if any. Should call this using
 * conn_from_freelist() for thread safety.
 */
conn *do_conn_from_freelist() {
  conn *c;

  if (freecurr > 0) {
    c = freeconns[--freecurr];
  } else {
    c = NULL;
  }

  return c;
}

/*
 * Adds a connection to the freelist. 0 = success. Should call this using
 * conn_add_to_freelist() for thread safety.
 */
bool do_conn_add_to_freelist(conn *c) {
  if (freecurr < freetotal) {
    freeconns[freecurr++] = c;
    return false;
  } else {
    /* try to enlarge free connections array */
    conn **new_freeconns = (conn **)realloc(freeconns, sizeof(conn *) * freetotal * 2);
    if (new_freeconns) {
      freetotal *= 2;
      freeconns = new_freeconns;
      freeconns[freecurr++] = c;
      return false;
    }
  }
  return true;
}

conn *conn_new(const int sfd, const int init_state, const int event_flags,
    const int read_buffer_size, struct event_base *base) {
  conn *c = conn_from_freelist();

  if (NULL == c) {
    if (!(c = (conn *)calloc(1, sizeof(conn)))) {
      //erro_out( "calloc(), %m\n");
      return NULL;
    }

    c->rbuf = c->wbuf = 0;
    c->iov = 0;
    c->msglist = 0;

    c->rsize = read_buffer_size;
    c->wsize = DATA_BUFFER_SIZE;
    c->iovsize = IOV_LIST_INITIAL;
    c->msgsize = MSG_LIST_INITIAL;

    c->rbuf = (char *)malloc((size_t)c->rsize);
    c->wbuf = (char *)malloc((size_t)c->wsize);
    c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
    c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);

    if (c->rbuf == 0 || c->wbuf == 0 || c->iov == 0 ||
        c->msglist == 0 ) {
      conn_free(c);
      //erro_out( "malloc() %m\n");
      return NULL;
    }

    STATS_LOCK();
    stats.conn_structs++;
    STATS_UNLOCK();
  }

  if (init_state == conn_listening){
    //erro_out( "<%d server listening\n", sfd);
  }else
    //erro_out( "<%d new client connection\n", sfd);

  c->sfd = sfd;
  c->state = init_state;
  c->rlbytes = 0;
  c->rbytes = c->wbytes = 0;
  c->wcurr = c->wbuf;
  c->rcurr = c->rbuf;
  c->ritem = 0;
  c->iovused = 0;
  c->msgcurr = 0;
  c->msgused = 0;

  c->write_and_go = conn_read;
  c->write_and_free = 0;
  c->write_and_delete = 0;
  *c->suffix = 0;
  c->nsuffix = 0;
  c->item = 0;
  c->vlen = 0;
  c->exptime = 0;

  c->xid = 0;
  *c->xkey = 0;
  c->xnkey = 0;

  event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
  event_base_set(base, &c->event);
  c->ev_flags = event_flags;

  if (event_add(&c->event, 0) == -1) {
    if (conn_add_to_freelist(c)) {
      conn_free(c);
    }
    //erro_out( "event_add() %m\n");
    return NULL;
  }

  STATS_LOCK();
  stats.curr_conns++;
  stats.total_conns++;
  STATS_UNLOCK();

  return c;
}

static void conn_cleanup(conn *c) {
  assert(c != NULL);

  if (c->item) {
    //debug_out( "#conn_clearup# free data(\033[34m%p\033[0m)\n", c->item);
    free(c->item);
    c->item = c->ritem = 0;
  }

  if (c->write_and_free) {
    free(c->write_and_free);
    c->write_and_free = 0;
  }

  if (c->write_and_delete) {
    //debug_out( "#conn_clearup# write_and_delete(\033[32m%p\033[0m)\n", c->write_and_delete);
    delete((QItem *)c->write_and_delete);
    c->write_and_delete = 0;
  }
}

/*
 * Frees a connection.
 */
void conn_free(conn *c) {
  if (c) {
    if (c->msglist)
      free(c->msglist);
    if (c->rbuf)
      free(c->rbuf);
    if (c->wbuf)
      free(c->wbuf);
    if (c->iov)
      free(c->iov);
    free(c);
  }
}

static void conn_close(conn *c) {
  assert(c != NULL);

  /* delete the event, the socket and the conn */
  event_del(&c->event);

  //erro_out( "<%d connection closed.\n", c->sfd);

  close(c->sfd);
  accept_new_conns(true);
  conn_cleanup(c);

  /* abort any transaction */
  if (c->xid) {
    if (qc->unremove(std::string(c->xkey, c->xnkey), c->xid) == false) {
      //erro_out( "Unremove non-existent transaction");
    }
    c->xid = 0;
  }

  /* if the connection has big buffers, just free it */
  if (c->rsize > READ_BUFFER_HIGHWAT || conn_add_to_freelist(c)) {
    conn_free(c);
  }

  STATS_LOCK();
  stats.curr_conns--;
  STATS_UNLOCK();

  return;
}


/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
static void conn_shrink(conn *c) {
  assert(c != NULL);

  if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
    char *newbuf;

    if (c->rcurr != c->rbuf)
      memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

    newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);

    if (newbuf) {
      c->rbuf = newbuf;
      c->rsize = DATA_BUFFER_SIZE;
    }
    /* TODO check other branch... */
    c->rcurr = c->rbuf;
  }

  if (c->msgsize > MSG_LIST_HIGHWAT) {
    struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
    if (newbuf) {
      c->msglist = newbuf;
      c->msgsize = MSG_LIST_INITIAL;
    }
    /* TODO check error condition? */
  }

  if (c->iovsize > IOV_LIST_HIGHWAT) {
    struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
    if (newbuf) {
      c->iov = newbuf;
      c->iovsize = IOV_LIST_INITIAL;
    }
    /* TODO check return value */
  }
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
static void conn_set_state(conn *c, int state) {
  assert(c != NULL);

  if (state != c->state) {
    if (state == conn_read) {
      conn_shrink(c);
    }
    c->state = state;
  }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(conn *c) {
  assert(c != NULL);

  if (c->iovused >= c->iovsize) {
    int i, iovnum;
    struct iovec *new_iov = (struct iovec *)realloc(c->iov,
        (c->iovsize * 2) * sizeof(struct iovec));
    if (! new_iov)
      return -1;
    c->iov = new_iov;
    c->iovsize *= 2;

    /* Point all the msghdr structures at the new list. */
    for (i = 0, iovnum = 0; i < c->msgused; i++) {
      c->msglist[i].msg_iov = &c->iov[iovnum];
      iovnum += c->msglist[i].msg_iovlen;
    }
  }

  return 0;
}


/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */

static int add_iov(conn *c, const void *buf, int len) {
  struct msghdr *m;
  int leftover;
  bool limit_to_mtu;

  assert(c != NULL);

  do {
    m = &c->msglist[c->msgused - 1];

    /*
     * Limit UDP packets, and the first payloads of TCP replies, to
     * UDP_MAX_PAYLOAD_SIZE bytes.
     */
    limit_to_mtu = (1 == c->msgused);

    /* We may need to start a new msghdr if this one is full. */
    if (m->msg_iovlen == IOV_MAX ||
        (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
      add_msghdr(c);
      m = &c->msglist[c->msgused - 1];
    }

    if (ensure_iov_space(c) != 0)
      return -1;

    /* If the fragment is too big to fit in the datagram, split it up */
    if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
      leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
      len -= leftover;
    } else {
      leftover = 0;
    }

    m = &c->msglist[c->msgused - 1];
    m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
    m->msg_iov[m->msg_iovlen].iov_len = len;

    c->msgbytes += len;
    c->iovused++;
    m->msg_iovlen++;

    buf = ((char *)buf) + len;
    len = leftover;
  } while (leftover > 0);

  return 0;
}

static void out_string(conn *c, const char *str) {
  size_t len;

  assert(c != NULL);

  //debug_out( ">%d %s\n", c->sfd, str);

  len = strlen(str);
  if ((len + 2) > (unsigned)c->wsize) {
    /* ought to be always enough. just fail for simplicity */
    str = "SERVER_ERROR output line too long";
    len = strlen(str);
  }

  memcpy(c->wbuf, str, len);
  memcpy(c->wbuf + len, "\r\n", 2);
  c->wbytes = len + 2;
  c->wcurr = c->wbuf;

  conn_set_state(c, conn_write);
  c->write_and_go = conn_read;
  return;
}

/*
 * we get here after reading the value in set/add/replace commands. The command
 * has been stored in c->item_comm, and the item is ready in c->item.
 */

static void complete_nread(conn *c) {
  assert(c != NULL);

  int ret;

  STATS_LOCK();
  stats.set_cmds++;
  STATS_UNLOCK();

  if (memcmp(c->item + c->vlen, "\r\n", 2) != 0) {
    free(c->item);
    out_string(c, "CLIENT_ERROR bad data chunk");
  } else {
    ret = qc->add(std::string(c->suffix, c->nsuffix), c->item, c->vlen, c->exptime);
    if (ret == true) {
      out_string(c, "STORED");
    } else {
      out_string(c, "NOT_STORED");
    }
  }

  c->item = c->ritem = 0;
}

typedef struct token_s {
  char *value;
  size_t length;
} token_t;

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define KEY_TOKEN 1
#define KEY_MAX_LENGTH 250

#define MAX_TOKENS 8

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *    for(int ix = 0; tokens[ix].length != 0; ix++) {
 *      ...
 *    }
 *    ncommand = tokens[ix].value - command;
 *    command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_command(char *command, token_t *tokens, const size_t max_tokens) {
  char *s, *e;
  size_t ntokens = 0;

  assert(command != NULL && tokens != NULL && max_tokens > 1);

  for (s = e = command; ntokens < max_tokens - 1; ++e) {
    if (*e == ' ') {
      if (s != e) {
        tokens[ntokens].value = s;
        tokens[ntokens].length = e - s;
        ntokens++;
        *e = '\0';
      }
      s = e + 1;
    }
    else if (*e == '\0') {
      if (s != e) {
        tokens[ntokens].value = s;
        tokens[ntokens].length = e - s;
        ntokens++;
      }

      break; /* string end */
    }
  }

  /*
   * If we scanned the whole string, the terminal value pointer is null,
   * otherwise it is the first unprocessed character.
   */
  tokens[ntokens].value =  *e == '\0' ? NULL : e;
  tokens[ntokens].length = 0;
  ntokens++;

  return ntokens;
}

/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, int bytes) {
  if (buf) {
    c->write_and_free = buf;
    c->wcurr = buf;
    c->wbytes = bytes;
    conn_set_state(c, conn_write);
    c->write_and_go = conn_read;
  } else {
    out_string(c, "SERVER_ERROR out of memory writing stats");
  }
}

/* ntokens is overwritten here... shrug.. */
static void on_get(conn *c, token_t *tokens, size_t ntokens) {
  char *key;
  size_t nkey;
  int i = 0;
  QItem *it;
  token_t *key_token = &tokens[KEY_TOKEN];
  assert(c != NULL);

  key = key_token->value;
  nkey = key_token->length;

  if(nkey > KEY_MAX_LENGTH) {
    out_string(c, "CLIENT_ERROR bad command line format");
    return;
  }

  int timeout = 0;
  int opening = 0;
  int closing = 0;
  int aborting = 0;
  int peeking = 0;

  char *opt = key;
  while ((opt = (char *)memchr(opt, '/', key_token->length - (opt - key)))) {
    if (nkey == key_token->length) {
      nkey = opt - key;
    }
    opt++;

    if (!memcmp(opt, "t=", 2))
      timeout = atoi(opt + 2);
    else if (!memcmp(opt, "open", 4))
      opening = 1;
    else if (!memcmp(opt, "close", 5))
      closing = 1;
    else if (!memcmp(opt, "abort", 5))
      aborting = 1;
    else if (!memcpy(opt, "peek", 4))
      peeking = 1;
  }

  if (((peeking || aborting) && (opening || closing)) || (peeking && aborting)) {
    out_string(c, "CLIENT_ERROR bad option");
    return;
  }

  if (aborting) {
    if (!c->xid) {
      //erro_out( "ABORT non-existent transaction on '%s'", key);
      out_string(c, "CLIENT_ERROR abort non-existent transaction");
      return;
    }
    if (nkey != c->xnkey || memcmp(key, c->xkey, nkey)) {
      out_string(c, "CLIENT_ERROR abort transation on wrong queue");
      return;
    }
    qc->unremove(std::string(c->xkey, c->xnkey), c->xid);
    c->xid = 0;
    out_string(c, "END");
    return;
  }

  if (closing) {
    if (!c->xid) {
      //erro_out( "ABORT non-existent transaction on '%s'", key);
      // let the client continue closing non-existent transaction.
      // it may be optimistically closing previous transactions as
      // it randomly jumps servers.
    } else {
      if (c->xid && (nkey != c->xnkey || memcmp(key, c->xkey, nkey))) {
        out_string(c, "CLIENT_ERROR close transation on wrong queue");
        return;
      }
      qc->confirmRemove(std::string(c->xkey, c->xnkey), c->xid);
      c->xid = 0;
    }
    if (!opening) {
      out_string(c, "END");
      return;
    }
    //else fall throught ...
  }

  if (c->xid && !peeking) {
    //erro_out( "GET unopen transaction on '%s'", key);
    out_string(c, "CLIENT_ERROR get unopen transation");
    return;
  }

  it = qc->remove(std::string(key, nkey), opening, peeking, timeout);
  if (it) {
    /*
     * Construct the response. Each hit adds three elements to the
     * outgoing data list:
     *   "VALUE "
     *   key
     *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
     */
    memcpy(c->suffix, key, nkey);
    c->nsuffix = nkey;
    c->nsuffix += sprintf(c->suffix + nkey, " 0 %d\r\n", it->getLength());

    if (add_iov(c, "VALUE ", 6) != 0 ||
        add_iov(c, c->suffix, c->nsuffix) != 0 ||
        add_iov(c, it->getData(), it->getLength()) != 0 ||
        add_iov(c, "\r\n", 2) != 0)
    {
      //erro_out( "add_iov() %m\n");
      out_string(c, "SERVER_ERROR add iov VALUE");
      return;
    }


    //debug_out( ">%d sending key %s\n", c->sfd, key);

    i++;

    if (opening) {
      c->xid = it->getXid();
      c->xnkey = nkey;
      memcpy(c->xkey, key, nkey);
      //don't free this time as opentransaction holds it
    } else {
      //free it later
      c->write_and_delete = it;
    }
  }


  //debug_out( ">%d END\n", c->sfd);

  if (add_iov(c, "END\r\n", 5) != 0) {
    //erro_out( "add_iov() %m\n");
    out_string(c, "SERVER_ERROR add iov END");
  }
  else {
    conn_set_state(c, conn_mwrite);
    c->msgcurr = 0;
  }

  STATS_LOCK();
  if (peeking)
    stats.peek_cmds ++;
  else
    stats.get_cmds ++;
  if (it)
    stats.get_hits ++;
  else
    stats.get_misses ++;
  STATS_UNLOCK();

  return;
}


static void on_set(conn *c, token_t *tokens, const size_t ntokens) {
  char *key;
  size_t nkey;
  int flags;
  time_t exptime;
  int vlen;

  char *data;

  assert(c != NULL);

  if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
    out_string(c, "CLIENT_ERROR bad command line format");
    return;
  }

  key = tokens[KEY_TOKEN].value;
  nkey = tokens[KEY_TOKEN].length;

  flags = strtoul(tokens[2].value, NULL, 10);
  exptime = strtol(tokens[3].value, NULL, 10);
  vlen = strtol(tokens[4].value, NULL, 10);

  if(errno == ERANGE || ((flags == 0 || exptime == 0) && errno == EINVAL)
      || vlen < 0 || nkey > SUFFIX_SIZE_MAX - 1) {
    out_string(c, "CLIENT_ERROR bad command line format");
    return;
  }

  data = (char *)malloc(vlen + 2);

  if (data == 0) {
    out_string(c, "SERVER_ERROR out of memory storing object");
    /* swallow the data line */
    c->write_and_go = conn_swallow;
    c->sbytes = vlen + 2;

    return;
  }
  //debug_out( "#on_set# alloc data(\033[33m%p\033[0m)\n", data);

  memcpy(c->suffix, key, nkey);
  c->nsuffix = nkey;
  c->exptime = exptime;
  c->item = data;
  c->vlen = vlen;

  c->ritem = data;
  c->rlbytes = vlen + 2;
  conn_set_state(c, conn_nread);
}

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
static void on_stats(conn *c, token_t *tokens, const size_t ntokens) {

  rel_time_t now = current_time;
  pid_t pid = getpid();

  char *subcommand;

  assert(c != NULL);

  if(ntokens < 2) {
    out_string(c, "CLIENT_ERROR bad command line");
    return;
  }
  char temp[2048];
  char *pos = temp;

/* avoid warning printf uint64_t using PRIu64 */
  STATS_LOCK();
  pos += sprintf(pos, "STAT pid %u\r\n", pid);
  pos += sprintf(pos, "STAT uptime %u\r\n", now);
  pos += sprintf(pos, "STAT time %ld\r\n", now + stats.started);
  pos += sprintf(pos, "STAT version " VERSION "\r\n");
  pos += sprintf(pos, "STAT pointer_size %lu\r\n", 8 * sizeof(void *));
  pos += sprintf(pos, "STAT curr_items %ld\r\n", qc->currentItems());
  pos += sprintf(pos, "STAT total_items %ld\r\n", qc->totalItems());
  pos += sprintf(pos, "STAT bytes %lu\r\n", qc->currentBytes());
  pos += sprintf(pos, "STAT curr_connections %d\r\n", stats.curr_conns - 1); /* ignore listening conn */
  pos += sprintf(pos, "STAT total_connections %d\r\n", stats.total_conns);
  pos += sprintf(pos, "STAT connection_structures %d\r\n", stats.conn_structs);
  pos += sprintf(pos, "STAT cmd_flush %ld\r\n", stats.flush_cmds);
  pos += sprintf(pos, "STAT cmd_get %ld\r\n", stats.get_cmds);
  pos += sprintf(pos, "STAT cmd_set %ld\r\n", stats.set_cmds);
  pos += sprintf(pos, "STAT get_hits %ld\r\n", stats.get_hits);
  pos += sprintf(pos, "STAT get_misses %ld\r\n", stats.get_misses);
  pos += sprintf(pos, "STAT bytes_read %lu\r\n", stats.bytes_read);
  pos += sprintf(pos, "STAT bytes_written %lu\r\n", stats.bytes_written);
  pos += sprintf(pos, "STAT threads %d\r\n", settings.num_threads);
  pos += sprintf(pos, "STAT accepting_conns %d\r\n", stats.accepting_conns);
  pos += sprintf(pos, "STAT listen_disabled_num %d\r\n", stats.listen_disabled_num);
  STATS_UNLOCK();

  std::string qstats;

  if (ntokens == 2) {
    qstats = qc->stats();
  } else {
    subcommand = tokens[SUBCOMMAND_TOKEN].value;
    qstats = qc->stats(subcommand);
  }

  qstats += "END\r\n";

  size_t bytes (pos - temp);
  char *buf = (char *)malloc(bytes + qstats.length());
  if (!buf) {
    out_string(c, "CLIENT_ERROR alloc stats");
    return;
  }
  memcpy(buf, temp, pos - temp);
  memcpy(buf + bytes, qstats.c_str(), qstats.length());
  write_and_free(c, buf, bytes + qstats.length());
}

static void on_shutdown() {
  if (event_base_loopexit(main_base, NULL) < 0) {
    fprintf(stderr, "event_base_loopexit()");
  }

  if (qc) qc->shutdown();
}

static void process_command(conn *c, char *command) {

  token_t tokens[MAX_TOKENS];
  size_t ntokens;

  assert(c != NULL);

  //debug_out( "<%d %s\n", c->sfd, command);

  /*
   * for commands set, we build an item and read the data
   * directly into it, then continue in nread_complete().
   */

  c->msgcurr = 0;
  c->msgused = 0;
  c->iovused = 0;
  if (add_msghdr(c) != 0) {
    out_string(c, "SERVER_ERROR out of memory preparing response");
    return;
  }

  ntokens = tokenize_command(command, tokens, MAX_TOKENS);
  if (ntokens == 3 && (!strcmp(tokens[COMMAND_TOKEN].value, "get"))) {

    on_get(c, tokens, ntokens);

  } else if ((ntokens == 6) && (!strcmp(tokens[COMMAND_TOKEN].value, "set"))) {

    on_set(c, tokens, ntokens);

  } else if ((ntokens == 3) && (!strcmp(tokens[COMMAND_TOKEN].value, "delete"))) {

    if (qc->deleteQueue(tokens[KEY_TOKEN].value) == false) {
      out_string(c, "END");
    }

    out_string(c, "OK");

  } else if ((ntokens == 2 || ntokens == 3) &&
      (!strcmp(tokens[COMMAND_TOKEN].value, "stats"))) {

    on_stats(c, tokens, ntokens);

  } else if (ntokens == 3 && (!strcmp(tokens[COMMAND_TOKEN].value, "flush"))) {
    set_current_time();

    STATS_LOCK();
    stats.flush_cmds++;
    STATS_UNLOCK();

    if (qc->flush(tokens[KEY_TOKEN].value) == false) {
      out_string(c, "END");
    }

    out_string(c, "OK");

  } else if (ntokens == 2 && (!strcmp(tokens[COMMAND_TOKEN].value, "version"))) {

    out_string(c, "VERSION " VERSION);

  } else if (ntokens == 2 && (!strcmp(tokens[COMMAND_TOKEN].value, "quit"))) {

    conn_set_state(c, conn_closing);

  } else if (ntokens == 2 && (!strcmp(tokens[COMMAND_TOKEN].value, "shutdown"))) {

    on_shutdown();

  } else {
    out_string(c, "ERROR");
  }
  return;
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(conn *c) {
  char *el, *cont;

  assert(c != NULL);
  assert(c->rcurr <= (c->rbuf + c->rsize));

  if (c->rbytes == 0)
    return 0;
  el = (char *)memchr(c->rcurr, '\n', c->rbytes);
  if (!el)
    return 0;
  cont = el + 1;
  if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
    el--;
  }
  *el = '\0';

  assert(cont <= (c->rcurr + c->rbytes));

  process_command(c, c->rcurr);

  c->rbytes -= (cont - c->rcurr);
  c->rcurr = cont;

  assert(c->rcurr <= (c->rbuf + c->rsize));

  return 1;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 * return 0 if there's nothing to read on the first read.
 */
static int try_read_network(conn *c) {
  int gotdata = 0;
  int res;

  assert(c != NULL);

  if (c->rcurr != c->rbuf) {
    if (c->rbytes != 0) /* otherwise there's nothing to copy */
      memmove(c->rbuf, c->rcurr, c->rbytes);
    c->rcurr = c->rbuf;
  }

  while (1) {
    if (c->rbytes >= c->rsize) {
      char *new_rbuf = (char *)realloc(c->rbuf, c->rsize * 2);
      if (!new_rbuf) {
        //erro_out( "Couldn't realloc input buffer\n");
        c->rbytes = 0; /* ignore what we read */
        out_string(c, "SERVER_ERROR out of memory reading request");
        c->write_and_go = conn_closing;
        return 1;
      }
      c->rcurr = c->rbuf = new_rbuf;
      c->rsize *= 2;
    }

    int avail = c->rsize - c->rbytes;
    res = read(c->sfd, c->rbuf + c->rbytes, avail);
    if (res > 0) {
      STATS_LOCK();
      stats.bytes_read += res;
      STATS_UNLOCK();
      gotdata = 1;
      c->rbytes += res;
      if (res == avail) {
        continue;
      } else {
        break;
      }
    }
    if (res == 0) {
      /* connection closed */
      conn_set_state(c, conn_closing);
      return 1;
    }
    if (res == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      /* Should close on unhandled errors. */
      conn_set_state(c, conn_closing);
      return 1;
    }
  }
  return gotdata;
}

static bool update_event(conn *c, const int new_flags) {
  assert(c != NULL);

  struct event_base *base = c->event.ev_base;
  if (c->ev_flags == new_flags)
    return true;
  if (event_del(&c->event) == -1) return false;
  event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
  event_base_set(base, &c->event);
  c->ev_flags = new_flags;
  if (event_add(&c->event, 0) == -1) return false;
  return true;
}

/*
 * Sets whether we are listening for new connections or not.
 */
void do_accept_new_conns(const bool do_accept) {
  conn *next;

  for (next = listen_conn; next; next = next->next) {
    if (do_accept) {
      update_event(next, EV_READ | EV_PERSIST);
      if (listen(next->sfd, settings.backlog) != 0) {
        //erro_out( "listen() %m\n");
      }
    }
    else {
      update_event(next, 0);
      if (listen(next->sfd, 0) != 0) {
        //erro_out( "listen() %m\n");
      }
    }
  }

  if (do_accept) {
    STATS_LOCK();
    stats.accepting_conns = 1;
    STATS_UNLOCK();
  } else {
    STATS_LOCK();
    stats.accepting_conns = 0;
    stats.listen_disabled_num++;
    STATS_UNLOCK();
  }
}


/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static int transmit(conn *c) {
  assert(c != NULL);

  if (c->msgcurr < c->msgused &&
      c->msglist[c->msgcurr].msg_iovlen == 0) {
    /* Finished writing the current msg; advance to the next. */
    c->msgcurr++;
  }
  if (c->msgcurr < c->msgused) {
    ssize_t res;
    struct msghdr *m = &c->msglist[c->msgcurr];

    res = sendmsg(c->sfd, m, 0);
    if (res > 0) {
      STATS_LOCK();
      stats.bytes_written += res;
      STATS_UNLOCK();

      /* We've written some of the data. Remove the completed
         iovec entries from the list of pending writes. */
      while (m->msg_iovlen > 0 && res >= (unsigned)m->msg_iov->iov_len) {
        res -= m->msg_iov->iov_len;
        m->msg_iovlen--;
        m->msg_iov++;
      }

      /* Might have written just part of the last iovec entry;
         adjust it so the next write will do the rest. */
      if (res > 0) {
        m->msg_iov->iov_base = (char *)m->msg_iov->iov_base + res;
        m->msg_iov->iov_len -= res;
      }
      return TRANSMIT_INCOMPLETE;
    }
    if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (!update_event(c, EV_WRITE | EV_PERSIST)) {
        //erro_out( "update_event() %m\n");
        conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
      }
      return TRANSMIT_SOFT_ERROR;
    }
    /* if res==0 or res==-1 and error is not EAGAIN or EWOULDBLOCK,
       we have a real error, on which we close the connection */
    //erro_out( "Failed to write, and not due to blocking\n");

    conn_set_state(c, conn_closing);
    return TRANSMIT_HARD_ERROR;
  } else {
    return TRANSMIT_COMPLETE;
  }
}

static void drive_machine(conn *c) {
  bool stop = false;
  int sfd, flags = 1;
  socklen_t addrlen;
  struct sockaddr_storage addr;
  int nreqs = settings.reqs_per_event;
  int res;

  assert(c != NULL);

  while (!stop) {

    switch(c->state) {
      case conn_listening:
        addrlen = sizeof(addr);
        if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            /* these are transient, so don't log anything */
            stop = true;
          } else if (errno == EMFILE) {
            //erro_out( "Too many open connections %m\n");
            accept_new_conns(false);
            stop = true;
          } else {
            //erro_out( "accept() %m\n");
            stop = true;
          }
          break;
        }
        if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
            fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
          //erro_out( "setting O_NONBLOCK %m\n");
          close(sfd);
          break;
        }
        dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,
            DATA_BUFFER_SIZE);
        break;

      case conn_read:
        if (try_read_command(c) != 0) {
          continue;
        }
        /* Only process nreqs at a time to avoid starving other
           connections */
        if (--nreqs && (try_read_network(c)) != 0) {
          continue;
        }
        /* we have no command line and no data to read from network */
        if (!update_event(c, EV_READ | EV_PERSIST)) {
          //erro_out( "update_event() %m\n");
          conn_set_state(c, conn_closing);
          break;
        }
        stop = true;
        break;

      case conn_nread:
        /* we are reading rlbytes into ritem; */
        if (c->rlbytes == 0) {
          complete_nread(c);
          break;
        }
        /* first check if we have leftovers in the conn_read buffer */
        if (c->rbytes > 0) {
          int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
          memcpy(c->ritem, c->rcurr, tocopy);
          c->ritem += tocopy;
          c->rlbytes -= tocopy;
          c->rcurr += tocopy;
          c->rbytes -= tocopy;
          break;
        }

        /*  now try reading from the socket */
        res = read(c->sfd, c->ritem, c->rlbytes);
        if (res > 0) {
          STATS_LOCK();
          stats.bytes_read += res;
          STATS_UNLOCK();
          c->ritem += res;
          c->rlbytes -= res;
          break;
        }
        if (res == 0) { /* end of stream */
          conn_set_state(c, conn_closing);
          break;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          if (!update_event(c, EV_READ | EV_PERSIST)) {
            //erro_out( "update_event() %m\n");
            conn_set_state(c, conn_closing);
            break;
          }
          stop = true;
          break;
        }
        /* otherwise we have a real error, on which we close the connection */
        //erro_out( "Failed to nread, and not due to blocking\n");
        conn_set_state(c, conn_closing);
        break;

      case conn_swallow:
        /* we are reading sbytes and throwing them away */
        if (c->sbytes == 0) {
          conn_set_state(c, conn_read);
          break;
        }

        /* first check if we have leftovers in the conn_read buffer */
        if (c->rbytes > 0) {
          int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
          c->sbytes -= tocopy;
          c->rcurr += tocopy;
          c->rbytes -= tocopy;
          break;
        }

        /*  now try reading from the socket */
        res = read(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
        if (res > 0) {
          STATS_LOCK();
          stats.bytes_read += res;
          STATS_UNLOCK();
          c->sbytes -= res;
          break;
        }
        if (res == 0) { /* end of stream */
          conn_set_state(c, conn_closing);
          break;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          if (!update_event(c, EV_READ | EV_PERSIST)) {
            //erro_out( "update_event() %m\n");
            conn_set_state(c, conn_closing);
            break;
          }
          stop = true;
          break;
        }
        /* otherwise we have a real error, on which we close the connection */
        //erro_out( "Failed to swalloc, and not due to blocking\n");
        conn_set_state(c, conn_closing);
        break;

      case conn_write:
        /*
         * We want to write out a simple response. If we haven't already,
         * assemble it into a msgbuf list (this will be a single-entry
         * list for TCP or a two-entry list for UDP).
         */
        if (c->iovused == 0) {
          if (add_iov(c, c->wcurr, c->wbytes) != 0) {
            //erro_out( "write add_iov() %m\n");
            conn_set_state(c, conn_closing);
            break;
          }
        }

        /* fall through... */

      case conn_mwrite:
        switch (transmit(c)) {
          case TRANSMIT_COMPLETE:
            if (c->state == conn_mwrite) {
              if (c->write_and_delete) {
                //debug_out( "#drive# write_and_delete(\033[32m%p\033[0m)\n", c->write_and_delete);
                delete((QItem *)c->write_and_delete);
                c->write_and_delete = 0;
              }
              conn_set_state(c, conn_read);
            } else if (c->state == conn_write) {
              if (c->write_and_free) {
                free(c->write_and_free);
                c->write_and_free = 0;
              }
              conn_set_state(c, c->write_and_go);
            } else {
              //erro_out( "Unexpected transmit state %d\n", c->state);
              conn_set_state(c, conn_closing);
            }
            break;

          case TRANSMIT_INCOMPLETE:
          case TRANSMIT_HARD_ERROR:
            break;           /* Continue in state machine. */

          case TRANSMIT_SOFT_ERROR:
            stop = true;
            break;
        }
        break;

      case conn_closing:
        conn_close(c);
        stop = true;
        break;
    }
  }

  return;
}

void event_handler(const int fd, const short which, void *arg) {
  conn *c;

  c = (conn *)arg;
  assert(c != NULL);

  c->which = which;

  /* sanity */
  if (fd != c->sfd) {
    //erro_out( "Catastrophic: event fd doesn't match conn fd!\n");
    conn_close(c);
    return;
  }

  drive_machine(c);

  /* wait for next event */
  return;
}

static int new_socket(struct addrinfo *ai) {
  int sfd;
  int flags;

  if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
    return -1;
  }

  if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
      fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
    fprintf(stderr, "setting O_NONBLOCK %m\n");
    close(sfd);
    return -1;
  }
  return sfd;
}

static int server_socket(const int port) {
  int sfd;
  struct linger ling = {0, 0};
  struct addrinfo *ai;
  struct addrinfo *next;
  struct addrinfo hints;
  char port_buf[NI_MAXSERV];
  int error;
  int success = 0;

  int flags =1;

  /*
   * the memset call clears nonstandard fields in some impementations
   * that otherwise mess things up.
   */
  memset(&hints, 0, sizeof (hints));
  hints.ai_flags  = AI_PASSIVE;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  snprintf(port_buf, NI_MAXSERV, "%d", port);
  error= getaddrinfo(settings.inter, port_buf, &hints, &ai);
  if (error != 0) {
    if (error != EAI_SYSTEM)
      fprintf(stderr, "getaddrinfo() %s\n", gai_strerror(error));
    else
      fprintf(stderr, "getaddrinfo() %m\n");

    return 1;
  }

  for (next= ai; next; next= next->ai_next) {
    conn *listen_conn_add;
    if ((sfd = new_socket(next)) == -1) {
      /* getaddrinfo can return "junk" addresses,
       * we make sure at least one works before erroring.
       */
      continue;
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    if (error != 0)
      perror("setsockopt");

    error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
    if (error != 0)
      perror("setsockopt");

    error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
    if (error != 0)
      perror("setsockopt");

    if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
      if (errno != EADDRINUSE) {
        perror("bind()");
        close(sfd);
        freeaddrinfo(ai);
        return 1;
      }
      close(sfd);
      continue;
    } else {
      success++;
      if (listen(sfd, settings.backlog) == -1) {
        perror("listen()");
        close(sfd);
        freeaddrinfo(ai);
        return 1;
      }
    }

    if (!(listen_conn_add = conn_new(sfd, conn_listening,
            EV_READ | EV_PERSIST, 1, main_base))) {
      fprintf(stderr, "failed to create listening connection\n");
      exit(EXIT_FAILURE);
    }

    listen_conn_add->next = listen_conn;
    listen_conn = listen_conn_add;
  }

  freeaddrinfo(ai);

  /* Return zero iff we detected no errors in starting up connections */
  return success == 0;
}

static int new_socket_unix(void) {
  int sfd;
  int flags;

  if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    perror("socket()");
    return -1;
  }

  if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
      fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
    perror("setting O_NONBLOCK");
    close(sfd);
    return -1;
  }
  return sfd;
}

static int server_socket_unix(const char *path, int access_mask) {
  int sfd;
  struct linger ling = {0, 0};
  struct sockaddr_un addr;
  struct stat tstat;
  int flags =1;
  int old_umask;

  if (!path) {
    return 1;
  }

  if ((sfd = new_socket_unix()) == -1) {
    return 1;
  }

  /*
   * Clean up a previous socket file if we left it around
   */
  if (lstat(path, &tstat) == 0) {
    if (S_ISSOCK(tstat.st_mode))
      unlink(path);
  }

  setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
  setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
  setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

  /*
   * the memset call clears nonstandard fields in some impementations
   * that otherwise mess things up.
   */
  memset(&addr, 0, sizeof(addr));

  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, path);
  old_umask=umask( ~(access_mask&0777));
  if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    perror("bind()");
    close(sfd);
    umask(old_umask);
    return 1;
  }
  umask(old_umask);
  if (listen(sfd, settings.backlog) == -1) {
    perror("listen()");
    close(sfd);
    return 1;
  }
  if (!(listen_conn = conn_new(sfd, conn_listening,
          EV_READ | EV_PERSIST, 1, main_base))) {
    fprintf(stderr, "failed to create listening connection\n");
    exit(EXIT_FAILURE);
  }

  return 0;
}

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;
static struct event clockevent;

/* time-sensitive callers can call it by hand with this, outside the normal ever-1-second timer */
static void set_current_time(void) {
  struct timeval timer;

  gettimeofday(&timer, NULL);
  current_time = (rel_time_t) (timer.tv_sec - stats.started);
}

static void clock_handler(const int fd, const short which, void *arg) {
//  struct timeval t = {.tv_sec = 1, .tv_usec = 0};
  struct timeval t = {1, 0};
  static bool initialized = false;

  if (initialized) {
    /* only delete the event if it's actually there. */
    evtimer_del(&clockevent);
  } else {
    initialized = true;
  }

  evtimer_set(&clockevent, clock_handler, 0);
  event_base_set(main_base, &clockevent);
  evtimer_add(&clockevent, &t);

  set_current_time();
}

static void save_pid(const pid_t pid, const char *pid_file) {
  assert(pid_file);
  FILE *fp;

  if (!access(pid_file, F_OK)) {
    fprintf(stderr, "%s already exist\n", pid_file);
    exit(EXIT_FAILURE);
  }

  if ((fp = fopen(pid_file, "w")) == NULL) {
    fprintf(stderr, "fopen('%s') %m\n", pid_file);
    exit(EXIT_FAILURE);
  }

  fprintf(fp,"%ld\n", (long)pid);
  if (fclose(fp) == -1) {
    fprintf(stderr, "fclose('%s') %m\n", pid_file);
    exit(EXIT_FAILURE);
  }
}

static void remove_pidfile(const char *pid_file) {
  if (pid_file == NULL)
    return;

  if (unlink(pid_file) != 0) {
    //erro_out( "remove('%s') %m\n", pid_file);
  }

}

static void sig_set(int signo, void (*handler)(int)) {
  struct sigaction sa;
  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = handler;

  if (sigemptyset(&sa.sa_mask) == -1 || sigaction(signo, &sa, 0) == -1) {
    fprintf(stderr, "Signal [%d] set sa_handler", signo);
    exit(EXIT_FAILURE);
  }
}

static void sig_handler(int signo) {
  switch (signo) {
    case SIGINT:
    case SIGTERM:
      fprintf(stdout, "Signal [%d] received, System exit...\n", signo);
      on_shutdown();
      break;
    case SIGPIPE:
      fprintf(stdout, "Signal [SIGPIPE] received, Ignored...\n");
      break;
    default:
      fprintf(stdout, "Signal [%d] received, Ignored...\n", signo);
      break;
  }
  return;
}

static void usage(void) {
  printf("\033[32m" VERSION "\033[0m\n");
  printf("\033[34m"
       "-V <dir>    var directory to save pidfile, queues and logs on disk,\n"
       "        (default: './'; pidfile->'./var/kestrel.pid',\n"
       "        queues->'./var/queues', log->'./var/log/kestrel.log')\n"
       "-p <num>    TCP port number to listen on (default: 11211)\n"
       "-d      run as a daemon\n"
       "-l <ip_addr>  interface to listen on (default: INDRR_ANY)\n"
       "-s <file>   unix socket path to listen on (disables network support)\n"
       "-m <num>    max memory per queue in megabytes (default: 64MB)\n"
       "-M <num>    max size per journal in megabytes (default: infinit)\n"
       "-i <num>    max item num per queue (default: infinit)\n"
       "-c <num>    max simultaneous connections (default: 1024)\n"
       "-N      disable journal file\n"
       "-S      enable sync journal\n"
       "-v      verbose (print errors/warnings while in event loop)\n"
       "-vv       very verbose (also print client commands/reponses)\n"
       "-h      print this help and exit\n"
       "-b      Set the backlog queue limit (default: 1024)\n"
       "-k      lock down all paged memory. Note that there is alimit\n"
       "        on how much memory you may lock.  Trying to allocate\n"
       "        more than that would fail, so be sure you set the limit\n"
       "        correctly for the user you started the daemon with\n"
       "        (this is done with 'ulimit -S -l NUM_KB').\n"
       "\033[0m\n"
       );

#ifdef USE_THREADS
  printf("\033[34m"
       "-t <num>    number of threads to use, default 4\n"
       "\033[0m\n"
       );
#endif
  return;
}

}

using namespace rdd;

int main (int argc, char **argv) {
  bool lock_memory = false;
  bool daemonize = false;

  /* init settings */
  settings_init();

  /* set stderr non-buffering (for running under, say, daemontools) */
  setbuf(stderr, NULL);

  int c;
  while ((c = getopt(argc, argv, "V:p:s:Sl:m:j:M:Ni:c:t:b:kdhv")) != -1) {
    switch (c) {
    case 'V':
      settings.vardir = optarg;
      break;
    case 'p':
      settings.port = atoi(optarg);
      break;
    case 'd':
      daemonize = true;
      break;
    case 's':
      settings.socketpath = optarg;
      break;
    case 'l':
      settings.inter = optarg;
      break;
    case 'm':
      settings.maxmembytes = ((size_t)atoi(optarg)) << 20;
      break;
    case 'j':
      settings.maxjnlbytes = ((size_t)atoi(optarg)) << 20;
      break;
    case 'M':
      settings.maxbytes = ((size_t)atoi(optarg)) << 20;
      break;
    case 'N':
      settings.keepjournal = false;
      break;
    case 'S':
      settings.syncjournal = true;
      break;
    case 'i':
      settings.maxitems = atoi(optarg);
      break;
    case 'c':
      settings.maxconns = atoi(optarg);
      break;
    case 'k':
      lock_memory = true;
      break;
    case 't':
      settings.num_threads = atoi(optarg) + 1; /* Extra dispatch thread */
      if (settings.num_threads < 2) {
        fprintf(stderr, "Number of threads must be greater than 0\n");
        return 1;
      }
      break;
    case 'b':
      settings.backlog = atoi(optarg);
      break;
    case 'h':
      usage();
      exit(EXIT_SUCCESS);
    case 'v':
      settings.verbose++;
      break;
    default:
      fprintf(stderr, "Illegal argument \"%c\"\n", c);
      exit(EXIT_FAILURE);
    }
  }

  settings_dump();

  if (access(settings.vardir, R_OK|W_OK|X_OK) < 0) {
    fprintf(stderr, "failed to access(%s) %m\n", settings.vardir);
    exit(EXIT_FAILURE);
  }

  char pid_file[MAX_PATH_SIZE];
  char queue_dir[MAX_PATH_SIZE];

  snprintf(pid_file, MAX_PATH_SIZE - 1, "%s/kestrel.pid", settings.vardir);
  snprintf(queue_dir, MAX_PATH_SIZE - 1, "%s/queue/", settings.vardir);

  if (mkdir(queue_dir, 0755) < 0 &&
      (errno != EEXIST || access(queue_dir, R_OK|W_OK|X_OK) < 0)) {
    fprintf(stderr, "failed to mkdir(%s) %m\n", queue_dir);
    exit(EXIT_FAILURE);
  }

  /*
  TLogConf logconf;
  switch (settings.verbose) {
  case 0:
    logconf.minLevel = LL_NOTICE;
    break;
  case 1:
    logconf.minLevel = LL_TRACE;
    break;
  case 2:
    logconf.minLevel = LL_DEBUG;
    break;
  }
  logconf.maxLen = 2040 << 20; //2040M
  logconf.spec.log2TTY = 0;
  slog_open(settings.vardir, "kestrel.", &logconf);
  */
  /*
   * If needed, increase rlimits to allow as many connections
   * as needed.
   */

  struct rlimit rlim;
  if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
    fprintf(stderr, "failed to getrlimit number of files\n");
    exit(EXIT_FAILURE);
  } else {
    int maxfiles = settings.maxconns;
    if (rlim.rlim_cur < (unsigned)maxfiles)
      rlim.rlim_cur = maxfiles + 3;
    if (rlim.rlim_max < rlim.rlim_cur)
      rlim.rlim_max = rlim.rlim_cur;
    if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
      fprintf(stderr, "failed to set rlimit for open files. Try running as root or requesting smaller maxconns value.\n");
      exit(EXIT_FAILURE);
    }
  }

  /* daemonize if requested */
  /* if we want to ensure our ability to dump core, don't chdir to / */
  if (daemonize && daemon(1, 1) == -1) {
    fprintf(stderr, "failed to daemon(): %m\n");
    return 1;
  }

  /* lock paged memory if needed */
  if (lock_memory && mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
    fprintf(stderr, "warning: -k invalid, mlockall() failed: %m\n");
  }

  qc = new QueueCollection(queue_dir, settings.maxmembytes, settings.maxjnlbytes,
            settings.maxitems, settings.maxbytes,
            settings.keepjournal, settings.syncjournal);
  if (!qc) {
    fprintf(stderr, "failed to new QueueCollection\n");
    exit(EXIT_FAILURE);
  }
  if (qc->loadQueues() < 0) {
    fprintf(stderr, "failed to load queues\n");
    exit(EXIT_FAILURE);
  }

  /* initialize main thread libevent instance */
  main_base = event_init();

  /* initialize other stuff */
  stats_init();
  conn_init();

  /* handle SIGINT */
  sig_set(SIGTERM, sig_handler);// KILL
  sig_set(SIGTSTP, sig_handler);// [Ctrl+Z]
  sig_set(SIGQUIT, sig_handler);// [Ctrl+\]
  sig_set(SIGINT,  sig_handler);// [Ctrl+C]
  sig_set(SIGPIPE, sig_handler);
  sig_set(SIGALRM, sig_handler);
  sig_set(SIGHUP,  sig_handler);
  sig_set(SIGCHLD, sig_handler);
  sig_set(SIGUSR1, sig_handler);
  sig_set(SIGUSR2, sig_handler);

  /* start up worker threads if MT mode */
  thread_init(settings.num_threads, main_base);
  /* initialise clock event */
  clock_handler(0, 0, 0);
  /* create unix mode sockets after dropping privileges */
  if (settings.socketpath != NULL) {
    errno = 0;
    if (server_socket_unix(settings.socketpath,settings.access)) {
      fprintf(stderr, "failed to listen on UNIX socket: %s, %m\n",
        settings.socketpath);
      exit(EXIT_FAILURE);
    }
  }

  /* create the listening socket, bind it, and init */
  if (settings.socketpath == NULL) {
    errno = 0;
    if (settings.port && server_socket(settings.port)) {
      fprintf(stderr, "failed to listen on TCP port %d, %m\n", settings.port);
      exit(EXIT_FAILURE);
    }
    /*
     * initialization order: first create the listening sockets
     * (may need root on low ports), then drop root if needed,
     * then daemonise if needed, then init libevent (in some cases
     * descriptors created by libevent wouldn't survive forking).
     */
  }

  /* save the PID in, do this after thread_init due to
     a file descriptor handling bug somewhere in libevent */
  save_pid(getpid(), pid_file);

  /* enter the event loop */
  event_base_loop(main_base, 0);

  /* remove the PID file */
  remove_pidfile(pid_file);

  return 0;
}
