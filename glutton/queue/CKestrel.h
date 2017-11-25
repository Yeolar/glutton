#pragma once

#include "QItem.h"

/* Set to nonzero if you want to include DTRACE */
#define ENABLE_DTRACE 0

/* machine is bigendian */
/* #undef ENDIAN_BIG */

/* machine is littleendian */
#define ENDIAN_LITTLE 1

/* Define to 1 if you have the `getpagesizes' function. */
/* #undef HAVE_GETPAGESIZES */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `memcntl' function. */
/* #undef HAVE_MEMCNTL */


/* Name of package */
#define PACKAGE "ckestrel"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "search@tsina"

/* Define to the version of this package. */
#define PACKAGE_VERSION "0.1"

/* Define this if you want to use pthreads */
/* #undef USE_THREADS */

#define USE_THREADS 1

/* Version number of package */
#define VERSION PACKAGE "" PACKAGE_VERSION " build " __TIME__ " " __DATE__

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* define to int if socklen_t not available */
/* #undef socklen_t */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>

#define DATA_BUFFER_SIZE    1024
#define UDP_MAX_PAYLOAD_SIZE  1400
#define MAX_SENDBUF_SIZE    (256 * 1024 * 1024)

#ifndef IOV_MAX
#define IOV_MAX  1024
#endif

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL    200

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL    400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL    10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT    8192
#define ITEM_LIST_HIGHWAT    400
#define IOV_LIST_HIGHWAT    600
#define MSG_LIST_HIGHWAT    100

/* Get a consistent bool type */
# include <stdbool.h>

# include <stdint.h>

/* unistd.h is here */
# include <unistd.h>

namespace rdd {

/** Time relative to server start. Smaller than time_t on 64-bit systems. */
typedef unsigned int rel_time_t;

struct stats {
  int      curr_conns;
  int      total_conns;
  int      conn_structs;
  long    get_cmds;
  long    set_cmds;
  long    peek_cmds;
  long    get_hits;
  long    get_misses;
  long    flush_cmds;
  time_t    started;      /* when the process was started */
  size_t    bytes_read;
  size_t    bytes_written;
  int      accepting_conns;  /* whether we are currently accepting */
  int      listen_disabled_num;
};

#define MAX_PATH_SIZE    256
#define MAX_VERBOSITY_LEVEL 2

struct settings {
  int access;      /* access mask (a la chmod) for unix domain socket */
  int port;
  char *vardir;    /* var dir for pid_file, queue_dir and log */
  char *inter;    /* bind inter address*/
  size_t maxmembytes;  /* maximum amount of a queue to keep in memory.
             * if a queue grows larger than
             * this (in bytes), it will drop into read-behind mode,
             * with only this amount kept in memory.*/
  size_t maxjnlbytes;  /* when a queue's journal reaches this size,
             * the queue will wait until it
             * is empty, and will then rotate the journal. */
  size_t maxbytes;  /* max bytes or journal */
  long maxitems;    /* max items in queue */
  int keepjournal;  /* don't keep a journal file for this queue. when kestrel exits, any */
            /* remaining contents will be lost. */
  int syncjournal;  /* force sync to journal */
  int maxconns;     /* to limit connections-related memory to about 5MB */
  int verbose;    /* log verbose */
  char *socketpath;  /* path to unix socket if using local socket */
  int num_threads;  /* number of libevent threads to run */
  int reqs_per_event;  /* Maximum number of io to process on each io-event. */
  int backlog;
};

extern struct stats stats;
extern struct settings settings;

enum conn_states {
  conn_listening,  /** the socket which listens for connections */
  conn_read,     /** reading in a command line */
  conn_write,    /** writing out a simple response */
  conn_nread,    /** reading in a fixed number of bytes */
  conn_swallow,  /** swallowing unnecessary bytes w/o storing */
  conn_closing,  /** closing this connection */
  conn_mwrite,   /** writing out many items sequentially */
};

#define SUFFIX_SIZE_MAX  128

typedef struct conn conn;
struct conn {
  int  sfd;
  int  state;
  struct event event;
  short  ev_flags;
  short  which;   /** which events were just triggered */

  char   *rbuf;   /** buffer to read commands into */
  char   *rcurr;  /** but if we parsed some already, this is where we stopped */
  int  rsize;   /** total allocated size of rbuf */
  int  rbytes;  /** how much data, starting from rcur, do we have unparsed */

  char   *wbuf;
  char   *wcurr;
  int  wsize;
  int  wbytes;
  int  write_and_go; /** which state to go into after finishing current write */
  void   *write_and_free; /** free this memory after finishing writing */
  void   *write_and_delete; /** free this memory after finishing mwriting */

  char   *ritem;  /** when we read in an item's value, it goes here */
  int  rlbytes;

  /* data for the nread state */

  /**
   * item is used to hold an item structure created after reading the command
   * line of set/add/replace commands, but before we finished reading the actual
   * data. The data is read into ITEM_data(item) to avoid extra copying.
   */

  /* data for the swallow state */
  int  sbytes;  /* how many bytes to swallow */

  /* data for the mwrite state */
  struct iovec *iov;
  int  iovsize;   /* number of elements allocated in iov[] */
  int  iovused;   /* number of elements used in iov[] */

  struct msghdr *msglist;
  int  msgsize;   /* number of elements allocated in msglist[] */
  int  msgused;   /* number of elements used in msglist[] */
  int  msgcurr;   /* element in msglist[] being transmitted now */
  int  msgbytes;  /* number of bytes in current msg */

  /* item */
  size_t nsuffix;
  char   suffix[SUFFIX_SIZE_MAX];   /* read: [key], write: [key len flag\r\n] */
  char   *item;
  size_t vlen;
  int  exptime;

  /* transaction */
  int  xid;
  size_t xnkey;
  char   xkey[SUFFIX_SIZE_MAX];

  conn   *next;   /* Used for generating a list of conn structures */
};

/* current time of day (updated periodically) */
extern volatile rel_time_t current_time;

/*
 * Functions
 */

void do_accept_new_conns(const bool do_accept);
conn *do_conn_from_freelist();
bool do_conn_add_to_freelist(conn *c);
conn *conn_new(const int sfd, const int init_state, const int event_flags, const int read_buffer_size, struct event_base *base);


/*
 * In multithreaded mode, we wrap certain functions with lock management and
 * replace the logic of some other functions. All wrapped functions have
 * "mt_" and "do_" variants. In multithreaded mode, the plain version of a
 * function is #define-d to the "mt_" variant, which often just grabs a
 * lock and calls the "do_" function. In singlethreaded mode, the "do_"
 * function is called directly.
 *
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
#ifdef USE_THREADS

void thread_init(int nthreads, struct event_base *main_base);
int  dispatch_event_add(int thread, conn *c);
void dispatch_conn_new(int sfd, int init_state, int event_flags, int read_buffer_size);

/* Lock wrappers for cache functions that are called from main loop. */
void mt_accept_new_conns(const bool do_accept);
conn *mt_conn_from_freelist(void);
bool  mt_conn_add_to_freelist(conn *c);
int   mt_is_listen_thread(void);
void  mt_stats_lock(void);
void  mt_stats_unlock(void);


# define accept_new_conns(x)     mt_accept_new_conns(x)
# define conn_from_freelist()    mt_conn_from_freelist()
# define conn_add_to_freelist(x)   mt_conn_add_to_freelist(x)
# define is_listen_thread()      mt_is_listen_thread()

# define STATS_LOCK()        mt_stats_lock()
# define STATS_UNLOCK()        mt_stats_unlock()

#else /* !USE_THREADS */

# define accept_new_conns(x)     do_accept_new_conns(x)
# define conn_from_freelist()    do_conn_from_freelist()
# define conn_add_to_freelist(x)   do_conn_add_to_freelist(x)
# define dispatch_conn_new(x,y,z,a)  conn_new(x,y,z,a,main_base)
# define dispatch_event_add(t,c)   event_add(&(c)->event, 0)
# define is_listen_thread()      1
# define thread_init(x,y)      /**/

# define STATS_LOCK()        /**/
# define STATS_UNLOCK()        /**/

#endif /* !USE_THREADS */

/* If supported, give compiler hints for branch prediction. */
#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x)     __builtin_expect((x),1)
#define unlikely(x)   __builtin_expect((x),0)

}
