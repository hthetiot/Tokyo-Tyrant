/*************************************************************************************************
 * The server of Tokyo Tyrant
 *                                                               Copyright (C) 2006-2010 FAL Labs
 * This file is part of Tokyo Tyrant.
 * Tokyo Tyrant is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License or any later version.  Tokyo Tyrant is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * You should have received a copy of the GNU Lesser General Public License along with Tokyo
 * Tyrant; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 *************************************************************************************************/


#include <ttutil.h>
#include <tculog.h>
#include <tcrdb.h>
#include "myconf.h"
#include "scrext.h"

#define DEFTHNUM       8                 // default thread number
#define DEFPIDPATH     "ttserver.pid"    // default name of the PID file
#define DEFRTSPATH     "ttserver.rts"    // default name of the RTS file
#define DEFULIMSIZ     (1LL<<30)         // default limit size of an update log file
#define MAXARGSIZ      (256<<20)         // maximum size of each argument
#define MAXARGNUM      (1<<20)           // maximum number of arguments
#define NUMBUFSIZ      32                // size of a numeric buffer
#define LINEBUFSIZ     8192              // size of a line buffer
#define TOKENUNIT      256               // unit number of tokens
#define RECMTXNUM      31                // number of mutexes of records
#define STASHBNUM      1021              // bucket number of the script stash object
#define REPLPERIOD     1.0               // period of calling replication request

enum {                                   // enumeration for command sequential numbers
  TTSEQPUT,                              // sequential number of put command
  TTSEQPUTKEEP,                          // sequential number of putkeep command
  TTSEQPUTCAT,                           // sequential number of putcat command
  TTSEQPUTSHL,                           // sequential number of putshl command
  TTSEQPUTNR,                            // sequential number of putnr command
  TTSEQOUT,                              // sequential number of out command
  TTSEQGET,                              // sequential number of get command
  TTSEQMGET,                             // sequential number of mget command
  TTSEQVSIZ,                             // sequential number of vsiz command
  TTSEQITERINIT,                         // sequential number of iterinit command
  TTSEQITERNEXT,                         // sequential number of iternext command
  TTSEQFWMKEYS,                          // sequential number of fwmkeys command
  TTSEQADDINT,                           // sequential number of addint command
  TTSEQADDDOUBLE,                        // sequential number of adddouble command
  TTSEQEXT,                              // sequential number of ext command
  TTSEQSYNC,                             // sequential number of sync command
  TTSEQOPTIMIZE,                         // sequential number of sync command
  TTSEQVANISH,                           // sequential number of vanish command
  TTSEQCOPY,                             // sequential number of copy command
  TTSEQRESTORE,                          // sequential number of restore command
  TTSEQSETMST,                           // sequential number of setmst command
  TTSEQRNUM,                             // sequential number of rnum command
  TTSEQSIZE,                             // sequential number of size command
  TTSEQSTAT,                             // sequential number of stat command
  TTSEQMISC,                             // sequential number of stat command
  TTSEQREPL,                             // sequential number of repl command
  TTSEQSLAVE,                            // sequential number of slave command
  TTSEQALLORG,                           // sequential number of all commands the original
  TTSEQALLMC,                            // sequential number of all commands the memcached
  TTSEQALLHTTP,                          // sequential number of all commands the HTTP
  TTSEQALLREAD,                          // sequential number of all commands of reading
  TTSEQALLWRITE,                         // sequential number of all commands of writing
  TTSEQALLMANAGE                         // sequential number of all commands of managing
};

enum {                                   // enumeration for command sequential numbers
  TTSEQPUTMISS = TTSEQSLAVE,             // sequential number of misses of get commands
  TTSEQOUTMISS,                          // sequential number of misses of out commands
  TTSEQGETMISS,                          // sequential number of misses of get commands
  TTSEQNUM                               // number of sequential numbers
};

typedef struct {                         // type of structure of logging opaque object
  int fd;
} LOGARG;

typedef struct {                         // type of structure of master synchronous object
  char host[TTADDRBUFSIZ];               // host name
  int port;                              // port number
  const char *rtspath;                   // path of the replication time stamp file
  uint64_t rts;                          // replication time stamp
  int opts;                              // options
  TCADB *adb;                            // database object
  TCULOG *ulog;                          // update log object
  uint32_t sid;                          // server ID number
  bool fail;                             // failure flag
  bool recon;                            // re-connect flag
  bool fatal;                            // fatal error flag
  uint64_t mts;                          // modified time stamp
} REPLARG;

typedef struct {                         // type of structure of periodic opaque object
  const char *name;                      // function name
  TCADB *adb;                            // database object
  TCULOG *ulog;                          // update log object
  uint32_t sid;                          // server ID number
  REPLARG *sarg;                         // replication object
  void *scrext;                          // script extension object
} EXTPCARG;

typedef struct {                         // type of structure of task opaque object
  int thnum;                             // number of threads
  uint64_t *counts;                      // conunters of execution
  uint64_t mask;                         // bit mask of commands
  TCADB *adb;                            // database object
  TCULOG *ulog;                          // update log object
  uint32_t sid;                          // server ID number
  REPLARG *sarg;                         // replication object
  pthread_mutex_t rmtxs[RECMTXNUM];      // mutex for records
  void **screxts;                        // script extension objects
} TASKARG;

typedef struct {                         // type of structure of termination opaque object
  int thnum;                             // number of threads
  TCADB *adb;                            // database object
  REPLARG *sarg;                         // replication object
  void **screxts;                        // script extension objects
  EXTPCARG *pcargs;                      // periodic opaque objects
  int pcnum;                             // number of periodic opaque objects
  bool err;                              // error flag
} TERMARG;


/* global variables */
const char *g_progname = NULL;           // program name
double g_starttime = 0.0;                // start time
TTSERV *g_serv = NULL;                   // server object
int g_loglevel = TTLOGINFO;              // whether to log debug information
bool g_restart = false;                  // restart flag


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static uint64_t getcmdmask(const char *expr);
static void sigtermhandler(int signum);
static void sigchldhandler(int signum);
static int proc(const char *dbname, const char *host, int port, int thnum, int tout,
                bool dmn, const char *pidpath, bool kl, const char *logpath,
                const char *ulogpath, uint64_t ulim, bool uas, uint32_t sid,
                const char *mhost, int mport, const char *rtspath, int ropts,
                const char *skelpath, int mulnum, const char *extpath, const TCLIST *extpcs,
                uint64_t mask);
static void do_log(int level, const char *msg, void *opq);
static void do_slave(void *opq);
static void do_extpc(void *opq);
static void do_task(TTSOCK *sock, void *opq, TTREQ *req);
static char **tokenize(char *str, int *np);
static uint32_t recmtxidx(const char *kbuf, int ksiz);
static uint64_t sumstat(TASKARG *arg, int seq);
static void do_put(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putkeep(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putcat(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putshl(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_putnr(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_out(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_get(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_mget(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_vsiz(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_iterinit(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_iternext(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_fwmkeys(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_addint(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_adddouble(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_ext(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_sync(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_optimize(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_vanish(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_copy(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_restore(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_setmst(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_rnum(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_size(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_stat(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_misc(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_repl(TTSOCK *sock, TASKARG *arg, TTREQ *req);
static void do_mc_set(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_add(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_replace(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_append(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_prepend(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_incr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_decr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_stats(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_flushall(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_version(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_mc_quit(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum);
static void do_http_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_head(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_put(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_post(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_http_options(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri);
static void do_term(void *opq);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  g_starttime = tctime();
  char *dbname = NULL;
  char *host = NULL;
  char *pidpath = NULL;
  char *logpath = NULL;
  char *ulogpath = NULL;
  char *mhost = NULL;
  char *rtspath = NULL;
  char *skelpath = NULL;
  char *extpath = NULL;
  TCLIST *extpcs = NULL;
  int port = TTDEFPORT;
  int thnum = DEFTHNUM;
  int tout = 0;
  bool dmn = false;
  bool kl = false;
  uint64_t ulim = DEFULIMSIZ;
  bool uas = false;
  uint32_t sid = 0;
  int mport = TTDEFPORT;
  int ropts = 0;
  int mulnum = 0;
  uint64_t mask = 0;
  for(int i = 1; i < argc; i++){
    if(!dbname && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-host")){
        if(++i >= argc) usage();
        host = argv[i];
      } else if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-thnum")){
        if(++i >= argc) usage();
        thnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-dmn")){
        dmn = true;
      } else if(!strcmp(argv[i], "-pid")){
        if(++i >= argc) usage();
        pidpath = argv[i];
      } else if(!strcmp(argv[i], "-kl")){
        kl = true;
      } else if(!strcmp(argv[i], "-log")){
        if(++i >= argc) usage();
        logpath = argv[i];
      } else if(!strcmp(argv[i], "-ld")){
        g_loglevel = TTLOGDEBUG;
      } else if(!strcmp(argv[i], "-le")){
        g_loglevel = TTLOGERROR;
      } else if(!strcmp(argv[i], "-ulog")){
        if(++i >= argc) usage();
        ulogpath = argv[i];
      } else if(!strcmp(argv[i], "-ulim")){
        if(++i >= argc) usage();
        ulim = tcatoix(argv[i]);
      } else if(!strcmp(argv[i], "-uas")){
        uas = true;
      } else if(!strcmp(argv[i], "-sid")){
        if(++i >= argc) usage();
        sid = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-mhost")){
        if(++i >= argc) usage();
        mhost = argv[i];
      } else if(!strcmp(argv[i], "-mport")){
        if(++i >= argc) usage();
        mport = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-rts")){
        if(++i >= argc) usage();
        rtspath = argv[i];
      } else if(!strcmp(argv[i], "-rcc")){
        ropts |= RDBROCHKCON;
      } else if(!strcmp(argv[i], "-skel")){
        if(++i >= argc) usage();
        skelpath = argv[i];
      } else if(!strcmp(argv[i], "-mul")){
        if(++i >= argc) usage();
        mulnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-ext")){
        if(++i >= argc) usage();
        extpath = argv[i];
      } else if(!strcmp(argv[i], "-extpc")){
        if(!extpcs) extpcs = tclistnew2(1);
        if(++i >= argc) usage();
        tclistpush2(extpcs, argv[i]);
        if(++i >= argc) usage();
        tclistpush2(extpcs, argv[i]);
      } else if(!strcmp(argv[i], "-mask")){
        if(++i >= argc) usage();
        mask |= getcmdmask(argv[i]);
      } else if(!strcmp(argv[i], "-unmask")){
        if(++i >= argc) usage();
        mask &= ~getcmdmask(argv[i]);
      } else if(!strcmp(argv[i], "--version")){
        printf("Tokyo Tyrant version %s (%d:%s) for %s\n",
               ttversion, _TT_LIBVER, _TT_PROTVER, TTSYSNAME);
        printf("Copyright (C) 2006-2010 FAL Labs\n");
        exit(0);
      } else {
        usage();
      }
    } else if(!dbname){
      dbname = argv[i];
    } else {
      usage();
    }
  }
  if(!dbname) dbname = "*";
  if(thnum < 1 || mport < 1) usage();
  if(dmn && !pidpath) pidpath = DEFPIDPATH;
  if(!rtspath) rtspath = DEFRTSPATH;
  g_serv = ttservnew();
  int rv = proc(dbname, host, port, thnum, tout, dmn, pidpath, kl, logpath,
                ulogpath, ulim, uas, sid, mhost, mport, rtspath, ropts,
                skelpath, mulnum, extpath, extpcs, mask);
  ttservdel(g_serv);
  if(extpcs) tclistdel(extpcs);
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: the server of Tokyo Tyrant\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s [-host name] [-port num] [-thnum num] [-tout num]"
          " [-dmn] [-pid path] [-kl] [-log path] [-ld|-le] [-ulog path] [-ulim num] [-uas]"
          " [-sid num] [-mhost name] [-mport num] [-rts path] [-rcc] [-skel name] [-mul num]"
          " [-ext path] [-extpc name period] [-mask expr] [-unmask expr] [dbname]\n",
          g_progname);
  fprintf(stderr, "\n");
  exit(1);
}


/* get the bit mask of a command name */
static uint64_t getcmdmask(const char *expr){
  uint64_t mask = 0;
  TCLIST *fields = tcstrsplit(expr, " ,");
  for(int i = 0; i < tclistnum(fields); i++){
    const char *name = tclistval2(fields, i);
    if(tcstrifwm(name, "0x")){
      mask |= tcatoih(name);
    } else if(!tcstricmp(name, "put")){
      mask |= 1ULL << TTSEQPUT;
    } else if(!tcstricmp(name, "putkeep")){
      mask |= 1ULL << TTSEQPUTKEEP;
    } else if(!tcstricmp(name, "putcat")){
      mask |= 1ULL << TTSEQPUTCAT;
    } else if(!tcstricmp(name, "putshl")){
      mask |= 1ULL << TTSEQPUTSHL;
    } else if(!tcstricmp(name, "putnr")){
      mask |= 1ULL << TTSEQPUTNR;
    } else if(!tcstricmp(name, "out")){
      mask |= 1ULL << TTSEQOUT;
    } else if(!tcstricmp(name, "get")){
      mask |= 1ULL << TTSEQGET;
    } else if(!tcstricmp(name, "mget")){
      mask |= 1ULL << TTSEQMGET;
    } else if(!tcstricmp(name, "vsiz")){
      mask |= 1ULL << TTSEQVSIZ;
    } else if(!tcstricmp(name, "iterinit")){
      mask |= 1ULL << TTSEQITERINIT;
    } else if(!tcstricmp(name, "iternext")){
      mask |= 1ULL << TTSEQITERNEXT;
    } else if(!tcstricmp(name, "fwmkeys")){
      mask |= 1ULL << TTSEQFWMKEYS;
    } else if(!tcstricmp(name, "addint")){
      mask |= 1ULL << TTSEQADDINT;
    } else if(!tcstricmp(name, "adddouble")){
      mask |= 1ULL << TTSEQADDDOUBLE;
    } else if(!tcstricmp(name, "ext")){
      mask |= 1ULL << TTSEQEXT;
    } else if(!tcstricmp(name, "sync")){
      mask |= 1ULL << TTSEQSYNC;
    } else if(!tcstricmp(name, "optimize")){
      mask |= 1ULL << TTSEQOPTIMIZE;
    } else if(!tcstricmp(name, "vanish")){
      mask |= 1ULL << TTSEQVANISH;
    } else if(!tcstricmp(name, "copy")){
      mask |= 1ULL << TTSEQCOPY;
    } else if(!tcstricmp(name, "restore")){
      mask |= 1ULL << TTSEQRESTORE;
    } else if(!tcstricmp(name, "setmst")){
      mask |= 1ULL << TTSEQSETMST;
    } else if(!tcstricmp(name, "rnum")){
      mask |= 1ULL << TTSEQRNUM;
    } else if(!tcstricmp(name, "size")){
      mask |= 1ULL << TTSEQSIZE;
    } else if(!tcstricmp(name, "stat")){
      mask |= 1ULL << TTSEQSTAT;
    } else if(!tcstricmp(name, "misc")){
      mask |= 1ULL << TTSEQMISC;
    } else if(!tcstricmp(name, "repl")){
      mask |= 1ULL << TTSEQREPL;
    } else if(!tcstricmp(name, "slave")){
      mask |= 1ULL << TTSEQSLAVE;
    } else if(!tcstricmp(name, "all")){
      mask |= UINT64_MAX;
    } else if(!tcstricmp(name, "allorg")){
      mask |= 1ULL << TTSEQALLORG;
    } else if(!tcstricmp(name, "allmc")){
      mask |= 1ULL << TTSEQALLMC;
    } else if(!tcstricmp(name, "allhttp")){
      mask |= 1ULL << TTSEQALLHTTP;
    } else if(!tcstricmp(name, "allread")){
      mask |= 1ULL << TTSEQALLREAD;
    } else if(!tcstricmp(name, "allwrite")){
      mask |= 1ULL << TTSEQALLWRITE;
    } else if(!tcstricmp(name, "allmanage")){
      mask |= 1ULL << TTSEQALLMANAGE;
    }
  }
  tclistdel(fields);
  return mask;
}


/* handle termination signals */
static void sigtermhandler(int signum){
  if(signum == SIGHUP) g_restart = true;
  ttservkill(g_serv);
}


/* handle child event signals */
static void sigchldhandler(int signum){
  return;
}


/* perform the command */
static int proc(const char *dbname, const char *host, int port, int thnum, int tout,
                bool dmn, const char *pidpath, bool kl, const char *logpath,
                const char *ulogpath, uint64_t ulim, bool uas, uint32_t sid,
                const char *mhost, int mport, const char *rtspath, int ropts,
                const char *skelpath, int mulnum, const char *extpath, const TCLIST *extpcs,
                uint64_t mask){
  LOGARG larg;
  larg.fd = 1;
  ttservsetloghandler(g_serv, do_log, &larg);
  if(dmn){
    if(dbname && *dbname != '*' && *dbname != '+' && *dbname != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: dbname(%s) is not the absolute path", dbname);
    if(port == 0 && host && *host != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: host(%s) is not the absolute path", host);
    if(pidpath && *pidpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: pid(%s) is not the absolute path", pidpath);
    if(logpath && *logpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: log(%s) is not the absolute path", logpath);
    if(ulogpath && *ulogpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: ulog(%s) is not the absolute path", ulogpath);
    if(mport == 0 && mhost && *mhost != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: mhost(%s) is not the absolute path", mhost);
    if(mhost && rtspath && *rtspath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: rts(%s) is not the absolute path", rtspath);
    if(skelpath && strchr(skelpath, MYPATHCHR) && *skelpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: skel(%s) is not the absolute path", skelpath);
    if(extpath && *extpath != MYPATHCHR)
      ttservlog(g_serv, TTLOGINFO, "warning: ext(%s) is not the absolute path", extpath);
    if(chdir("/") == -1){
      ttservlog(g_serv, TTLOGERROR, "chdir failed");
      return 1;
    }
  }
  if(!skelpath && dbname && *dbname != '*' && *dbname != '+' && !strstr(dbname, ".tc"))
    ttservlog(g_serv, TTLOGINFO, "warning: dbname(%s) has no suffix for database type", dbname);
  struct stat sbuf;
  if(ulogpath && (stat(ulogpath, &sbuf) != 0 || !S_ISDIR(sbuf.st_mode)))
    ttservlog(g_serv, TTLOGINFO, "warning: ulog(%s) is not a directory", ulogpath);
  if(pidpath){
    char *numstr = tcreadfile(pidpath, -1, NULL);
    if(numstr && kl){
      int64_t pid = tcatoi(numstr);
      tcfree(numstr);
      ttservlog(g_serv, TTLOGINFO,
                "warning: killing the process %lld with SIGTERM", (long long)pid);
      if(kill(pid, SIGTERM) != 0) ttservlog(g_serv, TTLOGERROR, "kill failed");
      int cnt = 0;
      while(true){
        tcsleep(0.1);
        if((numstr = tcreadfile(pidpath, -1, NULL)) != NULL){
          tcfree(numstr);
        } else {
          break;
        }
        if(++cnt >= 100){
          ttservlog(g_serv, TTLOGINFO,
                    "warning: killing the process %lld with SIGKILL", (long long)pid);
          if(kill(pid, SIGKILL) != 0) ttservlog(g_serv, TTLOGERROR, "kill failed");
          unlink(pidpath);
          break;
        }
      }
      numstr = tcreadfile(pidpath, -1, NULL);
    }
    if(numstr){
      int64_t pid = tcatoi(numstr);
      tcfree(numstr);
      ttservlog(g_serv, TTLOGERROR, "the process %lld may be already running", (long long)pid);
      return 1;
    }
  }
  if(sid > UINT16_MAX){
    ttservlog(g_serv, TTLOGINFO,
              "warning: the SID is ignored because it exceeds %d", UINT16_MAX);
    sid = 0;
  }
  if(sid < 1){
    if(ulogpath){
      ttservlog(g_serv, TTLOGINFO,
                "warning: update logging is omitted because the SID is not specified");
      ulogpath = NULL;
    }
    if(mhost){
      ttservlog(g_serv, TTLOGINFO,
                "warning: replication is omitted because the SID is not specified");
      mhost = NULL;
    }
  }
  if(dmn && !ttdaemonize()){
    ttservlog(g_serv, TTLOGERROR, "ttdaemonize failed");
    return 1;
  }
  if(logpath){
    int fd = open(logpath, O_WRONLY | O_APPEND | O_CREAT, 00644);
    if(fd != -1){
      larg.fd = fd;
    } else {
      ttservlog(g_serv, TTLOGERROR, "the log file %s could not be opened", logpath);
      return 1;
    }
  }
  int64_t pid = getpid();
  ttservlog(g_serv, TTLOGSYSTEM, "--------- logging started [%lld] --------", (long long)pid);
  if(pidpath){
    char buf[32];
    sprintf(buf, "%lld\n", (long long)pid);
    if(!tcwritefile(pidpath, buf, strlen(buf))){
      ttservlog(g_serv, TTLOGERROR, "tcwritefile failed");
      return 1;
    }
    ttservlog(g_serv, TTLOGSYSTEM, "process ID configuration: path=%s pid=%lld",
              pidpath, (long long)pid);
  }
  ttservlog(g_serv, TTLOGSYSTEM, "server configuration: host=%s port=%d",
            host ? host : "(any)", port);
  if(!ttservconf(g_serv, host, port)) return 1;
  struct rlimit rlbuf;
  memset(&rlbuf, 0, sizeof(rlbuf));
  if(getrlimit(RLIMIT_NOFILE, &rlbuf) == 0 && rlbuf.rlim_cur != RLIM_INFINITY){
    rlim_t min = rlbuf.rlim_cur;
    for(rlim_t max = INT32_MAX; max > min; max /= 2){
      rlbuf.rlim_cur = max;
      rlbuf.rlim_max = max;
      if(setrlimit(RLIMIT_NOFILE, &rlbuf) == 0) break;
    }
  } else {
    ttservlog(g_serv, TTLOGERROR, "getrlimit failed");
  }
  memset(&rlbuf, 0, sizeof(rlbuf));
  if(getrlimit(RLIMIT_NOFILE, &rlbuf) == 0){
    ttservlog(g_serv, TTLOGSYSTEM, "maximum connection: %d", (int)rlbuf.rlim_cur);
  } else {
    ttservlog(g_serv, TTLOGERROR, "getrlimit failed");
  }
  bool err = false;
  ADBSKEL skel;
  memset(&skel, 0, sizeof(skel));
  void *skellib = NULL;
  if(skelpath){
    ttservlog(g_serv, TTLOGSYSTEM, "skeleton database library: %s", skelpath);
    skellib = dlopen(skelpath, RTLD_LAZY);
    if(!skellib){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "dlopen failed: %s", dlerror());
    }
  }
  TCADB *adb = tcadbnew();
  if(skellib){
    void *initsym = dlsym(skellib, "initialize");
    if(initsym){
      bool (*initfunc)(ADBSKEL *);
      memcpy(&initfunc, &initsym, sizeof(initsym));
      if(initfunc(&skel)){
        if(!tcadbsetskel(adb, &skel)){
          if(skel.opq && skel.del) skel.del(skel.opq);
          err = true;
          ttservlog(g_serv, TTLOGERROR, "tcadbsetskel failed");
        }
      } else {
        if(skel.opq && skel.del) skel.del(skel.opq);
        err = true;
        ttservlog(g_serv, TTLOGERROR, "initialize failed");
      }
    } else {
      err = true;
      ttservlog(g_serv, TTLOGERROR, "dlsym failed: %s", dlerror());
    }
  }
  ttservlog(g_serv, TTLOGSYSTEM, "opening the database: %s", dbname);
  if(mulnum > 0 && !tcadbsetskelmulti(adb, mulnum)){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "tcadbsetskelmulti failed");
  }
  if(!tcadbopen(adb, dbname)){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "tcadbopen failed");
  }
  TCULOG *ulog = tculognew();
  if(ulogpath){
    ttservlog(g_serv, TTLOGSYSTEM,
              "update log configuration: path=%s limit=%llu async=%d sid=%d",
              ulogpath, (unsigned long long)ulim, uas, sid);
    if(uas && !tculogsetaio(ulog)){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "tculogsetaio failed");
    }
    if(!tculogopen(ulog, ulogpath, ulim)){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "tculogopen failed");
    }
  }
  ttservtune(g_serv, thnum, tout);
  if(mhost)
    ttservlog(g_serv, TTLOGSYSTEM, "replication configuration: host=%s port=%d ropts=%d",
              mhost, mport, ropts);
  uint64_t *counts = tccalloc(sizeof(*counts), (TTSEQNUM) * thnum);
  void *screxts[thnum];
  TCMDB *scrstash = NULL;
  TCMDB *scrlock = NULL;
  pthread_mutex_t *scrlcks = NULL;
  if(extpath){
    ttservlog(g_serv, TTLOGSYSTEM, "scripting extension: %s", extpath);
    scrstash = tcmdbnew2(STASHBNUM);
    scrlock = tcmdbnew2(thnum * 2 + 1);
    bool screrr = false;
    for(int i = 0; i < thnum; i++){
      screxts[i] = NULL;
    }
    for(int i = 0; i < thnum; i++){
      screxts[i] = scrextnew(screxts, thnum, i, extpath, adb, ulog, sid, scrstash, scrlock,
                             do_log, &larg);
      if(!screxts[i]) screrr = true;
    }
    if(screrr){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "scrextnew failed");
    }
  } else {
    for(int i = 0; i < thnum; i++){
      screxts[i] = NULL;
    }
  }
  if(mask != 0)
    ttservlog(g_serv, TTLOGSYSTEM, "command bit mask: 0x%llx", (unsigned long long)mask);
  REPLARG sarg;
  snprintf(sarg.host, TTADDRBUFSIZ, "%s", mhost ? mhost : "");
  sarg.port = mport;
  sarg.rtspath = rtspath;
  sarg.rts = 0;
  sarg.opts = ropts;
  sarg.adb = adb;
  sarg.ulog = ulog;
  sarg.sid = sid;
  sarg.fail = false;
  sarg.recon = false;
  sarg.fatal = false;
  sarg.mts = 0;
  if(!(mask & (1ULL << TTSEQSLAVE))) ttservaddtimedhandler(g_serv, REPLPERIOD, do_slave, &sarg);
  EXTPCARG *pcargs = NULL;
  int pcnum = 0;
  if(extpath && extpcs){
    pcnum = tclistnum(extpcs) / 2;
    pcargs = tcmalloc(sizeof(*pcargs) * pcnum);
    for(int i = 0; i < pcnum; i++){
      const char *name = tclistval2(extpcs, i * 2);
      double period = tcatof(tclistval2(extpcs, i * 2 + 1));
      EXTPCARG *pcarg = pcargs + i;
      pcarg->name = name;
      pcarg->adb = adb;
      pcarg->ulog = ulog;
      pcarg->sid = sid;
      pcarg->sarg = &sarg;
      pcarg->scrext = scrextnew(screxts, thnum, thnum + i, extpath, adb, ulog, sid,
                                scrstash, scrlock, do_log, &larg);
      if(pcarg->scrext){
        if(*name && period > 0) ttservaddtimedhandler(g_serv, period, do_extpc, pcarg);
      } else {
        err = true;
        ttservlog(g_serv, TTLOGERROR, "scrextnew failed");
      }
    }
  }
  TASKARG targ;
  targ.thnum = thnum;
  targ.counts = counts;
  targ.mask = mask;
  targ.adb = adb;
  targ.ulog = ulog;
  targ.sid = sid;
  targ.sarg = &sarg;
  for(int i = 0; i < RECMTXNUM; i++){
    if(pthread_mutex_init(targ.rmtxs + i, NULL) != 0)
      ttservlog(g_serv, TTLOGERROR, "pthread_mutex_init failed");
  }
  targ.screxts = screxts;
  ttservsettaskhandler(g_serv, do_task, &targ);
  TERMARG karg;
  karg.thnum = thnum;
  karg.adb = adb;
  karg.sarg = &sarg;
  karg.screxts = screxts;
  karg.pcargs = pcargs;
  karg.pcnum = pcnum;
  karg.err = false;
  ttservsettermhandler(g_serv, do_term, &karg);
  if(larg.fd != 1){
    close(larg.fd);
    larg.fd = 1;
  }
  do {
    g_restart = false;
    if(logpath){
      int fd = open(logpath, O_WRONLY | O_APPEND | O_CREAT, 00644);
      if(fd != -1){
        larg.fd = fd;
      } else {
        err = true;
        ttservlog(g_serv, TTLOGERROR, "open failed");
      }
    }
    if(signal(SIGTERM, sigtermhandler) == SIG_ERR || signal(SIGINT, sigtermhandler) == SIG_ERR ||
       signal(SIGHUP, sigtermhandler) == SIG_ERR || signal(SIGPIPE, SIG_IGN) == SIG_ERR ||
       signal(SIGCHLD, sigchldhandler) == SIG_ERR){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "signal failed");
    }
    if(!ttservstart(g_serv)) err = true;
  } while(g_restart);
  if(karg.err) err = true;
  if(pcargs){
    for(int i = 0; i < pcnum; i++){
      EXTPCARG *pcarg = pcargs + i;
      if(!pcarg->scrext) continue;
      if(!scrextdel(pcarg->scrext)){
        err = true;
        ttservlog(g_serv, TTLOGERROR, "scrextdel failed");
      }
    }
    tcfree(pcargs);
  }
  for(int i = 0; i < RECMTXNUM; i++){
    if(pthread_mutex_destroy(targ.rmtxs + i) != 0)
      ttservlog(g_serv, TTLOGERROR, "pthread_mutex_destroy failed");
  }
  for(int i = 0; i < thnum; i++){
    if(!screxts[i]) continue;
    if(!scrextdel(screxts[i])){
      err = true;
      ttservlog(g_serv, TTLOGERROR, "scrextdel failed");
    }
  }
  if(scrlcks){
    for(int i = 0; i < RECMTXNUM; i++){
      if(pthread_mutex_destroy(scrlcks + i) != 0)
        ttservlog(g_serv, TTLOGERROR, "pthread_mutex_destroy failed");
    }
    tcfree(scrlcks);
  }
  if(scrlock) tcmdbdel(scrlock);
  if(scrstash) tcmdbdel(scrstash);
  tcfree(counts);
  if(ulogpath && !tculogclose(ulog)){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "tculogclose failed");
  }
  tculogdel(ulog);
  tcadbdel(adb);
  if(skellib && dlclose(skellib) != 0){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "dlclose failed");
  }
  if(pidpath && unlink(pidpath) != 0){
    err = true;
    ttservlog(g_serv, TTLOGERROR, "unlink failed");
  }
  ttservlog(g_serv, TTLOGSYSTEM, "--------- logging finished [%d] --------", pid);
  if(logpath && close(larg.fd) == -1) err = true;
  return err ? 1 : 0;
}


/* handle a log message */
static void do_log(int level, const char *msg, void *opq){
  if(level < g_loglevel) return;
  LOGARG *arg = (LOGARG *)opq;
  char date[48];
  tcdatestrwww(INT64_MAX, INT_MAX, date);
  const char *lvstr = "unknown";
  switch(level){
    case TTLOGDEBUG: lvstr = "DEBUG"; break;
    case TTLOGINFO: lvstr = "INFO"; break;
    case TTLOGERROR: lvstr = "ERROR"; break;
    case TTLOGSYSTEM: lvstr = "SYSTEM"; break;
  }
  char buf[LINEBUFSIZ];
  int len = snprintf(buf, LINEBUFSIZ, "%s\t%s\t%s\n", date, lvstr, msg);
  if(len >= LINEBUFSIZ){
    buf[LINEBUFSIZ-1] = '\n';
    len = LINEBUFSIZ;
  }
  tcwrite(arg ? arg->fd : 1, buf, len);
}


/* replicate master data */
static void do_slave(void *opq){
  REPLARG *arg = opq;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(arg->fatal) return;
  if(arg->host[0] == '\0' || arg->port < 1) return;
  if(arg->mts > 0){
    char rtsbuf[NUMBUFSIZ];
    int len = sprintf(rtsbuf, "%llu\n", (unsigned long long)arg->mts);
    if(!tcwritefile(arg->rtspath, rtsbuf, len))
      ttservlog(g_serv, TTLOGERROR, "do_slave: tcwritefile failed");
    arg->mts = 0;
  }
  int rtsfd = open(arg->rtspath, O_RDWR | O_CREAT, 00644);
  if(rtsfd == -1){
    ttservlog(g_serv, TTLOGERROR, "do_slave: open failed");
    return;
  }
  struct stat sbuf;
  if(fstat(rtsfd, &sbuf) == -1){
    ttservlog(g_serv, TTLOGERROR, "do_slave: stat failed");
    close(rtsfd);
    return;
  }
  char rtsbuf[NUMBUFSIZ];
  memset(rtsbuf, 0, NUMBUFSIZ);
  arg->rts = 0;
  if(sbuf.st_size > 0 && tcread(rtsfd, rtsbuf, tclmin(NUMBUFSIZ - 1, sbuf.st_size)))
    arg->rts = tcatoi(rtsbuf);
  TCREPL *repl = tcreplnew();
  pthread_cleanup_push((void (*)(void *))tcrepldel, repl);
  if(tcreplopen(repl, arg->host, arg->port, arg->rts + 1, sid)){
    ttservlog(g_serv, TTLOGINFO, "replicating from sid=%u (%s:%d) after %llu",
              repl->mid, arg->host, arg->port, (unsigned long long)arg->rts);
    arg->fail = false;
    arg->recon = false;
    bool err = false;
    uint32_t rsid;
    const char *rbuf;
    int rsiz;
    uint64_t rts;
    while(!err && !ttserviskilled(g_serv) && !arg->recon &&
          (rbuf = tcreplread(repl, &rsiz, &rts, &rsid)) != NULL){
      if(rsiz < 1) continue;
      bool cc;
      if(!tculogadbredo(adb, rbuf, rsiz, ulog, rsid, repl->mid, &cc)){
        err = true;
        ttservlog(g_serv, TTLOGERROR, "do_slave: tculogadbredo failed");
      } else if(!cc){
        if(arg->opts & RDBROCHKCON){
          err = true;
          arg->fatal = true;
          ttservlog(g_serv, TTLOGERROR, "do_slave: detected inconsistency");
        } else {
          ttservlog(g_serv, TTLOGINFO, "do_slave: detected inconsistency");
        }
      }
      if(lseek(rtsfd, 0, SEEK_SET) != -1){
        int len = sprintf(rtsbuf, "%llu\n", (unsigned long long)rts);
        if(tcwrite(rtsfd, rtsbuf, len)){
          arg->rts = rts;
        } else {
          err = true;
          ttservlog(g_serv, TTLOGERROR, "do_slave: tcwrite failed");
        }
      } else {
        err = true;
        ttservlog(g_serv, TTLOGERROR, "do_slave: lseek failed");
      }
    }
    tcreplclose(repl);
    ttservlog(g_serv, TTLOGINFO, "replication finished");
  } else {
    if(!arg->fail) ttservlog(g_serv, TTLOGERROR, "do_slave: tcreplopen failed");
    arg->fail = true;
  }
  pthread_cleanup_pop(1);
  if(close(rtsfd) == -1) ttservlog(g_serv, TTLOGERROR, "do_slave: close failed");
}


/* perform an extension command */
static void do_extpc(void *opq){
  EXTPCARG *arg = (EXTPCARG *)opq;
  const char *name = arg->name;
  void *scr = arg->scrext;
  int xsiz;
  char *xbuf = scrextcallmethod(scr, name, "", 0, "", 0, &xsiz);
  tcfree(xbuf);
}


/* handle a task and dispatch it */
static void do_task(TTSOCK *sock, void *opq, TTREQ *req){
  TASKARG *arg = (TASKARG *)opq;
  int c = ttsockgetc(sock);
  if(c == TTMAGICNUM){
    switch(ttsockgetc(sock)){
      case TTCMDPUT:
        do_put(sock, arg, req);
        break;
      case TTCMDPUTKEEP:
        do_putkeep(sock, arg, req);
        break;
      case TTCMDPUTCAT:
        do_putcat(sock, arg, req);
        break;
      case TTCMDPUTSHL:
        do_putshl(sock, arg, req);
        break;
      case TTCMDPUTNR:
        do_putnr(sock, arg, req);
        break;
      case TTCMDOUT:
        do_out(sock, arg, req);
        break;
      case TTCMDGET:
        do_get(sock, arg, req);
        break;
      case TTCMDMGET:
        do_mget(sock, arg, req);
        break;
      case TTCMDVSIZ:
        do_vsiz(sock, arg, req);
        break;
      case TTCMDITERINIT:
        do_iterinit(sock, arg, req);
        break;
      case TTCMDITERNEXT:
        do_iternext(sock, arg, req);
        break;
      case TTCMDFWMKEYS:
        do_fwmkeys(sock, arg, req);
        break;
      case TTCMDADDINT:
        do_addint(sock, arg, req);
        break;
      case TTCMDADDDOUBLE:
        do_adddouble(sock, arg, req);
        break;
      case TTCMDEXT:
        do_ext(sock, arg, req);
        break;
      case TTCMDSYNC:
        do_sync(sock, arg, req);
        break;
      case TTCMDOPTIMIZE:
        do_optimize(sock, arg, req);
        break;
      case TTCMDVANISH:
        do_vanish(sock, arg, req);
        break;
      case TTCMDCOPY:
        do_copy(sock, arg, req);
        break;
      case TTCMDRESTORE:
        do_restore(sock, arg, req);
        break;
      case TTCMDSETMST:
        do_setmst(sock, arg, req);
        break;
      case TTCMDRNUM:
        do_rnum(sock, arg, req);
        break;
      case TTCMDSIZE:
        do_size(sock, arg, req);
        break;
      case TTCMDSTAT:
        do_stat(sock, arg, req);
        break;
      case TTCMDMISC:
        do_misc(sock, arg, req);
        break;
      case TTCMDREPL:
        do_repl(sock, arg, req);
        break;
      default:
        ttservlog(g_serv, TTLOGINFO, "unknown command");
        break;
    }
  } else {
    ttsockungetc(sock, c);
    char *line = ttsockgets2(sock);
    if(line){
      pthread_cleanup_push(tcfree, line);
      int tnum;
      char **tokens = tokenize(line, &tnum);
      pthread_cleanup_push(tcfree, tokens);
      if(tnum > 0){
        const char *cmd = tokens[0];
        if(!strcmp(cmd, "set")){
          do_mc_set(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "add")){
          do_mc_add(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "replace")){
          do_mc_replace(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "append")){
          do_mc_append(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "prepend")){
          do_mc_prepend(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "get") || !strcmp(cmd, "gets")){
          do_mc_get(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "delete")){
          do_mc_delete(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "incr")){
          do_mc_incr(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "decr")){
          do_mc_decr(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "stats")){
          do_mc_stats(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "flush_all")){
          do_mc_flushall(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "version")){
          do_mc_version(sock, arg, req, tokens, tnum);
        } else if(!strcmp(cmd, "quit")){
          do_mc_quit(sock, arg, req, tokens, tnum);
        } else if(tnum > 2 && tcstrfwm(tokens[2], "HTTP/1.")){
          int ver = tcatoi(tokens[2] + 7);
          const char *uri = tokens[1];
          if(tcstrifwm(uri, "http://")){
            const char *pv = strchr(uri + 7, '/');
            if(pv) uri = pv;
          }
          if(!strcmp(cmd, "GET")){
            do_http_get(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "HEAD")){
            do_http_head(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "PUT")){
            do_http_put(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "POST")){
            do_http_post(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "DELETE")){
            do_http_delete(sock, arg, req, ver, uri);
          } else if(!strcmp(cmd, "OPTIONS")){
            do_http_options(sock, arg, req, ver, uri);
          }
        }
      }
      pthread_cleanup_pop(1);
      pthread_cleanup_pop(1);
    }
  }
}


/* tokenize a string */
static char **tokenize(char *str, int *np){
  int anum = TOKENUNIT;
  char **tokens = tcmalloc(sizeof(*tokens) * anum);
  int tnum = 0;
  while(*str == ' ' || *str == '\t'){
    str++;
  }
  while(*str != '\0'){
    if(tnum >= anum){
      anum *= 2;
      tokens = tcrealloc(tokens, sizeof(*tokens) * anum);
    }
    tokens[tnum++] = str;
    while(*str != '\0' && *str != ' ' && *str != '\t'){
      str++;
    }
    while(*str == ' ' || *str == '\t'){
      *(str++) = '\0';
    }
  }
  *np = tnum;
  return tokens;
}


/* get the mutex index of a record */
static uint32_t recmtxidx(const char *kbuf, int ksiz){
  uint32_t hash = 725;
  while(ksiz--){
    hash = hash * 29 + *(uint8_t *)kbuf++;
  }
  return hash % RECMTXNUM;
}


/* get the summation of status information of a command */
static uint64_t sumstat(TASKARG *arg, int seq){
  int thnum = arg->thnum;
  uint64_t *counts = arg->counts;
  uint64_t sum = 0;
  for(int i = 0; i < thnum; i++){
    sum += counts[TTSEQNUM*i+seq];
  }
  return sum;
}


/* handle the put command */
static void do_put(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing put command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_put: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_put: forbidden");
    } else if(!tculogadbput(ulog, sid, 0, adb, buf, ksiz, buf + ksiz, vsiz)){
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_put: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_put: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_put: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putkeep command */
static void do_putkeep(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putkeep command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUTKEEP]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_putkeep: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQPUTKEEP) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putkeep: forbidden");
    } else if(!tculogadbputkeep(ulog, sid, 0, adb, buf, ksiz, buf + ksiz, vsiz)){
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      code = 1;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_putkeep: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putkeep: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putcat command */
static void do_putcat(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putcat command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUTCAT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_putcat: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQPUTCAT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putcat: forbidden");
    } else if(!tculogadbputcat(ulog, sid, 0, adb, buf, ksiz, buf + ksiz, vsiz)){
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_putcat: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_putcat: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putcat: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putshl command */
static void do_putshl(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putshl command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUTSHL]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  int width = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ ||
     width < 0){
    ttservlog(g_serv, TTLOGINFO, "do_putshl: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQPUTSHL) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putshl: forbidden");
    } else if(!tculogadbputshl(ulog, sid, 0, adb, buf, ksiz, buf + ksiz, vsiz, width)){
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_putshl: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_putshl: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putshl: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the putnr command */
static void do_putnr(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing putnr command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUTNR]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_putnr: invalid parameters");
    return;
  }
  int rsiz = ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQPUTNR) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_putnr: forbidden");
    } else if(!tculogadbput(ulog, sid, 0, adb, buf, ksiz, buf + ksiz, vsiz)){
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_putnr: operation failed");
    }
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_putnr: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the out command */
static void do_out(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing out command");
  arg->counts[TTSEQNUM*req->idx+TTSEQOUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_out: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQOUT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_out: forbidden");
    } else if(!tculogadbout(ulog, sid, 0, adb, buf, ksiz)){
      arg->counts[TTSEQNUM*req->idx+TTSEQOUTMISS]++;
      code = 1;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_out: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_out: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the get command */
static void do_get(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing get command");
  arg->counts[TTSEQNUM*req->idx+TTSEQGET]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int ksiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_get: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    char *vbuf;
    int vsiz;
    if(mask & ((1ULL << TTSEQGET) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
      vbuf = NULL;
      vsiz = 0;
      ttservlog(g_serv, TTLOGINFO, "do_get: forbidden");
    } else {
      vbuf = tcadbget(adb, buf, ksiz, &vsiz);
    }
    if(vbuf){
      int rsiz = vsiz + sizeof(uint8_t) + sizeof(uint32_t);
      char *rbuf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz);
      pthread_cleanup_push(free, (rbuf == stack) ? NULL : rbuf);
      *rbuf = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)vsiz);
      memcpy(rbuf + sizeof(uint8_t), &num, sizeof(uint32_t));
      memcpy(rbuf + sizeof(uint8_t) + sizeof(uint32_t), vbuf, vsiz);
      tcfree(vbuf);
      if(ttsocksend(sock, rbuf, rsiz)){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_get: response failed");
      }
      pthread_cleanup_pop(1);
    } else {
      arg->counts[TTSEQNUM*req->idx+TTSEQGETMISS]++;
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_get: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_get: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the mget command */
static void do_mget(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing mget command");
  arg->counts[TTSEQNUM*req->idx+TTSEQMGET]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int rnum = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || rnum < 0 || rnum > MAXARGNUM){
    ttservlog(g_serv, TTLOGINFO, "do_mget: invalid parameters");
    return;
  }
  TCLIST *keys = tclistnew2(rnum);
  pthread_cleanup_push((void (*)(void *))tclistdel, keys);
  char stack[TTIOBUFSIZ];
  for(int i = 0; i < rnum; i++){
    int ksiz = ttsockgetint32(sock);
    if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ) break;
    char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
    pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
    if(ttsockrecv(sock, buf, ksiz)) tclistpush(keys, buf, ksiz);
    pthread_cleanup_pop(1);
  }
  if(!ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    uint8_t code = 0;
    tcxstrcat(xstr, &code, sizeof(code));
    uint32_t num = 0;
    tcxstrcat(xstr, &num, sizeof(num));
    rnum = 0;
    if(mask & ((1ULL << TTSEQMGET) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
      ttservlog(g_serv, TTLOGINFO, "do_mget: forbidden");
    } else {
      for(int i = 0; i < tclistnum(keys); i++){
        int ksiz;
        const char *kbuf = tclistval(keys, i, &ksiz);
        int vsiz;
        char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
        if(vbuf){
          num = TTHTONL((uint32_t)ksiz);
          tcxstrcat(xstr, &num, sizeof(num));
          num = TTHTONL((uint32_t)vsiz);
          tcxstrcat(xstr, &num, sizeof(num));
          tcxstrcat(xstr, kbuf, ksiz);
          tcxstrcat(xstr, vbuf, vsiz);
          tcfree(vbuf);
          rnum++;
        }
      }
    }
    num = TTHTONL((uint32_t)rnum);
    memcpy((char *)tcxstrptr(xstr) + sizeof(code), &num, sizeof(num));
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mget: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mget: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the vsiz command */
static void do_vsiz(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing vsiz command");
  arg->counts[TTSEQNUM*req->idx+TTSEQVSIZ]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int ksiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_vsiz: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    int vsiz;
    if(mask & ((1ULL << TTSEQVSIZ) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
      vsiz = -1;
      ttservlog(g_serv, TTLOGINFO, "do_vsiz: forbidden");
    } else {
      vsiz = tcadbvsiz(adb, buf, ksiz);
    }
    if(vsiz >= 0){
      *stack = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)vsiz);
      memcpy(stack + sizeof(uint8_t), &num, sizeof(uint32_t));
      if(ttsocksend(sock, stack, sizeof(uint8_t) + sizeof(uint32_t))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_vsiz: response failed");
      }
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_vsiz: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_vsiz: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the iterinit command */
static void do_iterinit(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing iterinit command");
  arg->counts[TTSEQNUM*req->idx+TTSEQITERINIT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  uint8_t code = 0;
  if(mask & ((1ULL << TTSEQITERINIT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
    code = 1;
    ttservlog(g_serv, TTLOGINFO, "do_iterinit: forbidden");
  } else if(!tcadbiterinit(adb)){
    code = 1;
    ttservlog(g_serv, TTLOGERROR, "do_iterinit: operation failed");
  }
  if(ttsocksend(sock, &code, sizeof(code))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_iterinit: response failed");
  }
}


/* handle the iternext command */
static void do_iternext(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing iternext command");
  arg->counts[TTSEQNUM*req->idx+TTSEQITERNEXT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int vsiz;
  char *vbuf;
  if(mask & ((1ULL << TTSEQITERNEXT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
    vbuf = NULL;
    vsiz = 0;
    ttservlog(g_serv, TTLOGINFO, "do_iternext: forbidden");
  } else {
    vbuf = tcadbiternext(adb, &vsiz);
  }
  if(vbuf){
    int rsiz = vsiz + sizeof(uint8_t) + sizeof(uint32_t);
    char stack[TTIOBUFSIZ];
    char *rbuf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz);
    pthread_cleanup_push(free, (rbuf == stack) ? NULL : rbuf);
    *rbuf = 0;
    uint32_t num;
    num = TTHTONL((uint32_t)vsiz);
    memcpy(rbuf + sizeof(uint8_t), &num, sizeof(uint32_t));
    memcpy(rbuf + sizeof(uint8_t) + sizeof(uint32_t), vbuf, vsiz);
    tcfree(vbuf);
    if(ttsocksend(sock, rbuf, rsiz)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_iternext: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    uint8_t code = 1;
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_iternext: response failed");
    }
  }
}


/* handle the fwmkeys command */
static void do_fwmkeys(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing fwmkeys command");
  arg->counts[TTSEQNUM*req->idx+TTSEQFWMKEYS]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int psiz = ttsockgetint32(sock);
  int max = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    TCLIST *keys = tcadbfwmkeys(adb, buf, psiz, max);
    pthread_cleanup_push((void (*)(void *))tclistdel, keys);
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    uint8_t code = 0;
    tcxstrcat(xstr, &code, sizeof(code));
    uint32_t num = 0;
    tcxstrcat(xstr, &num, sizeof(num));
    int knum = 0;
    if(mask & ((1ULL << TTSEQFWMKEYS) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
      ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: forbidden");
    } else {
      for(int i = 0; i < tclistnum(keys); i++){
        int ksiz;
        const char *kbuf = tclistval(keys, i, &ksiz);
        num = TTHTONL((uint32_t)ksiz);
        tcxstrcat(xstr, &num, sizeof(num));
        tcxstrcat(xstr, kbuf, ksiz);
        knum++;
      }
    }
    num = TTHTONL((uint32_t)knum);
    memcpy((char *)tcxstrptr(xstr) + sizeof(code), &num, sizeof(num));
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: response failed");
    }
    pthread_cleanup_pop(1);
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_fwmkeys: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the addint command */
static void do_addint(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing addint command");
  arg->counts[TTSEQNUM*req->idx+TTSEQADDINT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  int anum = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_addint: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    int snum;
    if(mask & ((1ULL << TTSEQADDINT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      snum = INT_MIN;
      ttservlog(g_serv, TTLOGINFO, "do_addint: forbidden");
    } else {
      snum = tculogadbaddint(ulog, sid, 0, adb, buf, ksiz, anum);
    }
    if(snum != INT_MIN){
      *stack = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)snum);
      memcpy(stack + sizeof(uint8_t), &num, sizeof(uint32_t));
      if(ttsocksend(sock, stack, sizeof(uint8_t) + sizeof(uint32_t))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_addint: response failed");
      }
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_addint: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_addint: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the adddouble command */
static void do_adddouble(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing adddouble command");
  arg->counts[TTSEQNUM*req->idx+TTSEQADDDOUBLE]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int ksiz = ttsockgetint32(sock);
  char abuf[sizeof(uint64_t)*2];
  if(!ttsockrecv(sock, abuf, sizeof(abuf)) || ttsockcheckend(sock) ||
     ksiz < 0 || ksiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_adddouble: invalid parameters");
    return;
  }
  double anum = ttunpackdouble(abuf);
  char stack[TTIOBUFSIZ];
  char *buf = (ksiz < TTIOBUFSIZ) ? stack : tcmalloc(ksiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, ksiz) && !ttsockcheckend(sock)){
    double snum;
    if(mask & ((1ULL << TTSEQADDDOUBLE) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      snum = nan("");
      ttservlog(g_serv, TTLOGINFO, "do_adddouble: forbidden");
    } else {
      snum = tculogadbadddouble(ulog, sid, 0, adb, buf, ksiz, anum);
    }
    if(!isnan(snum)){
      *stack = 0;
      ttpackdouble(snum, abuf);
      memcpy(stack + sizeof(uint8_t), abuf, sizeof(abuf));
      if(ttsocksend(sock, stack, sizeof(uint8_t) + sizeof(abuf))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_adddouble: response failed");
      }
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_adddouble: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_adddouble: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the ext command */
static void do_ext(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing ext command");
  arg->counts[TTSEQNUM*req->idx+TTSEQEXT]++;
  uint64_t mask = arg->mask;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  void *scr = arg->screxts[req->idx];
  int nsiz = ttsockgetint32(sock);
  int opts = ttsockgetint32(sock);
  int ksiz = ttsockgetint32(sock);
  int vsiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || nsiz < 0 || nsiz >= TTADDRBUFSIZ ||
     ksiz < 0 || ksiz > MAXARGSIZ || vsiz < 0 || vsiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_ext: invalid parameters");
    return;
  }
  int rsiz = nsiz + ksiz + vsiz;
  char stack[TTIOBUFSIZ];
  char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, rsiz) && !ttsockcheckend(sock)){
    char name[TTADDRBUFSIZ];
    memcpy(name, buf, nsiz);
    name[nsiz] = '\0';
    const char *kbuf = buf + nsiz;
    const char *vbuf = kbuf + ksiz;
    int xsiz = 0;
    char *xbuf = NULL;
    if(mask & ((1ULL << TTSEQEXT) | (1ULL << TTSEQALLORG))){
      ttservlog(g_serv, TTLOGINFO, "do_ext: forbidden");
    } else if(scr){
      if(opts & RDBXOLCKGLB){
        bool err = false;
        for(int i = 0; i < RECMTXNUM; i++){
          if(pthread_mutex_lock(rmtxs + i) != 0){
            ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_lock failed");
            while(--i >= 0){
              pthread_mutex_unlock(rmtxs + i);
            }
            err = true;
            break;
          }
        }
        if(!err){
          xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
          for(int i = RECMTXNUM - 1; i >= 0; i--){
            if(pthread_mutex_unlock(rmtxs + i) != 0)
              ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_unlock failed");
          }
        }
      } else if(opts & RDBXOLCKREC){
        int mtxidx = recmtxidx(kbuf, ksiz);
        if(pthread_mutex_lock(rmtxs + mtxidx) == 0){
          xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
          if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
            ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_unlock failed");
        } else {
          ttservlog(g_serv, TTLOGERROR, "do_ext: pthread_mutex_lock failed");
        }
      } else {
        xbuf = scrextcallmethod(scr, name, kbuf, ksiz, vbuf, vsiz, &xsiz);
      }
    }
    if(xbuf){
      int rsiz = xsiz + sizeof(uint8_t) + sizeof(uint32_t);
      char *rbuf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz);
      pthread_cleanup_push(free, (rbuf == stack) ? NULL : rbuf);
      *rbuf = 0;
      uint32_t num;
      num = TTHTONL((uint32_t)xsiz);
      memcpy(rbuf + sizeof(uint8_t), &num, sizeof(uint32_t));
      memcpy(rbuf + sizeof(uint8_t) + sizeof(uint32_t), xbuf, xsiz);
      tcfree(xbuf);
      if(ttsocksend(sock, rbuf, rsiz)){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_ext: response failed");
      }
      pthread_cleanup_pop(1);
    } else {
      uint8_t code = 1;
      if(ttsocksend(sock, &code, sizeof(code))){
        req->keep = true;
      } else {
        ttservlog(g_serv, TTLOGINFO, "do_ext: response failed");
      }
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_ext: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the sync command */
static void do_sync(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing sync command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSYNC]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  uint8_t code = 0;
  if(mask & ((1ULL << TTSEQSYNC) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLMANAGE))){
    code = 1;
    ttservlog(g_serv, TTLOGINFO, "do_sync: forbidden");
  } else if(!tculogadbsync(ulog, sid, 0, adb)){
    code = 1;
    ttservlog(g_serv, TTLOGERROR, "do_sync: operation failed");
  }
  if(ttsocksend(sock, &code, sizeof(code))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_sync: response failed");
  }
}


/* handle the optimize command */
static void do_optimize(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing optimize command");
  arg->counts[TTSEQNUM*req->idx+TTSEQOPTIMIZE]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int psiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_optimize: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    buf[psiz] = '\0';
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQOPTIMIZE) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLMANAGE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_optimize: forbidden");
    } else if(!tculogadboptimize(ulog, sid, 0, adb, buf)){
      code = 1;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_optimize: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_optimize: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the vanish command */
static void do_vanish(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing vanish command");
  arg->counts[TTSEQNUM*req->idx+TTSEQVANISH]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  uint8_t code = 0;
  if(mask & ((1ULL << TTSEQVANISH) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
    code = 1;
    ttservlog(g_serv, TTLOGINFO, "do_vanish: forbidden");
  } else if(!tculogadbvanish(ulog, sid, 0, adb)){
    code = 1;
    ttservlog(g_serv, TTLOGERROR, "do_vanish: operation failed");
  }
  if(ttsocksend(sock, &code, sizeof(code))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_vanish: response failed");
  }
}


/* handle the copy command */
static void do_copy(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing copy command");
  arg->counts[TTSEQNUM*req->idx+TTSEQCOPY]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  int psiz = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_copy: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    buf[psiz] = '\0';
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQCOPY) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLMANAGE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_copy: forbidden");
    } else if(!tcadbcopy(adb, buf)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_copy: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_copy: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_copy: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the restore command */
static void do_restore(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing restore command");
  arg->counts[TTSEQNUM*req->idx+TTSEQRESTORE]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  int psiz = ttsockgetint32(sock);
  uint64_t ts = ttsockgetint64(sock);
  int opts = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || psiz < 0 || psiz > MAXARGSIZ){
    ttservlog(g_serv, TTLOGINFO, "do_restore: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (psiz < TTIOBUFSIZ) ? stack : tcmalloc(psiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, psiz) && !ttsockcheckend(sock)){
    buf[psiz] = '\0';
    bool con = (opts & RDBROCHKCON) != 0;
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQRESTORE) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLMANAGE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_restore: forbidden");
    } else if(!tculogadbrestore(adb, buf, ts, con, ulog)){
      code = 1;
      ttservlog(g_serv, TTLOGERROR, "do_restore: operation failed");
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_restore: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_restore: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the setmst command */
static void do_setmst(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing setmst command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSETMST]++;
  uint64_t mask = arg->mask;
  REPLARG *sarg = arg->sarg;
  int hsiz = ttsockgetint32(sock);
  int port = ttsockgetint32(sock);
  uint64_t ts = ttsockgetint64(sock);
  int opts = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || hsiz < 0 || hsiz > MAXARGSIZ || port < 0){
    ttservlog(g_serv, TTLOGINFO, "do_setmst: invalid parameters");
    return;
  }
  char stack[TTIOBUFSIZ];
  char *buf = (hsiz < TTIOBUFSIZ) ? stack : tcmalloc(hsiz + 1);
  pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
  if(ttsockrecv(sock, buf, hsiz) && !ttsockcheckend(sock)){
    buf[hsiz] = '\0';
    uint8_t code = 0;
    if(mask & ((1ULL << TTSEQSETMST) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLMANAGE))){
      code = 1;
      ttservlog(g_serv, TTLOGINFO, "do_setmst: forbidden");
    } else {
      snprintf(sarg->host, TTADDRBUFSIZ, "%s", buf);
      sarg->port = port;
      sarg->opts = opts;
      sarg->recon = true;
      sarg->fatal = false;
      sarg->mts = ts;
    }
    if(ttsocksend(sock, &code, sizeof(code))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_setmst: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_setmst: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the rnum command */
static void do_rnum(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing rnum command");
  arg->counts[TTSEQNUM*req->idx+TTSEQRNUM]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  char buf[LINEBUFSIZ];
  *buf = 0;
  uint64_t rnum;
  if(mask & ((1ULL << TTSEQRNUM) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
    rnum = 0;
    ttservlog(g_serv, TTLOGINFO, "do_rnum: forbidden");
  } else {
    rnum = tcadbrnum(adb);
  }
  rnum = TTHTONLL(rnum);
  memcpy(buf + sizeof(uint8_t), &rnum, sizeof(uint64_t));
  if(ttsocksend(sock, buf, sizeof(uint8_t) + sizeof(uint64_t))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_rnum: response failed");
  }
}


/* handle the size command */
static void do_size(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing size command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSIZE]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  char buf[LINEBUFSIZ];
  *buf = 0;
  uint64_t size;
  if(mask & ((1ULL << TTSEQSIZE) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
    size = 0;
    ttservlog(g_serv, TTLOGINFO, "do_size: forbidden");
  } else {
    size = tcadbsize(adb);
  }
  size = TTHTONLL(size);
  memcpy(buf + sizeof(uint8_t), &size, sizeof(uint64_t));
  if(ttsocksend(sock, buf, sizeof(uint8_t) + sizeof(uint64_t))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_size: response failed");
  }
}


/* handle the stat command */
static void do_stat(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing stat command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSTAT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  REPLARG *sarg = arg->sarg;
  char buf[TTIOBUFSIZ];
  char *wp = buf + sizeof(uint8_t) + sizeof(uint32_t);
  if(mask & ((1ULL << TTSEQSTAT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
    ttservlog(g_serv, TTLOGINFO, "do_stat: forbidden");
  } else {
    double now = tctime();
    wp += sprintf(wp, "version\t%s\n", ttversion);
    wp += sprintf(wp, "libver\t%d\n", _TT_LIBVER);
    wp += sprintf(wp, "protver\t%s\n", _TT_PROTVER);
    wp += sprintf(wp, "os\t%s\n", TTSYSNAME);
    wp += sprintf(wp, "time\t%.6f\n", now);
    wp += sprintf(wp, "pid\t%lld\n", (long long)getpid());
    wp += sprintf(wp, "sid\t%d\n", arg->sid);
    switch(tcadbomode(adb)){
      case ADBOVOID: wp += sprintf(wp, "type\tvoid\n"); break;
      case ADBOMDB: wp += sprintf(wp, "type\ton-memory hash\n"); break;
      case ADBONDB: wp += sprintf(wp, "type\ton-memory tree\n"); break;
      case ADBOHDB: wp += sprintf(wp, "type\thash\n"); break;
      case ADBOBDB: wp += sprintf(wp, "type\tB+ tree\n"); break;
      case ADBOFDB: wp += sprintf(wp, "type\tfixed-length\n"); break;
      case ADBOTDB: wp += sprintf(wp, "type\ttable\n"); break;
      case ADBOSKEL: wp += sprintf(wp, "type\tskeleton\n"); break;
    }
    const char *path = tcadbpath(adb);
    if(path) wp += sprintf(wp, "path\t%s\n", path);
    wp += sprintf(wp, "rnum\t%llu\n", (unsigned long long)tcadbrnum(adb));
    wp += sprintf(wp, "size\t%llu\n", (unsigned long long)tcadbsize(adb));
    TCLIST *args = tclistnew2(1);
    pthread_cleanup_push((void (*)(void *))tclistdel, args);
    TCLIST *res = tcadbmisc(adb, "error", args);
    if(res){
      int rnum = tclistnum(res);
      const char *emsg = NULL;
      bool fatal = false;
      for(int i = 0; i < rnum; i++){
        const char *vbuf = tclistval2(res, i);
        if(!tcstricmp(vbuf, "fatal")){
          fatal = true;
        } else {
          emsg = vbuf;
        }
      }
      if(fatal) wp += sprintf(wp, "fatal\t%s\n", emsg);
      tclistdel(res);
    }
    pthread_cleanup_pop(1);
    wp += sprintf(wp, "bigend\t%d\n", TTBIGEND);
    if(sarg->host[0] != '\0'){
      wp += sprintf(wp, "mhost\t%s\n", sarg->host);
      wp += sprintf(wp, "mport\t%d\n", sarg->port);
      wp += sprintf(wp, "rts\t%llu\n", (unsigned long long)sarg->rts);
      double delay = now - sarg->rts / 1000000.0;
      wp += sprintf(wp, "delay\t%.6f\n", delay >= 0 ? delay : 0.0);
    }
    wp += sprintf(wp, "fd\t%d\n", sock->fd);
    wp += sprintf(wp, "loadavg\t%.6f\n", ttgetloadavg());
    TCMAP *info = tcsysinfo();
    if(info){
      const char *vbuf = tcmapget2(info, "size");
      if(vbuf) wp += sprintf(wp, "memsize\t%s\n", vbuf);
      vbuf = tcmapget2(info, "rss");
      if(vbuf) wp += sprintf(wp, "memrss\t%s\n", vbuf);
      vbuf = tcmapget2(info, "utime");
      if(vbuf) wp += sprintf(wp, "ru_user\t%s\n", vbuf);
      vbuf = tcmapget2(info, "stime");
      if(vbuf) wp += sprintf(wp, "ru_sys\t%s\n", vbuf);
      tcmapdel(info);
    }
    wp += sprintf(wp, "ru_real\t%.6f\n", now - g_starttime);
  }
  wp += sprintf(wp, "cnt_put\t%llu\n", (unsigned long long)sumstat(arg, TTSEQPUT));
  wp += sprintf(wp, "cnt_putkeep\t%llu\n", (unsigned long long)sumstat(arg, TTSEQPUTKEEP));
  wp += sprintf(wp, "cnt_putcat\t%llu\n", (unsigned long long)sumstat(arg, TTSEQPUTCAT));
  wp += sprintf(wp, "cnt_putshl\t%llu\n", (unsigned long long)sumstat(arg, TTSEQPUTSHL));
  wp += sprintf(wp, "cnt_putnr\t%llu\n", (unsigned long long)sumstat(arg, TTSEQPUTNR));
  wp += sprintf(wp, "cnt_out\t%llu\n", (unsigned long long)sumstat(arg, TTSEQOUT));
  wp += sprintf(wp, "cnt_get\t%llu\n", (unsigned long long)sumstat(arg, TTSEQGET));
  wp += sprintf(wp, "cnt_mget\t%llu\n", (unsigned long long)sumstat(arg, TTSEQMGET));
  wp += sprintf(wp, "cnt_vsiz\t%llu\n", (unsigned long long)sumstat(arg, TTSEQVSIZ));
  wp += sprintf(wp, "cnt_iterinit\t%llu\n", (unsigned long long)sumstat(arg, TTSEQITERINIT));
  wp += sprintf(wp, "cnt_iternext\t%llu\n", (unsigned long long)sumstat(arg, TTSEQITERNEXT));
  wp += sprintf(wp, "cnt_fwmkeys\t%llu\n", (unsigned long long)sumstat(arg, TTSEQFWMKEYS));
  wp += sprintf(wp, "cnt_addint\t%llu\n", (unsigned long long)sumstat(arg, TTSEQADDINT));
  wp += sprintf(wp, "cnt_adddouble\t%llu\n", (unsigned long long)sumstat(arg, TTSEQADDDOUBLE));
  wp += sprintf(wp, "cnt_ext\t%llu\n", (unsigned long long)sumstat(arg, TTSEQEXT));
  wp += sprintf(wp, "cnt_sync\t%llu\n", (unsigned long long)sumstat(arg, TTSEQSYNC));
  wp += sprintf(wp, "cnt_optimize\t%llu\n", (unsigned long long)sumstat(arg, TTSEQOPTIMIZE));
  wp += sprintf(wp, "cnt_vanish\t%llu\n", (unsigned long long)sumstat(arg, TTSEQVANISH));
  wp += sprintf(wp, "cnt_copy\t%llu\n", (unsigned long long)sumstat(arg, TTSEQCOPY));
  wp += sprintf(wp, "cnt_restore\t%llu\n", (unsigned long long)sumstat(arg, TTSEQRESTORE));
  wp += sprintf(wp, "cnt_setmst\t%llu\n", (unsigned long long)sumstat(arg, TTSEQSETMST));
  wp += sprintf(wp, "cnt_rnum\t%llu\n", (unsigned long long)sumstat(arg, TTSEQRNUM));
  wp += sprintf(wp, "cnt_size\t%llu\n", (unsigned long long)sumstat(arg, TTSEQSIZE));
  wp += sprintf(wp, "cnt_stat\t%llu\n", (unsigned long long)sumstat(arg, TTSEQSTAT));
  wp += sprintf(wp, "cnt_misc\t%llu\n", (unsigned long long)sumstat(arg, TTSEQMISC));
  wp += sprintf(wp, "cnt_repl\t%llu\n", (unsigned long long)sumstat(arg, TTSEQREPL));
  wp += sprintf(wp, "cnt_put_miss\t%llu\n", (unsigned long long)sumstat(arg, TTSEQPUTMISS));
  wp += sprintf(wp, "cnt_out_miss\t%llu\n", (unsigned long long)sumstat(arg, TTSEQOUTMISS));
  wp += sprintf(wp, "cnt_get_miss\t%llu\n", (unsigned long long)sumstat(arg, TTSEQGETMISS));
  *buf = 0;
  uint32_t size = wp - buf - (sizeof(uint8_t) + sizeof(uint32_t));
  size = TTHTONL(size);
  memcpy(buf + sizeof(uint8_t), &size, sizeof(uint32_t));
  if(ttsocksend(sock, buf, wp - buf)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_stat: response failed");
  }
}


/* handle the misc command */
static void do_misc(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGDEBUG, "doing misc command");
  arg->counts[TTSEQNUM*req->idx+TTSEQMISC]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  int nsiz = ttsockgetint32(sock);
  int opts = ttsockgetint32(sock);
  int rnum = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || nsiz < 0 || nsiz >= TTADDRBUFSIZ || rnum < 0 || rnum > MAXARGNUM){
    ttservlog(g_serv, TTLOGINFO, "do_misc: invalid parameters");
    return;
  }
  char name[TTADDRBUFSIZ];
  if(!ttsockrecv(sock, name, nsiz) && !ttsockcheckend(sock)){
    ttservlog(g_serv, TTLOGINFO, "do_misc: invalid parameters");
    return;
  }
  name[nsiz] = '\0';
  TCLIST *args = tclistnew2(rnum);
  pthread_cleanup_push((void (*)(void *))tclistdel, args);
  char stack[TTIOBUFSIZ];
  for(int i = 0; i < rnum; i++){
    int rsiz = ttsockgetint32(sock);
    if(ttsockcheckend(sock) || rsiz < 0 || rsiz > MAXARGSIZ) break;
    char *buf = (rsiz < TTIOBUFSIZ) ? stack : tcmalloc(rsiz + 1);
    pthread_cleanup_push(free, (buf == stack) ? NULL : buf);
    if(ttsockrecv(sock, buf, rsiz)) tclistpush(args, buf, rsiz);
    pthread_cleanup_pop(1);
  }
  if(!ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    uint8_t code = 0;
    tcxstrcat(xstr, &code, sizeof(code));
    uint32_t num = 0;
    tcxstrcat(xstr, &num, sizeof(num));
    rnum = 0;
    if(mask & ((1ULL << TTSEQMISC) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLWRITE))){
      ttservlog(g_serv, TTLOGINFO, "do_misc: forbidden");
    } else {
      TCLIST *res = (opts & RDBMONOULOG) ?
        tcadbmisc(adb, name, args) : tculogadbmisc(ulog, sid, 0, adb, name, args);
      if(res){
        for(int i = 0; i < tclistnum(res); i++){
          int esiz;
          const char *ebuf = tclistval(res, i, &esiz);
          num = TTHTONL((uint32_t)esiz);
          tcxstrcat(xstr, &num, sizeof(num));
          tcxstrcat(xstr, ebuf, esiz);
          rnum++;
        }
        tclistdel(res);
      } else {
        *(uint8_t *)tcxstrptr(xstr) = 1;
      }
    }
    num = TTHTONL((uint32_t)rnum);
    memcpy((char *)tcxstrptr(xstr) + sizeof(code), &num, sizeof(num));
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_misc: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_misc: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the repl command */
static void do_repl(TTSOCK *sock, TASKARG *arg, TTREQ *req){
  ttservlog(g_serv, TTLOGINFO, "doing repl command");
  arg->counts[TTSEQNUM*req->idx+TTSEQREPL]++;
  uint64_t mask = arg->mask;
  TCULOG *ulog = arg->ulog;
  uint64_t ts = ttsockgetint64(sock);
  uint32_t sid = ttsockgetint32(sock);
  if(ttsockcheckend(sock) || ts < 1 || sid < 1){
    ttservlog(g_serv, TTLOGINFO, "do_repl: invalid parameters");
    return;
  }
  if(mask & (1ULL << TTSEQREPL)){
    ttservlog(g_serv, TTLOGINFO, "do_repl: forbidden");
    return;
  }
  if(sid == arg->sid){
    ttservlog(g_serv, TTLOGINFO, "do_repl: rejected circular replication");
    return;
  }
  uint32_t lnum = TTHTONL(arg->sid);
  if(!ttsocksend(sock, &lnum, sizeof(lnum))){
    ttservlog(g_serv, TTLOGINFO, "do_repl: response failed");
    return;
  }
  TCULRD *ulrd = tculrdnew(ulog, ts);
  if(ulrd){
    ttservlog(g_serv, TTLOGINFO, "replicating to sid=%u after %llu",
              (unsigned int)sid, (unsigned long long)ts - 1);
    pthread_cleanup_push((void (*)(void *))tculrddel, ulrd);
    bool err = false;
    double noptime = 0;
    char stack[TTIOBUFSIZ];
    while(!err && !ttserviskilled(g_serv)){
      ttsocksetlife(sock, UINT_MAX);
      double now = tctime();
      req->mtime = now + UINT_MAX;
      if(now - noptime >= 1.0){
        *(unsigned char *)stack = TCULMAGICNOP;
        if(!ttsocksend(sock, stack, sizeof(uint8_t))){
          err = true;
          ttservlog(g_serv, TTLOGINFO, "do_repl: connection closed");
        }
        noptime = now;
      }
      tculrdwait(ulrd);
      uint32_t nopcnt = 0;
      const char *rbuf;
      int rsiz;
      uint64_t rts;
      uint32_t rsid, rmid;
      while(!err && (rbuf = tculrdread(ulrd, &rsiz, &rts, &rsid, &rmid)) != NULL){
        if(rsid == sid || rmid == sid){
          if((nopcnt++ & 0xff) == 0){
            now = tctime();
            if(now - noptime >= 1.0){
              *(unsigned char *)stack = TCULMAGICNOP;
              if(!ttsocksend(sock, stack, sizeof(uint8_t))){
                err = true;
                ttservlog(g_serv, TTLOGINFO, "do_repl: connection closed");
              }
              noptime = now;
            }
          }
          continue;
        }
        int msiz = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) * 2 + rsiz;
        char *mbuf = (msiz < TTIOBUFSIZ) ? stack : tcmalloc(msiz);
        pthread_cleanup_push(free, (mbuf == stack) ? NULL : mbuf);
        unsigned char *wp = (unsigned char *)mbuf;
        *(wp++) = TCULMAGICNUM;
        uint64_t llnum = TTHTONLL(rts);
        memcpy(wp, &llnum, sizeof(llnum));
        wp += sizeof(llnum);
        lnum = TTHTONL(rsid);
        memcpy(wp, &lnum, sizeof(lnum));
        wp += sizeof(lnum);
        lnum = TTHTONL(rsiz);
        memcpy(wp, &lnum, sizeof(lnum));
        wp += sizeof(lnum);
        memcpy(wp, rbuf, rsiz);
        if(!ttsocksend(sock, mbuf, msiz)){
          err = true;
          ttservlog(g_serv, TTLOGINFO, "do_repl: response failed");
        }
        pthread_cleanup_pop(1);
      }
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGERROR, "do_repl: tculrdnew failed");
  }
}


/* handle the memcached set command */
static void do_mc_set(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_set command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_set: forbidden");
    } else if(tculogadbput(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
      len = sprintf(stack, "STORED\r\n");
    } else {
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
      ttservlog(g_serv, TTLOGERROR, "do_mc_set: operation failed");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_set: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_set: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached add command */
static void do_mc_add(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_add command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUTKEEP]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & ((1ULL << TTSEQPUTKEEP) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_add: forbidden");
    } else if(tculogadbputkeep(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
      len = sprintf(stack, "STORED\r\n");
    } else {
      arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
      len = sprintf(stack, "NOT_STORED\r\n");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_add: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_add: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached replace command */
static void do_mc_replace(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_replace command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_replace: forbidden");
    } else {
      if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_replace: pthread_mutex_lock failed");
      } else if(tcadbvsiz(adb, kbuf, ksiz) >= 0){
        if(tculogadbput(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
          len = sprintf(stack, "STORED\r\n");
        } else {
          len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
          ttservlog(g_serv, TTLOGERROR, "do_mc_replace: operation failed");
        }
      } else {
        arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
        len = sprintf(stack, "NOT_STORED\r\n");
      }
      if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
        ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_unlock failed");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_replace: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_replace: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached append command */
static void do_mc_append(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_append command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_append: forbidden");
    } else {
      if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_append: pthread_mutex_lock failed");
      } else if(tcadbvsiz(adb, kbuf, ksiz) >= 0){
        if(tculogadbputcat(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
          len = sprintf(stack, "STORED\r\n");
        } else {
          len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
          ttservlog(g_serv, TTLOGERROR, "do_mc_append: operation failed");
        }
      } else {
        arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
        len = sprintf(stack, "NOT_STORED\r\n");
      }
      if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
        ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_unlock failed");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_append: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_append: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached prepend command */
static void do_mc_prepend(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_prepend command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 5){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 5 && !strcmp(tokens[5], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int vsiz = tclmax(tcatoi(tokens[4]), 0);
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(ttsockrecv(sock, vbuf, vsiz) && ttsockgetc(sock) == '\r' && ttsockgetc(sock) == '\n' &&
     !ttsockcheckend(sock)){
    int len;
    if(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
      len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
      ttservlog(g_serv, TTLOGINFO, "do_mc_prepend: forbidden");
    } else {
      char *obuf;
      int osiz;
      if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_prepend: pthread_mutex_lock failed");
      } else if((obuf = tcadbget(adb, kbuf, ksiz, &osiz)) != NULL){
        char *nbuf = tcmalloc(vsiz + osiz + 1);
        memcpy(nbuf, vbuf, vsiz);
        memcpy(nbuf + vsiz, obuf, osiz);
        tculogadbput(ulog, sid, 0, adb, kbuf, ksiz, nbuf, vsiz + osiz);
        len = sprintf(stack, "STORED\r\n");
        tcfree(nbuf);
        tcfree(obuf);
      } else {
        arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
        len = sprintf(stack, "NOT_STORED\r\n");
      }
      if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
        ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_unlock failed");
    }
    if(nr || ttsocksend(sock, stack, len)){
      req->keep = true;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_mc_prepend: response failed");
    }
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_prepend: invalid entity");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached get command */
static void do_mc_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_get command");
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  if(tnum < 2){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  for(int i = 1; i < tnum; i++){
    arg->counts[TTSEQNUM*req->idx+TTSEQGET]++;
    const char *kbuf = tokens[i];
    int ksiz = strlen(kbuf);
    int vsiz;
    char *vbuf;
    if(mask & ((1ULL << TTSEQGET) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLREAD))){
      vbuf = NULL;
      vsiz = 0;
      ttservlog(g_serv, TTLOGINFO, "do_mc_get: forbidden");
    } else {
      vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    }
    if(vbuf){
      tcxstrprintf(xstr, "VALUE %s 0 %d\r\n", kbuf, vsiz);
      tcxstrcat(xstr, vbuf, vsiz);
      tcxstrcat(xstr, "\r\n", 2);
      tcfree(vbuf);
    } else {
      arg->counts[TTSEQNUM*req->idx+TTSEQGETMISS]++;
    }
  }
  tcxstrprintf(xstr, "END\r\n");
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_get: response failed");
  }
  pthread_cleanup_pop(1);
}


/* handle the memcached delete command */
static void do_mc_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_delete command");
  arg->counts[TTSEQNUM*req->idx+TTSEQOUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  if(tnum < 2){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = (tnum > 2 && !strcmp(tokens[2], "noreply")) ||
    (tnum > 3 && !strcmp(tokens[3], "noreply"));
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & ((1ULL << TTSEQOUT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_delete: forbidden");
  } else if(tculogadbout(ulog, sid, 0, adb, kbuf, ksiz)){
    len = sprintf(stack, "DELETED\r\n");
  } else {
    arg->counts[TTSEQNUM*req->idx+TTSEQOUTMISS]++;
    len = sprintf(stack, "NOT_FOUND\r\n");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_delete: response failed");
  }
}


/* handle the memcached incr command */
static void do_mc_incr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_incr command");
  arg->counts[TTSEQNUM*req->idx+TTSEQADDINT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 3){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 3 && !strcmp(tokens[3], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int64_t num = tcatoi(tokens[2]);
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & ((1ULL << TTSEQADDINT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_incr: forbidden");
  } else {
    if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
      ttsockprintf(sock, "SERVER_ERROR unexpected\r\n");
      ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_lock failed");
      return;
    }
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      num += tcatoi(vbuf);
      if(num < 0) num = 0;
      len = sprintf(stack, "%lld", (long long)num);
      if(tculogadbput(ulog, sid, 0, adb, kbuf, ksiz, stack, len)){
        len = sprintf(stack, "%lld\r\n", (long long)num);
      } else {
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_incr: operation failed");
      }
      tcfree(vbuf);
    } else {
      len = sprintf(stack, "NOT_FOUND\r\n");
    }
    if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
      ttservlog(g_serv, TTLOGERROR, "do_mc_incr: pthread_mutex_unlock failed");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_incr: response failed");
  }
}


/* handle the memcached decr command */
static void do_mc_decr(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_decr command");
  arg->counts[TTSEQNUM*req->idx+TTSEQADDINT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  if(tnum < 3){
    ttsockprintf(sock, "CLIENT_ERROR error\r\n");
    return;
  }
  bool nr = tnum > 3 && !strcmp(tokens[3], "noreply");
  const char *kbuf = tokens[1];
  int ksiz = strlen(kbuf);
  int64_t num = tcatoi(tokens[2]) * -1;
  int mtxidx = recmtxidx(kbuf, ksiz);
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & ((1ULL << TTSEQADDINT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_decr: forbidden");
  } else {
    if(pthread_mutex_lock(rmtxs + mtxidx) != 0){
      ttsockprintf(sock, "SERVER_ERROR unexpected\r\n");
      ttservlog(g_serv, TTLOGERROR, "do_mc_decr: pthread_mutex_lock failed");
      return;
    }
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      num += tcatoi(vbuf);
      if(num < 0) num = 0;
      len = sprintf(stack, "%lld", (long long)num);
      if(tculogadbput(ulog, sid, 0, adb, kbuf, ksiz, stack, len)){
        len = sprintf(stack, "%lld\r\n", (long long)num);
      } else {
        len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
        ttservlog(g_serv, TTLOGERROR, "do_mc_decr: operation failed");
      }
      tcfree(vbuf);
    } else {
      len = sprintf(stack, "NOT_FOUND\r\n");
    }
    if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
      ttservlog(g_serv, TTLOGERROR, "do_mc_decr: pthread_mutex_unlock failed");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_decr: response failed");
  }
}


/* handle the memcached stat command */
static void do_mc_stats(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_stats command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSTAT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  char stack[TTIOBUFSIZ];
  char *wp = stack;
  if(mask & ((1ULL << TTSEQSTAT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLREAD))){
    ttservlog(g_serv, TTLOGINFO, "do_mc_stats: forbidden");
  } else {
    wp += sprintf(wp, "STAT pid %lld\r\n", (long long)getpid());
    time_t now = time(NULL);
    wp += sprintf(wp, "STAT uptime %lld\r\n", (long long)(now - (int)g_starttime));
    wp += sprintf(wp, "STAT time %lld\r\n", (long long)now);
    wp += sprintf(wp, "STAT version %s\r\n", ttversion);
    wp += sprintf(wp, "STAT pointer_size %d\r\n", (int)sizeof(void *) * 8);
    struct rusage ubuf;
    memset(&ubuf, 0, sizeof(ubuf));
    if(getrusage(RUSAGE_SELF, &ubuf) == 0){
      wp += sprintf(wp, "STAT rusage_user %d.%06d\r\n",
                    (int)ubuf.ru_utime.tv_sec, (int)ubuf.ru_utime.tv_usec);
      wp += sprintf(wp, "STAT rusage_system %d.%06d\r\n",
                    (int)ubuf.ru_stime.tv_sec, (int)ubuf.ru_stime.tv_usec);
    }
    uint64_t putsum = sumstat(arg, TTSEQPUT) + sumstat(arg, TTSEQPUTKEEP) +
      sumstat(arg, TTSEQPUTCAT) + sumstat(arg, TTSEQPUTSHL) + sumstat(arg, TTSEQPUTNR);
    uint64_t putmiss = sumstat(arg, TTSEQPUTMISS);
    wp += sprintf(wp, "STAT cmd_set %llu\r\n", (unsigned long long)putsum);
    wp += sprintf(wp, "STAT cmd_set_hits %llu\r\n", (unsigned long long)(putsum - putmiss));
    wp += sprintf(wp, "STAT cmd_set_misses %llu\r\n", (unsigned long long)putmiss);
    uint64_t outsum = sumstat(arg, TTSEQOUT);
    uint64_t outmiss = sumstat(arg, TTSEQOUTMISS);
    wp += sprintf(wp, "STAT cmd_delete %llu\r\n", (unsigned long long)outsum);
    wp += sprintf(wp, "STAT cmd_delete_hits %llu\r\n", (unsigned long long)(outsum - outmiss));
    wp += sprintf(wp, "STAT cmd_delete_misses %llu\r\n", (unsigned long long)outmiss);
    uint64_t getsum = sumstat(arg, TTSEQGET);
    uint64_t getmiss = sumstat(arg, TTSEQGETMISS);
    wp += sprintf(wp, "STAT cmd_get %llu\r\n", (unsigned long long)getsum);
    wp += sprintf(wp, "STAT cmd_get_hits %llu\r\n", (unsigned long long)(getsum - getmiss));
    wp += sprintf(wp, "STAT cmd_get_misses %llu\r\n", (unsigned long long)getmiss);
    wp += sprintf(wp, "STAT cmd_flush %llu\r\n", (unsigned long long)sumstat(arg, TTSEQVANISH));
    int64_t rnum = tcadbrnum(adb);
    wp += sprintf(wp, "STAT curr_items %lld\r\n", (long long)rnum);
    wp += sprintf(wp, "STAT total_items %lld\r\n", (long long)rnum);
    wp += sprintf(wp, "STAT bytes %lld\r\n", (long long)tcadbsize(adb));
    wp += sprintf(wp, "STAT threads %d\r\n", arg->thnum);
    wp += sprintf(wp, "END\r\n");
  }
  if(ttsocksend(sock, stack, wp - stack)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_stats: response failed");
  }
}


/* handle the memcached flush_all command */
static void do_mc_flushall(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGINFO, "doing mc_flushall command");
  arg->counts[TTSEQNUM*req->idx+TTSEQVANISH]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool nr = (tnum > 1 && !strcmp(tokens[1], "noreply")) ||
    (tnum > 2 && !strcmp(tokens[2], "noreply"));
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & ((1ULL << TTSEQVANISH) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLWRITE))){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_flushall: forbidden");
  } else if(tculogadbvanish(ulog, sid, 0, adb)){
    len = sprintf(stack, "OK\r\n");
  } else {
    len = sprintf(stack, "SERVER_ERROR unexpected\r\n");
    ttservlog(g_serv, TTLOGERROR, "do_mc_flushall: operation failed");
  }
  if(nr || ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_flushall: response failed");
  }
}


/* handle the memcached version command */
static void do_mc_version(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_version command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSTAT]++;
  uint64_t mask = arg->mask;
  char stack[TTIOBUFSIZ];
  int len;
  if(mask & ((1ULL << TTSEQSTAT) | (1ULL << TTSEQALLMC) | (1ULL << TTSEQALLREAD))){
    len = sprintf(stack, "CLIENT_ERROR forbidden\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_mc_version: forbidden");
  } else {
    len = sprintf(stack, "VERSION %s\r\n", ttversion);
  }
  if(ttsocksend(sock, stack, len)){
    req->keep = true;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_mc_version: response failed");
  }
}


/* handle the memcached quit command */
static void do_mc_quit(TTSOCK *sock, TASKARG *arg, TTREQ *req, char **tokens, int tnum){
  ttservlog(g_serv, TTLOGDEBUG, "doing mc_quit command");
}


/* handle the HTTP GET command */
static void do_http_get(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_get command");
  arg->counts[TTSEQNUM*req->idx+TTSEQGET]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & ((1ULL << TTSEQGET) | (1ULL << TTSEQALLHTTP) | (1ULL << TTSEQALLREAD))){
    int len = sprintf(line, "Forbidden\n");
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
    tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
    tcxstrprintf(xstr, "\r\n");
    tcxstrcat(xstr, line, len);
    ttservlog(g_serv, TTLOGINFO, "do_http_get: forbidden");
  } else {
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(vbuf){
      tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
      tcxstrprintf(xstr, "Content-Type: application/octet-stream\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", vsiz);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, vbuf, vsiz);
      tcfree(vbuf);
    } else {
      arg->counts[TTSEQNUM*req->idx+TTSEQGETMISS]++;
      int len = sprintf(line, "Not Found\n");
      tcxstrprintf(xstr, "HTTP/1.1 404 Not Found\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
    }
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_get: response failed");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP HEAD command */
static void do_http_head(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_head command");
  arg->counts[TTSEQNUM*req->idx+TTSEQVSIZ]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & ((1ULL << TTSEQVSIZ) | (1ULL << TTSEQALLHTTP) | (1ULL << TTSEQALLREAD))){
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "\r\n");
    ttservlog(g_serv, TTLOGINFO, "do_http_head: forbidden");
  } else {
    int vsiz = tcadbvsiz(adb, kbuf, ksiz);
    if(vsiz >= 0){
      tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
      tcxstrprintf(xstr, "Content-Type: application/octet-stream\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", vsiz);
      tcxstrprintf(xstr, "\r\n");
    } else {
      tcxstrprintf(xstr, "HTTP/1.1 404 Not Found\r\n");
      tcxstrprintf(xstr, "\r\n");
    }
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_head: response failed");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP PUT command */
static void do_http_put(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_put command");
  arg->counts[TTSEQNUM*req->idx+TTSEQPUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool keep = ver >= 1;
  int vsiz = 0;
  int pdmode = 0;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    } else if(!tcstricmp(line, "content-length")){
      vsiz = tcatoi(pv);
    } else if(!tcstricmp(line, "x-tt-pdmode")){
      pdmode = tcatoi(pv);
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(vsiz >= 0 && ttsockrecv(sock, vbuf, vsiz) && !ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    if(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLHTTP) | (1ULL << TTSEQALLWRITE))){
      int len = sprintf(line, "Forbidden\n");
      tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
      ttservlog(g_serv, TTLOGINFO, "do_http_put: forbidden");
    } else {
      switch(pdmode){
        case 1:
          if(tculogadbputkeep(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
            int len = sprintf(line, "Created\n");
            tcxstrprintf(xstr, "HTTP/1.1 201 Created\r\n");
            tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
            tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
            tcxstrprintf(xstr, "\r\n");
            tcxstrcat(xstr, line, len);
          } else {
            arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
            int len = sprintf(line, "Conflict\n");
            tcxstrprintf(xstr, "HTTP/1.1 409 Conflict\r\n");
            tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
            tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
            tcxstrprintf(xstr, "\r\n");
            tcxstrcat(xstr, line, len);
          }
          break;
        case 2:
          if(tculogadbputcat(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
            int len = sprintf(line, "Created\n");
            tcxstrprintf(xstr, "HTTP/1.1 201 Created\r\n");
            tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
            tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
            tcxstrprintf(xstr, "\r\n");
            tcxstrcat(xstr, line, len);
          } else {
            arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
            int len = sprintf(line, "Internal Server Error\n");
            tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
            tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
            tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
            tcxstrprintf(xstr, "\r\n");
            tcxstrcat(xstr, line, len);
            ttservlog(g_serv, TTLOGERROR, "do_http_put: operation failed");
          }
          break;
        default:
          if(tculogadbput(ulog, sid, 0, adb, kbuf, ksiz, vbuf, vsiz)){
            int len = sprintf(line, "Created\n");
            tcxstrprintf(xstr, "HTTP/1.1 201 Created\r\n");
            tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
            tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
            tcxstrprintf(xstr, "\r\n");
            tcxstrcat(xstr, line, len);
          } else {
            arg->counts[TTSEQNUM*req->idx+TTSEQPUTMISS]++;
            int len = sprintf(line, "Internal Server Error\n");
            tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
            tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
            tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
            tcxstrprintf(xstr, "\r\n");
            tcxstrcat(xstr, line, len);
            ttservlog(g_serv, TTLOGERROR, "do_http_put: operation failed");
          }
      }
    }
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = keep;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_http_put: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_put: invalid entity");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP POST command */
static void do_http_post(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_post command");
  arg->counts[TTSEQNUM*req->idx+TTSEQEXT]++;
  uint64_t mask = arg->mask;
  pthread_mutex_t *rmtxs = arg->rmtxs;
  void *scr = arg->screxts[req->idx];
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool keep = ver >= 1;
  int vsiz = 0;
  char ctype[LINEBUFSIZ/4+1];
  *ctype = '\0';
  char xname[LINEBUFSIZ/4+1];
  *xname = '\0';
  char mname[LINEBUFSIZ/4+1];
  *mname = '\0';
  int xopts = 0;
  int mopts = 0;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    } else if(!tcstricmp(line, "content-length")){
      vsiz = tcatoi(pv);
    } else if(!tcstricmp(line, "content-type")){
      snprintf(ctype, sizeof(ctype), "%s", pv);
      ctype[sizeof(ctype)-1] = '\0';
    } else if(!tcstricmp(line, "x-tt-xname")){
      snprintf(xname, sizeof(xname), "%s", pv);
      xname[sizeof(xname)-1] = '\0';
    } else if(!tcstricmp(line, "x-tt-mname")){
      snprintf(mname, sizeof(mname), "%s", pv);
      mname[sizeof(mname)-1] = '\0';
    } else if(!tcstricmp(line, "x-tt-xopts")){
      xopts = tcatoi(pv);
    } else if(!tcstricmp(line, "x-tt-mopts")){
      mopts = tcatoi(pv);
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  char stack[TTIOBUFSIZ];
  char *vbuf = (vsiz < TTIOBUFSIZ) ? stack : tcmalloc(vsiz + 1);
  pthread_cleanup_push(free, (vbuf == stack) ? NULL : vbuf);
  if(vsiz >= 0 && ttsockrecv(sock, vbuf, vsiz) && !ttsockcheckend(sock)){
    TCXSTR *xstr = tcxstrnew();
    pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
    if(*xname != '\0'){
      if(mask & ((1ULL << TTSEQEXT) | (1ULL << TTSEQALLHTTP))){
        int len = sprintf(line, "Forbidden\n");
        tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
        tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
        tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
        tcxstrprintf(xstr, "\r\n");
        tcxstrcat(xstr, line, len);
        ttservlog(g_serv, TTLOGINFO, "do_http_post: forbidden");
      } else {
        int xsiz = 0;
        char *xbuf = NULL;
        if(scr){
          if(xopts & RDBXOLCKGLB){
            bool err = false;
            for(int i = 0; i < RECMTXNUM; i++){
              if(pthread_mutex_lock(rmtxs + i) != 0){
                ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_lock failed");
                while(--i >= 0){
                  pthread_mutex_unlock(rmtxs + i);
                }
                err = true;
                break;
              }
            }
            if(!err){
              xbuf = scrextcallmethod(scr, xname, kbuf, ksiz, vbuf, vsiz, &xsiz);
              for(int i = RECMTXNUM - 1; i >= 0; i--){
                if(pthread_mutex_unlock(rmtxs + i) != 0)
                  ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_unlock failed");
              }
            }
          } else if(xopts & RDBXOLCKREC){
            int mtxidx = recmtxidx(kbuf, ksiz);
            if(pthread_mutex_lock(rmtxs + mtxidx) == 0){
              xbuf = scrextcallmethod(scr, xname, kbuf, ksiz, vbuf, vsiz, &xsiz);
              if(pthread_mutex_unlock(rmtxs + mtxidx) != 0)
                ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_unlock failed");
            } else {
              ttservlog(g_serv, TTLOGERROR, "do_http_post: pthread_mutex_lock failed");
            }
          } else {
            xbuf = scrextcallmethod(scr, xname, kbuf, ksiz, vbuf, vsiz, &xsiz);
          }
        }
        if(xbuf){
          tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
          tcxstrprintf(xstr, "Content-Type: application/octet-stream\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", xsiz);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, xbuf, xsiz);
          tcfree(xbuf);
        } else {
          int len = sprintf(line, "Internal Server Error\n");
          tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
        }
      }
    } else if(*mname != '\0'){
      if(mask & ((1ULL << TTSEQMISC) | (1ULL << TTSEQALLHTTP))){
        int len = sprintf(line, "Forbidden\n");
        tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
        tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
        tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
        tcxstrprintf(xstr, "\r\n");
        tcxstrcat(xstr, line, len);
        ttservlog(g_serv, TTLOGINFO, "do_http_post: forbidden");
      } else {
        TCMAP *params = tcmapnew2(1);
        pthread_cleanup_push((void (*)(void *))tcmapdel, params);
        if(vsiz > 0) tcwwwformdecode2(vbuf, vsiz, ctype, params);
        TCLIST *args = tcmapvals(params);
        pthread_cleanup_push((void (*)(void *))tclistdel, args);
        TCLIST *res = (mopts & RDBMONOULOG) ?
          tcadbmisc(adb, mname, args) : tculogadbmisc(ulog, sid, 0, adb, mname, args);
        if(res){
          TCXSTR *rbuf = tcxstrnew();
          for(int i = 0; i < tclistnum(res); i++){
            int esiz;
            const char *ebuf = tclistval(res, i, &esiz);
            if(i > 0) tcxstrcat(rbuf, "&", 1);
            tcxstrprintf(rbuf, "res%d=", i + 1);
            char *enc = tcurlencode(ebuf, esiz);
            tcxstrcat2(rbuf, enc);
            tcfree(enc);
          }
          tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
          tcxstrprintf(xstr, "Content-Type: application/x-www-form-urlencoded\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", tcxstrsize(rbuf));
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, tcxstrptr(rbuf), tcxstrsize(rbuf));
          tcxstrdel(rbuf);
          tclistdel(res);
        } else {
          int len = sprintf(line, "Internal Server Error\n");
          tcxstrprintf(xstr, "HTTP/1.1 500 Internal Server Error\r\n");
          tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
          tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
          tcxstrprintf(xstr, "\r\n");
          tcxstrcat(xstr, line, len);
        }
        pthread_cleanup_pop(1);
        pthread_cleanup_pop(1);
      }
    } else {
      int len = sprintf(line, "Bad Request\n");
      tcxstrprintf(xstr, "HTTP/1.1 400 Bad Request\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
      ttservlog(g_serv, TTLOGINFO, "do_http_post: bad request");
    }
    if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
      req->keep = keep;
    } else {
      ttservlog(g_serv, TTLOGINFO, "do_http_post: response failed");
    }
    pthread_cleanup_pop(1);
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_post: invalid entity");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP DELETE command */
static void do_http_delete(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_delete command");
  arg->counts[TTSEQNUM*req->idx+TTSEQOUT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  TCULOG *ulog = arg->ulog;
  uint32_t sid = arg->sid;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  if(*uri == '/') uri++;
  int ksiz;
  char *kbuf = tcurldecode(uri, &ksiz);
  pthread_cleanup_push(free, kbuf);
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & ((1ULL << TTSEQOUT) | (1ULL << TTSEQALLHTTP) | (1ULL << TTSEQALLWRITE))){
    int len = sprintf(line, "Forbidden\n");
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
    tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
    tcxstrprintf(xstr, "\r\n");
    tcxstrcat(xstr, line, len);
    ttservlog(g_serv, TTLOGINFO, "do_http_delete: forbidden");
  } else {
    if(tculogadbout(ulog, sid, 0, adb, kbuf, ksiz)){
      int len = sprintf(line, "OK\n");
      tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
    } else {
      arg->counts[TTSEQNUM*req->idx+TTSEQOUTMISS]++;
      int len = sprintf(line, "Not Found\n");
      tcxstrprintf(xstr, "HTTP/1.1 404 Not Found\r\n");
      tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
      tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
      tcxstrprintf(xstr, "\r\n");
      tcxstrcat(xstr, line, len);
    }
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_delete: response failed");
  }
  pthread_cleanup_pop(1);
  pthread_cleanup_pop(1);
}


/* handle the HTTP OPTIONS command */
static void do_http_options(TTSOCK *sock, TASKARG *arg, TTREQ *req, int ver, const char *uri){
  ttservlog(g_serv, TTLOGDEBUG, "doing http_options command");
  arg->counts[TTSEQNUM*req->idx+TTSEQSTAT]++;
  uint64_t mask = arg->mask;
  TCADB *adb = arg->adb;
  REPLARG *sarg = arg->sarg;
  bool keep = ver >= 1;
  char line[LINEBUFSIZ];
  while(ttsockgets(sock, line, LINEBUFSIZ) && *line != '\0'){
    char *pv = strchr(line, ':');
    if(!pv) continue;
    *(pv++) = '\0';
    while(*pv == ' ' || *pv == '\t'){
      pv++;
    }
    if(!tcstricmp(line, "connection")){
      if(!tcstricmp(pv, "close")){
        keep = false;
      } else if(!tcstricmp(pv, "keep-alive")){
        keep = true;
      }
    }
  }
  TCXSTR *xstr = tcxstrnew();
  pthread_cleanup_push((void (*)(void *))tcxstrdel, xstr);
  if(mask & ((1ULL << TTSEQSTAT) | (1ULL << TTSEQALLORG) | (1ULL << TTSEQALLREAD))){
    int len = sprintf(line, "Forbidden\n");
    tcxstrprintf(xstr, "HTTP/1.1 403 Forbidden\r\n");
    tcxstrprintf(xstr, "Content-Type: text/plain\r\n");
    tcxstrprintf(xstr, "Content-Length: %d\r\n", len);
    tcxstrprintf(xstr, "\r\n");
    tcxstrcat(xstr, line, len);
    ttservlog(g_serv, TTLOGINFO, "do_http_options: forbidden");
  } else {
    double now = tctime();
    tcxstrprintf(xstr, "HTTP/1.1 200 OK\r\n");
    tcxstrprintf(xstr, "Content-Length: 0\r\n");
    tcxstrprintf(xstr, "Allow: OPTIONS");
    if(!(mask & ((1ULL << TTSEQGET) | (1ULL << TTSEQALLREAD)))) tcxstrprintf(xstr, ", GET");
    if(!(mask & ((1ULL << TTSEQVSIZ) | (1ULL << TTSEQALLREAD)))) tcxstrprintf(xstr, ", HEAD");
    if(!(mask & ((1ULL << TTSEQPUT) | (1ULL << TTSEQALLWRITE)))) tcxstrprintf(xstr, ", PUT");
    if(!(mask & ((1ULL << TTSEQEXT) | (1ULL << TTSEQALLWRITE)))) tcxstrprintf(xstr, ", POST");
    if(!(mask & ((1ULL << TTSEQOUT) | (1ULL << TTSEQALLWRITE)))) tcxstrprintf(xstr, ", DELETE");
    tcxstrprintf(xstr, "\r\n");
    tcxstrprintf(xstr, "X-TT-VERSION: %s\r\n", ttversion);
    tcxstrprintf(xstr, "X-TT-LIBVER: %d\r\n", _TT_LIBVER);
    tcxstrprintf(xstr, "X-TT-PROTVER: %s\r\n", _TT_PROTVER);
    tcxstrprintf(xstr, "X-TT-OS: %s\r\n", TTSYSNAME);
    tcxstrprintf(xstr, "X-TT-TIME: %.6f\r\n", now);
    tcxstrprintf(xstr, "X-TT-PID: %lld\r\n", (long long)getpid());
    tcxstrprintf(xstr, "X-TT-SID: %d\r\n", arg->sid);
    switch(tcadbomode(adb)){
      case ADBOVOID: tcxstrprintf(xstr, "X-TT-TYPE: void\r\n"); break;
      case ADBOMDB: tcxstrprintf(xstr, "X-TT-TYPE: on-memory hash\r\n"); break;
      case ADBONDB: tcxstrprintf(xstr, "X-TT-TYPE: on-memory tree\r\n"); break;
      case ADBOHDB: tcxstrprintf(xstr, "X-TT-TYPE: hash\r\n"); break;
      case ADBOBDB: tcxstrprintf(xstr, "X-TT-TYPE: B+ tree\r\n"); break;
      case ADBOFDB: tcxstrprintf(xstr, "X-TT-TYPE: fixed-length\r\n"); break;
      case ADBOTDB: tcxstrprintf(xstr, "X-TT-TYPE: table\r\n"); break;
      case ADBOSKEL: tcxstrprintf(xstr, "X-TT-TYPE: skeleton\r\n"); break;
    }
    const char *path = tcadbpath(adb);
    if(path) tcxstrprintf(xstr, "X-TT-PATH: %s\r\n", path);
    tcxstrprintf(xstr, "X-TT-RNUM: %llu\r\n", (unsigned long long)tcadbrnum(adb));
    tcxstrprintf(xstr, "X-TT-SIZE: %llu\r\n", (unsigned long long)tcadbsize(adb));
    tcxstrprintf(xstr, "X-TT-BIGEND: %d\r\n", TTBIGEND);
    if(sarg->host[0] != '\0'){
      tcxstrprintf(xstr, "X-TT-MHOST: %s\r\n", sarg->host);
      tcxstrprintf(xstr, "X-TT-MPORT: %d\r\n", sarg->port);
      tcxstrprintf(xstr, "X-TT-RTS: %llu\r\n", (unsigned long long)sarg->rts);
      double delay = now - sarg->rts / 1000000.0;
      tcxstrprintf(xstr, "X-TT-DELAY: %.6f\r\n", delay >= 0 ? delay : 0.0);
    }
    tcxstrprintf(xstr, "X-TT-FD: %d\r\n", sock->fd);
    tcxstrprintf(xstr, "X-TT-LOADAVG: %.6f\r\n", ttgetloadavg());
    TCMAP *info = tcsysinfo();
    if(info){
      const char *vbuf = tcmapget2(info, "size");
      if(vbuf) tcxstrprintf(xstr, "X-TT-MEMSIZE: %s\r\n", vbuf);
      vbuf = tcmapget2(info, "rss");
      if(vbuf) tcxstrprintf(xstr, "X-TT-MEMRSS: %s\r\n", vbuf);
      tcmapdel(info);
    }
    tcxstrprintf(xstr, "X-TT-RU_REAL: %.6f\r\n", now - g_starttime);
    struct rusage ubuf;
    memset(&ubuf, 0, sizeof(ubuf));
    if(getrusage(RUSAGE_SELF, &ubuf) == 0){
      tcxstrprintf(xstr, "X-TT-RU_USER: %d.%06d\r\n",
                   (int)ubuf.ru_utime.tv_sec, (int)ubuf.ru_utime.tv_usec);
      tcxstrprintf(xstr, "X-TT-RU_SYS: %d.%06d\r\n",
                   (int)ubuf.ru_stime.tv_sec, (int)ubuf.ru_stime.tv_usec);
    }
    tcxstrprintf(xstr, "\r\n");
  }
  if(ttsocksend(sock, tcxstrptr(xstr), tcxstrsize(xstr))){
    req->keep = keep;
  } else {
    ttservlog(g_serv, TTLOGINFO, "do_http_options: response failed");
  }
  pthread_cleanup_pop(1);
}


/* handle the termination event */
static void do_term(void *opq){
  TERMARG *arg = (TERMARG *)opq;
  int thnum = arg->thnum;
  TCADB *adb = arg->adb;
  REPLARG *sarg = arg->sarg;
  void **screxts = arg->screxts;
  EXTPCARG *pcargs = arg->pcargs;
  int pcnum = arg->pcnum;
  if(sarg->host[0] != '\0') tcsleep(REPLPERIOD * 1.2);
  if(g_restart) return;
  if(pcargs){
    for(int i = 0; i < pcnum; i++){
      EXTPCARG *pcarg = pcargs + i;
      if(!pcarg->scrext) continue;
      if(!scrextkill(pcarg->scrext)){
        arg->err = true;
        ttservlog(g_serv, TTLOGERROR, "scrextkill failed");
      }
    }
  }
  for(int i = 0; i < thnum; i++){
    if(!screxts[i]) continue;
    if(!scrextkill(screxts[i])){
      arg->err = true;
      ttservlog(g_serv, TTLOGERROR, "scrextkill failed");
    }
  }
  ttservlog(g_serv, TTLOGSYSTEM, "closing the database");
  if(!tcadbclose(adb)){
    arg->err = true;
    ttservlog(g_serv, TTLOGERROR, "tcadbclose failed");
  }
}



// END OF FILE
