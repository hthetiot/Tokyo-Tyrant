/*************************************************************************************************
 * The test cases of the remote database API
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


#include <tcrdb.h>
#include "myconf.h"

#define RECBUFSIZ      32                // buffer for records

typedef struct {                         // type of structure for write thread
  TCRDB *rdb;
  int rnum;
  bool nr;
  const char *ext;
  bool rnd;
  int id;
} TARGWRITE;

typedef struct {                         // type of structure for read thread
  TCRDB *rdb;
  int rnum;
  int mul;
  bool rnd;
  int id;
} TARGREAD;

typedef struct {                         // type of structure for remove thread
  TCRDB *rdb;
  int rnum;
  bool rnd;
  int id;
} TARGREMOVE;

typedef struct {                         // type of structure for typical thread
  TCRDB *rdb;
  int rnum;
  int id;
} TARGTYPICAL;

typedef struct {                         // type of structure for table thread
  TCRDB *rdb;
  int rnum;
  bool rnd;
  int id;
} TARGTABLE;


/* global variables */
const char *g_progname;                  // program name


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void iprintf(const char *format, ...);
static void eprint(TCRDB *rdb, int line, const char *func);
static int myrand(int range);
static int myrandnd(int range);
static bool myopen(TCRDB *rdb, const char *host, int port);
static int runwrite(int argc, char **argv);
static int runread(int argc, char **argv);
static int runremove(int argc, char **argv);
static int runtypical(int argc, char **argv);
static int runtable(int argc, char **argv);
static int procwrite(const char *host, int port, int tnum, int rnum,
                     bool nr, const char *ext, bool rnd);
static int procread(const char *host, int port, int tnum, int mul, bool rnd);
static int procremove(const char *host, int port, int tnum, bool rnd);
static int proctypical(const char *host, int port, int tnum, int rnum);
static int proctable(const char *host, int port, int tnum, int rnum, bool rnd);
static void *threadwrite(void *targ);
static void *threadread(void *targ);
static void *threadremove(void *targ);
static void *threadtypical(void *targ);
static void *threadtable(void *targ);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  srand((unsigned int)(tctime() * 1000) % UINT_MAX);
  if(argc < 2) usage();
  int rv = 0;
  if(!strcmp(argv[1], "write")){
    rv = runwrite(argc, argv);
  } else if(!strcmp(argv[1], "read")){
    rv = runread(argc, argv);
  } else if(!strcmp(argv[1], "remove")){
    rv = runremove(argc, argv);
  } else if(!strcmp(argv[1], "typical")){
    rv = runtypical(argc, argv);
  } else if(!strcmp(argv[1], "table")){
    rv = runtable(argc, argv);
  } else {
    usage();
  }
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: test cases of the remote database API of Tokyo Tyrant\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s write [-port num] [-tnum num] [-nr] [-ext name] [-rnd] host rnum\n",
          g_progname);
  fprintf(stderr, "  %s read [-port num] [-tnum num] [-mul num] host\n", g_progname);
  fprintf(stderr, "  %s remove [-port num] [-tnum num] host\n", g_progname);
  fprintf(stderr, "  %s typical [-port num] [-tnum num] host rnum\n", g_progname);
  fprintf(stderr, "  %s table [-port num] [-tnum num] host rnum\n", g_progname);
  fprintf(stderr, "\n");
  exit(1);
}


/* print formatted information string and flush the buffer */
static void iprintf(const char *format, ...){
  va_list ap;
  va_start(ap, format);
  vprintf(format, ap);
  fflush(stdout);
  va_end(ap);
}


/* print error message of abstract database */
static void eprint(TCRDB *rdb, int line, const char *func){
  int ecode = tcrdbecode(rdb);
  fprintf(stderr, "%s: %d: %s: error: %d: %s\n",
          g_progname, line, func, ecode, tcrdberrmsg(ecode));
}


/* get a random number */
static int myrand(int range){
  if(range < 2) return 0;
  int high = (unsigned int)rand() >> 4;
  int low = range * (rand() / (RAND_MAX + 1.0));
  low &= (unsigned int)INT_MAX >> 4;
  return (high + low) % range;
}


/* get a random number based on normal distribution */
static int myrandnd(int range){
  int num = (int)tcdrandnd(range >> 1, range / 10);
  return (num < 0 || num >= range) ? 0 : num;
}


/* open the remote database */
static bool myopen(TCRDB *rdb, const char *host, int port){
  bool err = false;
  if(strchr(host, ':') || strchr(host, '#')){
    if(!tcrdbopen2(rdb, host)) err = true;
  } else {
    if(!tcrdbopen(rdb, host, port)) err = true;
  }
  return !err;
}


/* parse arguments of write command */
static int runwrite(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int tnum = 1;
  bool nr = false;
  char *ext = NULL;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tnum")){
        if(++i >= argc) usage();
        tnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nr")){
        nr = true;
      } else if(!strcmp(argv[i], "-ext")){
        if(++i >= argc) usage();
        ext = argv[i];
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !rstr || tnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwrite(host, port, tnum, rnum, nr, ext, rnd);
  return rv;
}


/* parse arguments of read command */
static int runread(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  int tnum = 1;
  int mul = 0;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tnum")){
        if(++i >= argc) usage();
        tnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-mul")){
        if(++i >= argc) usage();
        mul = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host || tnum < 1) usage();
  int rv = procread(host, port, tnum, mul, rnd);
  return rv;
}


/* parse arguments of remove command */
static int runremove(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  int tnum = 1;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tnum")){
        if(++i >= argc) usage();
        tnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host || tnum < 1) usage();
  int rv = procremove(host, port, tnum, rnd);
  return rv;
}


/* parse arguments of typical command */
static int runtypical(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int tnum = 1;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tnum")){
        if(++i >= argc) usage();
        tnum = tcatoi(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !rstr || tnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = proctypical(host, port, tnum, rnum);
  return rv;
}


/* parse arguments of table command */
static int runtable(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int tnum = 1;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tnum")){
        if(++i >= argc) usage();
        tnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !rstr || tnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = proctable(host, port, tnum, rnum, rnd);
  return rv;
}


/* perform write command */
static int procwrite(const char *host, int port, int tnum, int rnum,
                     bool nr, const char *ext, bool rnd){
  iprintf("<Writing Test>\n  host=%s  port=%d  tnum=%d  rnum=%d  nr=%d  ext=%s  rnd=%d\n\n",
          host, port, tnum, rnum, nr, ext ? ext : "", rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[tnum];
  for(int i = 0; i < tnum; i++){
    rdbs[i] = tcrdbnew();
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  TARGWRITE targs[tnum];
  pthread_t threads[tnum];
  if(tnum == 1){
    targs[0].rdb = rdbs[0];
    targs[0].rnum = rnum;
    targs[0].nr = nr;
    targs[0].ext = ext;
    targs[0].rnd = rnd;
    targs[0].id = 0;
    if(threadwrite(targs) != NULL) err = true;
  } else {
    for(int i = 0; i < tnum; i++){
      targs[i].rdb = rdbs[i];
      targs[i].rnum = rnum;
      targs[i].nr = nr;
      targs[i].ext = ext;
      targs[i].rnd = rnd;
      targs[i].id = i;
      if(pthread_create(threads + i, NULL, threadwrite, targs + i) != 0){
        eprint(rdb, __LINE__, "pthread_create");
        targs[i].id = -1;
        err = true;
      }
    }
    for(int i = 0; i < tnum; i++){
      if(targs[i].id == -1) continue;
      void *rv;
      if(pthread_join(threads[i], &rv) != 0){
        eprint(rdb, __LINE__, "pthread_join");
        err = true;
      } else if(rv){
        err = true;
      }
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < tnum; i++){
    if(!tcrdbclose(rdbs[i])){
      eprint(rdbs[i], __LINE__, "tcrdbclose");
      err = true;
    }
    tcrdbdel(rdbs[i]);
  }
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform read command */
static int procread(const char *host, int port, int tnum, int mul, bool rnd){
  iprintf("<Reading Test>\n  host=%s  port=%d  tnum=%d  mul=%d  rnd=%d\n\n",
          host, port, tnum, mul, rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[tnum];
  for(int i = 0; i < tnum; i++){
    rdbs[i] = tcrdbnew();
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  int rnum = tcrdbrnum(rdb) / tnum;
  TARGREAD targs[tnum];
  pthread_t threads[tnum];
  if(tnum == 1){
    targs[0].rdb = rdbs[0];
    targs[0].rnum = rnum;
    targs[0].mul = mul;
    targs[0].rnd = rnd;
    targs[0].id = 0;
    if(threadread(targs) != NULL) err = true;
  } else {
    for(int i = 0; i < tnum; i++){
      targs[i].rdb = rdbs[i];
      targs[i].rnum = rnum;
      targs[i].mul = mul;
      targs[i].rnd = rnd;
      targs[i].id = i;
      if(pthread_create(threads + i, NULL, threadread, targs + i) != 0){
        eprint(rdb, __LINE__, "pthread_create");
        targs[i].id = -1;
        err = true;
      }
    }
    for(int i = 0; i < tnum; i++){
      if(targs[i].id == -1) continue;
      void *rv;
      if(pthread_join(threads[i], &rv) != 0){
        eprint(rdb, __LINE__, "pthread_join");
        err = true;
      } else if(rv){
        err = true;
      }
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < tnum; i++){
    if(!tcrdbclose(rdbs[i])){
      eprint(rdbs[i], __LINE__, "tcrdbclose");
      err = true;
    }
    tcrdbdel(rdbs[i]);
  }
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform remove command */
static int procremove(const char *host, int port, int tnum, bool rnd){
  iprintf("<Removing Test>\n  host=%s  port=%d  tnum=%d  rnd=%d\n\n", host, port, tnum, rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[tnum];
  for(int i = 0; i < tnum; i++){
    rdbs[i] = tcrdbnew();
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  int rnum = tcrdbrnum(rdb) / tnum;
  TARGREMOVE targs[tnum];
  pthread_t threads[tnum];
  if(tnum == 1){
    targs[0].rdb = rdbs[0];
    targs[0].rnum = rnum;
    targs[0].rnd = rnd;
    targs[0].id = 0;
    if(threadremove(targs) != NULL) err = true;
  } else {
    for(int i = 0; i < tnum; i++){
      targs[i].rdb = rdbs[i];
      targs[i].rnum = rnum;
      targs[i].rnd = rnd;
      targs[i].id = i;
      if(pthread_create(threads + i, NULL, threadremove, targs + i) != 0){
        eprint(rdb, __LINE__, "pthread_create");
        targs[i].id = -1;
        err = true;
      }
    }
    for(int i = 0; i < tnum; i++){
      if(targs[i].id == -1) continue;
      void *rv;
      if(pthread_join(threads[i], &rv) != 0){
        eprint(rdb, __LINE__, "pthread_join");
        err = true;
      } else if(rv){
        err = true;
      }
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < tnum; i++){
    if(!tcrdbclose(rdbs[i])){
      eprint(rdbs[i], __LINE__, "tcrdbclose");
      err = true;
    }
    tcrdbdel(rdbs[i]);
  }
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform typical command */
static int proctypical(const char *host, int port, int tnum, int rnum){
  iprintf("<Typical Access Test>\n  host=%s  port=%d  tnum=%d  rnum=%d\n\n",
          host, port, tnum, rnum);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[tnum];
  for(int i = 0; i < tnum; i++){
    rdbs[i] = tcrdbnew();
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  TARGTYPICAL targs[tnum];
  pthread_t threads[tnum];
  if(tnum == 1){
    targs[0].rdb = rdbs[0];
    targs[0].rnum = rnum;
    targs[0].id = 0;
    if(threadtypical(targs) != NULL) err = true;
  } else {
    for(int i = 0; i < tnum; i++){
      targs[i].rdb = rdbs[i];
      targs[i].rnum = rnum;
      targs[i].id = i;
      if(pthread_create(threads + i, NULL, threadtypical, targs + i) != 0){
        eprint(rdb, __LINE__, "pthread_create");
        targs[i].id = -1;
        err = true;
      }
    }
    for(int i = 0; i < tnum; i++){
      if(targs[i].id == -1) continue;
      void *rv;
      if(pthread_join(threads[i], &rv) != 0){
        eprint(rdb, __LINE__, "pthread_join");
        err = true;
      } else if(rv){
        err = true;
      }
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < tnum; i++){
    if(!tcrdbclose(rdbs[i])){
      eprint(rdbs[i], __LINE__, "tcrdbclose");
      err = true;
    }
    tcrdbdel(rdbs[i]);
  }
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform table command */
static int proctable(const char *host, int port, int tnum, int rnum, bool rnd){
  iprintf("<Table Extension Test>\n  host=%s  port=%d  tnum=%d  rnum=%d  rnd=%d\n\n",
          host, port, tnum, rnum, rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[tnum];
  for(int i = 0; i < tnum; i++){
    rdbs[i] = tcrdbnew();
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  if(!tcrdbvanish(rdb)){
    eprint(rdb, __LINE__, "tcrdbvanish");
    err = true;
  }
  if(!tcrdbtblsetindex(rdb, "c", RDBITLEXICAL)){
    eprint(rdb, __LINE__, "tcrdbtblsetindex");
    err = true;
  }
  if(!tcrdbtblsetindex(rdb, "x", RDBITDECIMAL)){
    eprint(rdb, __LINE__, "tcrdbtblsetindex");
    err = true;
  }
  TARGTABLE targs[tnum];
  pthread_t threads[tnum];
  if(tnum == 1){
    targs[0].rdb = rdbs[0];
    targs[0].rnum = rnum;
    targs[0].rnd = rnd;
    targs[0].id = 0;
    if(threadtable(targs) != NULL) err = true;
  } else {
    for(int i = 0; i < tnum; i++){
      targs[i].rdb = rdbs[i];
      targs[i].rnum = rnum;
      targs[i].rnd = rnd;
      targs[i].id = i;
      if(pthread_create(threads + i, NULL, threadtable, targs + i) != 0){
        eprint(rdb, __LINE__, "pthread_create");
        targs[i].id = -1;
        err = true;
      }
    }
    for(int i = 0; i < tnum; i++){
      if(targs[i].id == -1) continue;
      void *rv;
      if(pthread_join(threads[i], &rv) != 0){
        eprint(rdb, __LINE__, "pthread_join");
        err = true;
      } else if(rv){
        err = true;
      }
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < tnum; i++){
    if(!tcrdbclose(rdbs[i])){
      eprint(rdbs[i], __LINE__, "tcrdbclose");
      err = true;
    }
    tcrdbdel(rdbs[i]);
  }
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* thread the write function */
static void *threadwrite(void *targ){
  TCRDB *rdb = ((TARGWRITE *)targ)->rdb;
  int rnum = ((TARGWRITE *)targ)->rnum;
  bool nr = ((TARGWRITE *)targ)->nr;
  const char *ext = ((TARGWRITE *)targ)->ext;
  bool rnd = ((TARGWRITE *)targ)->rnd;
  int id = ((TARGWRITE *)targ)->id;
  bool err = false;
  int base = id * rnum;
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", base + (rnd ? myrand(i) + 1 : i));
    if(nr){
      if(!tcrdbputnr(rdb, buf, len, buf, len)){
        eprint(rdb, __LINE__, "tcrdbputnr");
        err = true;
        break;
      }
    } else if(ext){
      int rsiz;
      char *rbuf = tcrdbext(rdb, ext, 0, buf, len, buf, len, &rsiz);
      if(!rbuf && tcrdbecode(rdb) != TCEMISC){
        eprint(rdb, __LINE__, "tcrdbext");
        err = true;
        break;
      }
      tcfree(rbuf);
    } else {
      if(!tcrdbput(rdb, buf, len, buf, len)){
        eprint(rdb, __LINE__, "tcrdbput");
        err = true;
        break;
      }
    }
    if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
      putchar('.');
      fflush(stdout);
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  return err ? "error" : NULL;
}


/* thread the read function */
static void *threadread(void *targ){
  TCRDB *rdb = ((TARGREAD *)targ)->rdb;
  int rnum = ((TARGREAD *)targ)->rnum;
  int mul = ((TARGREAD *)targ)->mul;
  bool rnd = ((TARGREAD *)targ)->rnd;
  int id = ((TARGREAD *)targ)->id;
  bool err = false;
  int base = id * rnum;
  TCMAP *recs = mul > 1 ? tcmapnew() : NULL;
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", base + (rnd ? myrandnd(i) + 1 : i));
    if(mul > 1){
      tcmapput(recs, kbuf, ksiz, kbuf, ksiz);
      if(i % mul == 0){
        if(!tcrdbget3(rdb, recs)){
          eprint(rdb, __LINE__, "tcrdbget3");
          err = true;
          break;
        }
        tcmapclear(recs);
      }
    } else {
      int vsiz;
      char *vbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz);
      if(!vbuf && !rnd){
        eprint(rdb, __LINE__, "tcrdbget");
        err = true;
        break;
      }
      tcfree(vbuf);
    }
    if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
      putchar('.');
      fflush(stdout);
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(recs) tcmapdel(recs);
  return err ? "error" : NULL;
}


/* thread the remove function */
static void *threadremove(void *targ){
  TCRDB *rdb = ((TARGREMOVE *)targ)->rdb;
  int rnum = ((TARGREMOVE *)targ)->rnum;
  bool rnd = ((TARGREMOVE *)targ)->rnd;
  int id = ((TARGREMOVE *)targ)->id;
  bool err = false;
  int base = id * rnum;
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", base + (rnd ? myrandnd(i) + 1 : i));
    if(!tcrdbout(rdb, kbuf, ksiz) && !rnd){
      eprint(rdb, __LINE__, "tcrdbout");
      err = true;
      break;
    }
    if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
      putchar('.');
      fflush(stdout);
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  return err ? "error" : NULL;
}


/* thread the typical function */
static void *threadtypical(void *targ){
  TCRDB *rdb = ((TARGTYPICAL *)targ)->rdb;
  int rnum = ((TARGTYPICAL *)targ)->rnum;
  int id = ((TARGTYPICAL *)targ)->id;
  bool err = false;
  int range = (id + 1) * rnum;
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(range) + 1);
    char *vbuf;
    int vsiz;
    switch(myrand(6)){
      case 0:
        if(!tcrdbput(rdb, kbuf, ksiz, kbuf, ksiz)){
          eprint(rdb, __LINE__, "tcrdbput");
          err = true;
        }
        break;
      case 1:
        if(!tcrdbputkeep(rdb, kbuf, ksiz, kbuf, ksiz) && tcrdbecode(rdb) != TTEKEEP){
          eprint(rdb, __LINE__, "tcrdbputkeep");
          err = true;
        }
        break;
      case 2:
        if(!tcrdbputcat(rdb, kbuf, ksiz, kbuf, ksiz)){
          eprint(rdb, __LINE__, "tcrdbputcat");
          err = true;
        }
        break;
      case 3:
        if(!tcrdbputnr(rdb, kbuf, ksiz, kbuf, ksiz)){
          eprint(rdb, __LINE__, "tcrdbputnr");
          err = true;
        }
        break;
      case 4:
        if(!tcrdbout(rdb, kbuf, ksiz) && tcrdbecode(rdb) != TTENOREC){
          eprint(rdb, __LINE__, "tcrdbout");
          err = true;
        }
        break;
      default:
        vbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz);
        if(vbuf){
          tcfree(vbuf);
        } else if(tcrdbecode(rdb) != TTENOREC){
          eprint(rdb, __LINE__, "tcrdbget");
          err = true;
        }
    }
    if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
      putchar('.');
      fflush(stdout);
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  return err ? "error" : NULL;
}


/* thread the table function */
static void *threadtable(void *targ){
  TCRDB *rdb = ((TARGTABLE *)targ)->rdb;
  int rnum = ((TARGTABLE *)targ)->rnum;
  bool rnd = ((TARGREMOVE *)targ)->rnd;
  int id = ((TARGTABLE *)targ)->id;
  bool err = false;
  int base = id * rnum;
  for(int i = 1; i <= rnum && !err; i++){
    char pkbuf[RECBUFSIZ];
    int pksiz = sprintf(pkbuf, "%08d", base + (rnd ? myrand(rnum / 2 + i) : i));
    TCMAP *cols = tcmapnew2(7);
    char vbuf[RECBUFSIZ*5];
    int vsiz = sprintf(vbuf, "%d", myrand(i) + 1);
    tcmapput(cols, "c", 1, vbuf, vsiz);
    vsiz = sprintf(vbuf, "%lld", (long long)tctime() + (rnd ? myrand(3600) + 1 : 60));
    tcmapput(cols, "x", 1, vbuf, vsiz);
    if(rnd){
      int act = myrand(100);
      if(act < 5){
        RDBQRY *qry = tcrdbqrynew(rdb);
        if(myrand(5) == 0){
          sprintf(vbuf, "%d", myrand(i));
          tcrdbqryaddcond(qry, "c", RDBQCSTREQ, vbuf);
        } else {
          sprintf(vbuf, "%d,%d", myrand(rnum), myrand(rnum));
          tcrdbqryaddcond(qry, "x", RDBQCNUMBT, vbuf);
        }
        tcrdbqrysetlimit(qry, 10, 0);
        TCLIST *res = tcrdbqrysearch(qry);
        tclistdel(res);
        tcrdbqrydel(qry);
      } else if(act < 10){
        if(!tcrdbtblout(rdb, pkbuf, pksiz) && tcrdbecode(rdb) != TTENOREC){
          eprint(rdb, __LINE__, "tcrdbtblout");
          err = true;
        }
      } else {
        if(!tcrdbtblput(rdb, pkbuf, pksiz, cols)){
          eprint(rdb, __LINE__, "tcrdbtblput");
          err = true;
        }
      }
    } else {
      if(!tcrdbtblput(rdb, pkbuf, pksiz, cols)){
        eprint(rdb, __LINE__, "tcrdbtblput");
        err = true;
      }
    }
    tcmapdel(cols);
    if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
      putchar('.');
      fflush(stdout);
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  return err ? "error" : NULL;
}



// END OF FILE
