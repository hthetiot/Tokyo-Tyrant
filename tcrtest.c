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


/* global variables */
const char *g_progname;                  // program name


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void iprintf(const char *format, ...);
static void iputchar(int c);
static void eprint(TCRDB *rdb, int line, const char *func);
static int myrand(int range);
static bool myopen(TCRDB *rdb, const char *host, int port);
static int runwrite(int argc, char **argv);
static int runread(int argc, char **argv);
static int runremove(int argc, char **argv);
static int runrcat(int argc, char **argv);
static int runmisc(int argc, char **argv);
static int runwicked(int argc, char **argv);
static int runtable(int argc, char **argv);
static int procwrite(const char *host, int port, int cnum, int tout,
                     int rnum, bool nr, bool rnd);
static int procread(const char *host, int port, int cnum, int tout, int mul, bool rnd);
static int procremove(const char *host, int port, int cnum, int tout, bool rnd);
static int procrcat(const char *host, int port, int cnum, int tout, int rnum,
                    int shl, bool dai, bool dad, const char *ext, int xopts);
static int procmisc(const char *host, int port, int cnum, int tout, int rnum);
static int procwicked(const char *host, int port, int cnum, int tout, int rnum);
static int proctable(const char *host, int port, int cnum, int tout, int rnum, int exp);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  srand((unsigned int)(tctime() * 1000) % UINT_MAX);
  signal(SIGPIPE, SIG_IGN);
  if(argc < 2) usage();
  int rv = 0;
  if(!strcmp(argv[1], "write")){
    rv = runwrite(argc, argv);
  } else if(!strcmp(argv[1], "read")){
    rv = runread(argc, argv);
  } else if(!strcmp(argv[1], "remove")){
    rv = runremove(argc, argv);
  } else if(!strcmp(argv[1], "rcat")){
    rv = runrcat(argc, argv);
  } else if(!strcmp(argv[1], "misc")){
    rv = runmisc(argc, argv);
  } else if(!strcmp(argv[1], "wicked")){
    rv = runwicked(argc, argv);
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
  fprintf(stderr, "  %s write [-port num] [-cnum num] [-tout num] [-nr] [-rnd] host rnum\n",
          g_progname);
  fprintf(stderr, "  %s read [-port num] [-cnum num] [-tout num] [-mul num] [-rnd] host\n",
          g_progname);
  fprintf(stderr, "  %s remove [-port num] [-cnum num] [-tout num] [-rnd] host\n", g_progname);
  fprintf(stderr, "  %s rcat [-port num] [-cnum num] [-tout num] [-shl num] [-dai|-dad]"
          " [-ext name] [-xlr|-xlg] host rnum\n", g_progname);
  fprintf(stderr, "  %s misc [-port num] [-cnum num] [-tout num] host rnum\n", g_progname);
  fprintf(stderr, "  %s wicked [-port num] [-cnum num] [-tout num] host rnum\n", g_progname);
  fprintf(stderr, "  %s table [-port num] [-cnum num] [-tout num] [-exp num] host rnum\n",
          g_progname);
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


/* print a character and flush the buffer */
static void iputchar(int c){
  putchar(c);
  fflush(stdout);
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
  int cnum = 1;
  int tout = 0;
  bool nr = false;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nr")){
        nr = true;
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
  if(!host || !rstr || cnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwrite(host, port, cnum, tout, rnum, nr, rnd);
  return rv;
}


/* parse arguments of read command */
static int runread(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  int cnum = 1;
  int tout = 0;
  int mul = 0;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
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
  if(!host || cnum < 1) usage();
  int rv = procread(host, port, cnum, tout, mul, rnd);
  return rv;
}


/* parse arguments of remove command */
static int runremove(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  int cnum = 1;
  int tout = 0;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
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
  if(!host || cnum < 1) usage();
  int rv = procremove(host, port, cnum, tout, rnd);
  return rv;
}


/* parse arguments of rcat command */
static int runrcat(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int cnum = 1;
  int tout = 0;
  int shl = 0;
  bool dai = false;
  bool dad = false;
  char *ext = NULL;
  int xopts = 0;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-shl")){
        if(++i >= argc) usage();
        shl = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-dai")){
        dai = true;
      } else if(!strcmp(argv[i], "-dad")){
        dad = true;
      } else if(!strcmp(argv[i], "-ext")){
        if(++i >= argc) usage();
        ext = argv[i];
      } else if(!strcmp(argv[i], "-xlr")){
        xopts |= RDBXOLCKREC;
      } else if(!strcmp(argv[i], "-xlg")){
        xopts |= RDBXOLCKGLB;
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
  if(!host || !rstr || cnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procrcat(host, port, cnum, tout, rnum, shl, dai, dad, ext, xopts);
  return rv;
}


/* parse arguments of misc command */
static int runmisc(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int cnum = 1;
  int tout = 0;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
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
  if(!host || !rstr || cnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procmisc(host, port, cnum, tout, rnum);
  return rv;
}


/* parse arguments of wicked command */
static int runwicked(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int cnum = 1;
  int tout = 0;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
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
  if(!host || !rstr || cnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwicked(host, port, cnum, tout, rnum);
  return rv;
}


/* parse arguments of table command */
static int runtable(int argc, char **argv){
  char *host = NULL;
  char *rstr = NULL;
  int port = TTDEFPORT;
  int cnum = 1;
  int tout = 0;
  int exp = 0;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-cnum")){
        if(++i >= argc) usage();
        cnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-tout")){
        if(++i >= argc) usage();
        tout = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-exp")){
        if(++i >= argc) usage();
        exp = tcatoi(argv[i]);
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
  if(!host || !rstr || cnum < 1) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = proctable(host, port, cnum, tout, rnum, exp);
  return rv;
}


/* perform write command */
static int procwrite(const char *host, int port, int cnum, int tout,
                     int rnum, bool nr, bool rnd){
  iprintf("<Writing Test>\n  host=%s  port=%d  cnum=%d  tout=%d  rnum=%d  nr=%d  rnd=%d\n\n",
          host, port, cnum, tout, rnum, nr, rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  if(!rnd && !tcrdbvanish(rdb)){
    eprint(rdb, __LINE__, "tcrdbvanish");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", rnd ? myrand(rnum) + 1 : i);
    if(nr){
      if(!tcrdbputnr(rdb, buf, len, buf, len)){
        eprint(rdb, __LINE__, "tcrdbputnr");
        err = true;
        break;
      }
    } else {
      if(!tcrdbput(rdb, buf, len, buf, len)){
        eprint(rdb, __LINE__, "tcrdbput");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < cnum; i++){
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
static int procread(const char *host, int port, int cnum, int tout, int mul, bool rnd){
  iprintf("<Reading Test>\n  host=%s  port=%d  cnum=%d  tout=%d  mul=%d  rnd=%d\n\n",
          host, port, cnum, tout, mul, rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  TCMAP *recs = (mul > 1) ? tcmapnew() : NULL;
  int rnum = tcrdbrnum(rdb);
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", rnd ? myrand(rnum) + 1 : i);
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
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  if(recs) tcmapdel(recs);
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < cnum; i++){
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
static int procremove(const char *host, int port, int cnum, int tout, bool rnd){
  iprintf("<Removing Test>\n  host=%s  port=%d  cnum=%d  tout=%d  rnd=%d\n\n",
          host, port, cnum, tout, rnd);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  int rnum = tcrdbrnum(rdb);
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", rnd ? myrand(rnum) + 1 : i);
    if(!tcrdbout(rdb, kbuf, ksiz) && !rnd){
      eprint(rdb, __LINE__, "tcrdbout");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < cnum; i++){
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


/* perform rcat command */
static int procrcat(const char *host, int port, int cnum, int tout, int rnum,
                    int shl, bool dai, bool dad, const char *ext, int xopts){
  iprintf("<Random Concatenating Test>\n  host=%s  port=%d  cnum=%d  tout=%d  rnum=%d"
          "  shl=%d  dai=%d  dad=%d  ext=%s  xopts=%d\n\n",
          host, port, cnum, tout, rnum, shl, dai, dad, ext ? ext : "", xopts);
  int pnum = rnum / 5 + 1;
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
    if(!myopen(rdbs[i], host, port)){
      eprint(rdbs[i], __LINE__, "tcrdbopen");
      err = true;
    }
  }
  TCRDB *rdb = rdbs[0];
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(pnum) + 1);
    if(shl > 0){
      if(!tcrdbputshl(rdb, kbuf, ksiz, kbuf, ksiz, shl)){
        eprint(rdb, __LINE__, "tcrdbputshl");
        err = true;
        break;
      }
    } else if(dai){
      if(tcrdbaddint(rdb, kbuf, ksiz, 1) == INT_MIN){
        eprint(rdb, __LINE__, "tcrdbaddint");
        err = true;
        break;
      }
    } else if(dad){
      if(isnan(tcrdbadddouble(rdb, kbuf, ksiz, 1.0))){
        eprint(rdb, __LINE__, "tcrdbadddouble");
        err = true;
        break;
      }
    } else if(ext){
      int xsiz;
      char *xbuf = tcrdbext(rdb, ext, xopts, kbuf, ksiz, kbuf, ksiz, &xsiz);
      if(!xbuf && tcrdbecode(rdb) != TTEMISC){
        eprint(rdb, __LINE__, "tcrdbext");
        err = true;
        break;
      }
      tcfree(xbuf);
    } else {
      if(!tcrdbputcat(rdb, kbuf, ksiz, kbuf, ksiz)){
        eprint(rdb, __LINE__, "tcrdbputcat");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < cnum; i++){
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


/* perform misc command */
static int procmisc(const char *host, int port, int cnum, int tout, int rnum){
  iprintf("<Miscellaneous Test>\n  host=%s  port=%d  cnum=%d  tout=%d  rnum=%d\n\n",
          host, port, cnum, tout, rnum);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
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
  iprintf("writing:\n");
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(myrand(10) > 0){
      if(!tcrdbputkeep(rdb, buf, len, buf, len)){
        eprint(rdb, __LINE__, "tcrdbputkeep");
        err = true;
        break;
      }
    } else {
      if(!tcrdbputnr(rdb, buf, len, buf, len)){
        eprint(rdb, __LINE__, "tcrdbputnr");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  for(int i = 0; i < cnum; i++){
    if(tcrdbrnum(rdbs[i]) < 1){
      eprint(rdb, __LINE__, "tcrdbrnum");
      err = true;
      break;
    }
  }
  iprintf("reading:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", i);
    int vsiz;
    char *vbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(rdb, __LINE__, "tcrdbget");
      err = true;
      break;
    } else if(vsiz != ksiz || memcmp(vbuf, kbuf, vsiz)){
      eprint(rdb, __LINE__, "(validation)");
      err = true;
      tcfree(vbuf);
      break;
    }
    tcfree(vbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  if(tcrdbrnum(rdb) != rnum){
    eprint(rdb, __LINE__, "(validation)");
    err = true;
  }
  iprintf("random writing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum) + 1);
    char vbuf[RECBUFSIZ];
    int vsiz = myrand(RECBUFSIZ);
    memset(vbuf, '*', vsiz);
    if(!tcrdbput(rdb, kbuf, ksiz, vbuf, vsiz)){
      eprint(rdb, __LINE__, "tcrdbput");
      err = true;
      break;
    }
    int rsiz;
    char *rbuf = tcrdbget(rdb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(rdb, __LINE__, "tcrdbget");
      err = true;
      break;
    }
    if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(rdb, __LINE__, "(validation)");
      err = true;
      tcfree(rbuf);
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
    tcfree(rbuf);
  }
  iprintf("word writing:\n");
  const char *words[] = {
    "a", "A", "bb", "BB", "ccc", "CCC", "dddd", "DDDD", "eeeee", "EEEEEE",
    "mikio", "hirabayashi", "tokyo", "cabinet", "hyper", "estraier", "19780211", "birth day",
    "one", "first", "two", "second", "three", "third", "four", "fourth", "five", "fifth",
    "_[1]_", "uno", "_[2]_", "dos", "_[3]_", "tres", "_[4]_", "cuatro", "_[5]_", "cinco",
    "[\xe5\xb9\xb3\xe6\x9e\x97\xe5\xb9\xb9\xe9\x9b\x84]", "[\xe9\xa6\xac\xe9\xb9\xbf]", NULL
  };
  for(int i = 0; words[i] != NULL; i += 2){
    const char *kbuf = words[i];
    int ksiz = strlen(kbuf);
    const char *vbuf = words[i+1];
    int vsiz = strlen(vbuf);
    if(!tcrdbputkeep(rdb, kbuf, ksiz, vbuf, vsiz)){
      eprint(rdb, __LINE__, "tcrdbputkeep");
      err = true;
      break;
    }
    if(rnum > 250) putchar('.');
  }
  if(rnum > 250) iprintf(" (%08d)\n", (int)(sizeof(words) / sizeof(*words)));
  iprintf("random erasing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    if(!tcrdbout(rdb, kbuf, ksiz) && tcrdbecode(rdb) != TTENOREC){
      eprint(rdb, __LINE__, "tcrdbout");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("writing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "[%d]", i);
    char vbuf[RECBUFSIZ];
    int vsiz = i % RECBUFSIZ;
    memset(vbuf, '*', vsiz);
    if(!tcrdbputkeep(rdb, kbuf, ksiz, vbuf, vsiz)){
      eprint(rdb, __LINE__, "tcrdbputkeep");
      err = true;
      break;
    }
    if(vsiz < 1){
      char tbuf[PATH_MAX];
      for(int j = 0; j < PATH_MAX; j++){
        tbuf[j] = myrand(0x100);
      }
      if(!tcrdbput(rdb, kbuf, ksiz, tbuf, PATH_MAX)){
        eprint(rdb, __LINE__, "tcrdbput");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("erasing:\n");
  for(int i = 1; i <= rnum; i++){
    if(i % 2 == 1){
      char kbuf[RECBUFSIZ];
      int ksiz = sprintf(kbuf, "[%d]", i);
      if(!tcrdbout(rdb, kbuf, ksiz)){
        eprint(rdb, __LINE__, "tcrdbout");
        err = true;
        break;
      }
      tcrdbout(rdb, kbuf, ksiz);
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("random multi reading:\n");
  for(int i = 1; i <= rnum; i++){
    if(i % 10 == 1){
      TCMAP *recs = tcmapnew();
      int num = myrand(10);
      if(myrand(2) == 0){
        char pbuf[RECBUFSIZ];
        int psiz = sprintf(pbuf, "%d", myrand(100) + 1);
        TCLIST *keys = tcrdbfwmkeys(rdb, pbuf, psiz, num);
        for(int j = 0; j < tclistnum(keys); j++){
          int ksiz;
          const char *kbuf = tclistval(keys, j, &ksiz);
          tcmapput(recs, kbuf, ksiz, kbuf, ksiz);
        }
        tclistdel(keys);
      } else {
        for(int j = 0; j < num; j++){
          char kbuf[RECBUFSIZ];
          int ksiz = sprintf(kbuf, "%d", myrand(rnum) + 1);
          tcmapput(recs, kbuf, ksiz, kbuf, ksiz);
        }
      }
      if(tcrdbget3(rdb, recs)){
        tcmapiterinit(recs);
        const char *kbuf;
        int ksiz;
        while((kbuf = tcmapiternext(recs, &ksiz))){
          int vsiz;
          const char *vbuf = tcmapiterval(kbuf, &vsiz);
          int rsiz;
          char *rbuf = tcrdbget(rdb, kbuf, ksiz, &rsiz);
          if(rbuf){
            if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
              eprint(rdb, __LINE__, "(validation)");
              err = true;
            }
            tcfree(rbuf);
          } else {
            eprint(rdb, __LINE__, "tcrdbget");
            err = true;
          }
        }
      } else {
        eprint(rdb, __LINE__, "tcrdbget3");
        err = true;
      }
      tcmapdel(recs);
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("script extension calling:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "(%d)", i);
    const char *name;
    switch(myrand(7)){
      default: name = "put"; break;
      case 1: name = "putkeep"; break;
      case 2: name = "putcat"; break;
      case 3: name = "out"; break;
      case 4: name = "get"; break;
      case 5: name = "iterinit"; break;
      case 6: name = "iternext"; break;
    }
    int vsiz;
    char *vbuf = tcrdbext(rdb, name, 0, kbuf, ksiz, kbuf, ksiz, &vsiz);
    if(!vbuf && tcrdbecode(rdb) != TTEMISC){
      eprint(rdb, __LINE__, "tcrdbext");
      err = true;
      break;
    }
    tcfree(vbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      rdb = rdbs[myrand(rnum)%cnum];
    }
  }
  iprintf("checking versatile functions:\n");
  TCLIST *args = tclistnew();
  for(int i = 1; i <= rnum; i++){
    if(myrand(10) == 0){
      const char *name;
      switch(myrand(3)){
        default: name = "putlist"; break;
        case 1: name = "outlist"; break;
        case 2: name = "getlist"; break;
      }
      TCLIST *res = tcrdbmisc(rdb, name, 0, args);
      if(res){
        tclistdel(res);
      } else {
        eprint(rdb, __LINE__, "tcrdbmisc");
        err = true;
        break;
      }
      tclistclear(args);
    } else {
      char kbuf[RECBUFSIZ];
      int ksiz = sprintf(kbuf, "(%d)", myrand(rnum));
      tclistpush(args, kbuf, ksiz);
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  tclistdel(args);
  char *sbuf = tcrdbstat(rdb);
  if(sbuf){
    tcfree(sbuf);
  } else {
    eprint(rdb, __LINE__, "tcrdbstat");
    err = true;
  }
  if(!tcrdbsync(rdb)){
    eprint(rdb, __LINE__, "tcrdbsync");
    err = true;
  }
  if(!tcrdboptimize(rdb, NULL)){
    eprint(rdb, __LINE__, "tcrdbsync");
    err = true;
  }
  if(!tcrdbvanish(rdb)){
    eprint(rdb, __LINE__, "tcrdbvanish");
    err = true;
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < cnum; i++){
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


/* perform wicked command */
static int procwicked(const char *host, int port, int cnum, int tout, int rnum){
  iprintf("<Wicked Writing Test>\n  host=%s  port=%d  cnum=%d  tout=%d  rnum=%d\n\n",
          host, port, cnum, tout, rnum);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
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
  TCMAP *map = tcmapnew2(rnum / 5);
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    char vbuf[RECBUFSIZ];
    int vsiz = myrand(RECBUFSIZ);
    memset(vbuf, '*', vsiz);
    vbuf[vsiz] = '\0';
    char *rbuf;
    switch(myrand(16)){
      case 0:
        putchar('0');
        if(!tcrdbput(rdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(rdb, __LINE__, "tcrdbput");
          err = true;
        }
        tcmapput(map, kbuf, ksiz, vbuf, vsiz);
        break;
      case 1:
        putchar('1');
        if(!tcrdbput2(rdb, kbuf, vbuf)){
          eprint(rdb, __LINE__, "tcrdbput2");
          err = true;
        }
        tcmapput2(map, kbuf, vbuf);
        break;
      case 2:
        putchar('2');
        tcrdbputkeep(rdb, kbuf, ksiz, vbuf, vsiz);
        tcmapputkeep(map, kbuf, ksiz, vbuf, vsiz);
        break;
      case 3:
        putchar('3');
        tcrdbputkeep2(rdb, kbuf, vbuf);
        tcmapputkeep2(map, kbuf, vbuf);
        break;
      case 4:
        putchar('4');
        if(!tcrdbputcat(rdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(rdb, __LINE__, "tcrdbputcat");
          err = true;
        }
        tcmapputcat(map, kbuf, ksiz, vbuf, vsiz);
        break;
      case 5:
        putchar('5');
        if(!tcrdbputcat2(rdb, kbuf, vbuf)){
          eprint(rdb, __LINE__, "tcrdbputcat2");
          err = true;
        }
        tcmapputcat2(map, kbuf, vbuf);
        break;
      case 6:
        putchar('6');
        if(myrand(10) == 0){
          if(!tcrdbputnr(rdb, kbuf, ksiz, vbuf, vsiz)){
            eprint(rdb, __LINE__, "tcrdbputcat");
            err = true;
          }
          if(tcrdbrnum(rdb) < 1){
            eprint(rdb, __LINE__, "tcrdbrnum");
            err = true;
          }
          tcmapput(map, kbuf, ksiz, vbuf, vsiz);
        }
        break;
      case 7:
        putchar('7');
        if(myrand(10) == 0){
          if(!tcrdbputnr2(rdb, kbuf, vbuf)){
            eprint(rdb, __LINE__, "tcrdbputcat2");
            err = true;
          }
          if(tcrdbrnum(rdb) < 1){
            eprint(rdb, __LINE__, "tcrdbrnum");
            err = true;
          }
          tcmapput2(map, kbuf, vbuf);
        }
        break;
      case 8:
        putchar('8');
        if(myrand(10) == 0){
          tcrdbout(rdb, kbuf, ksiz);
          tcmapout(map, kbuf, ksiz);
        }
        break;
      case 9:
        putchar('9');
        if(myrand(10) == 0){
          tcrdbout2(rdb, kbuf);
          tcmapout2(map, kbuf);
        }
        break;
      case 10:
        putchar('A');
        if((rbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz)) != NULL) tcfree(rbuf);
        break;
      case 11:
        putchar('B');
        if((rbuf = tcrdbget2(rdb, kbuf)) != NULL) tcfree(rbuf);
        break;
      case 12:
        putchar('C');
        tcrdbvsiz(rdb, kbuf, ksiz);
        break;
      case 13:
        putchar('D');
        tcrdbvsiz2(rdb, kbuf);
        break;
      case 14:
        putchar('E');
        if(myrand(rnum / 50) == 0){
          if(!tcrdbiterinit(rdb)){
            eprint(rdb, __LINE__, "tcrdbiterinit");
            err = true;
          }
        }
        for(int j = myrand(rnum) / 1000 + 1; j >= 0; j--){
          int iksiz;
          char *ikbuf = tcrdbiternext(rdb, &iksiz);
          if(ikbuf) tcfree(ikbuf);
        }
        break;
      default:
        putchar('@');
        if(myrand(10000) == 0) srand((unsigned int)(tctime() * 1000) % UINT_MAX);
        rdb = rdbs[myrand(rnum)%cnum];
        break;
    }
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  if(rnum % 50 > 0) iprintf(" (%08d)\n", rnum);
  tcrdbsync(rdb);
  if(tcrdbrnum(rdb) != tcmaprnum(map)){
    eprint(rdb, __LINE__, "(validation)");
    err = true;
  }
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", i - 1);
    int vsiz;
    const char *vbuf = tcmapget(map, kbuf, ksiz, &vsiz);
    int rsiz;
    char *rbuf = tcrdbget(rdb, kbuf, ksiz, &rsiz);
    if(vbuf){
      putchar('.');
      if(!rbuf){
        eprint(rdb, __LINE__, "tcrdbget");
        err = true;
      } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
        eprint(rdb, __LINE__, "(validation)");
        err = true;
      }
    } else {
      putchar('*');
      if(rbuf){
        eprint(rdb, __LINE__, "(validation)");
        err = true;
      }
    }
    tcfree(rbuf);
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  if(rnum % 50 > 0) iprintf(" (%08d)\n", rnum);
  tcmapiterinit(map);
  int ksiz;
  const char *kbuf;
  for(int i = 1; (kbuf = tcmapiternext(map, &ksiz)) != NULL; i++){
    putchar('+');
    int vsiz;
    const char *vbuf = tcmapiterval(kbuf, &vsiz);
    int rsiz;
    char *rbuf = tcrdbget(rdb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(rdb, __LINE__, "tcrdbget");
      err = true;
    } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(rdb, __LINE__, "(validation)");
      err = true;
    }
    tcfree(rbuf);
    if(!tcrdbout(rdb, kbuf, ksiz)){
      eprint(rdb, __LINE__, "tcrdbout");
      err = true;
    }
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  int mrnum = tcmaprnum(map);
  if(mrnum % 50 > 0) iprintf(" (%08d)\n", mrnum);
  if(tcrdbrnum(rdb) != 0){
    eprint(rdb, __LINE__, "(validation)");
    err = true;
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  tcmapdel(map);
  for(int i = 0; i < cnum; i++){
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
static int proctable(const char *host, int port, int cnum, int tout, int rnum, int exp){
  iprintf("<Table Extension Test>\n  host=%s  port=%d  cnum=%d  tout=%d  rnum=%d  exp=%d\n\n",
          host, port, cnum, tout, rnum, exp);
  bool err = false;
  double stime = tctime();
  TCRDB *rdbs[cnum];
  for(int i = 0; i < cnum; i++){
    rdbs[i] = tcrdbnew();
    if(tout > 0) tcrdbtune(rdbs[i], tout, RDBTRECON);
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
  if(exp > 0){
    if(!tcrdbtblsetindex(rdb, "x", RDBITDECIMAL)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    for(int i = 1; i <= rnum && !err; i++){
      char pkbuf[RECBUFSIZ];
      int pksiz = sprintf(pkbuf, "%d", myrand(rnum) + 1);
      int act = myrand(100);
      if(act < 50){
        TCMAP *cols = tcmapnew2(7);
        char vbuf[RECBUFSIZ*5];
        int vsiz = sprintf(vbuf, "%08d", i);
        tcmapput(cols, "n", 1, vbuf, vsiz);
        vsiz = sprintf(vbuf, "%d", (int)tctime() + exp);
        tcmapput(cols, "x", 1, vbuf, vsiz);
        if(!tcrdbtblput(rdb, pkbuf, pksiz, cols)){
          eprint(rdb, __LINE__, "tcrdbtblput");
          err = true;
        }
        tcmapdel(cols);
      } else if(act == 60){
        if(!tcrdbtblout(rdb, pkbuf, pksiz) && tcrdbecode(rdb) != TTENOREC){
          eprint(rdb, __LINE__, "tcrdbtblout");
          err = true;
        }
      } else {
        TCMAP *cols = tcrdbtblget(rdb, pkbuf, pksiz);
        if(cols){
          tcmapdel(cols);
        } else if(tcrdbecode(rdb) != TTENOREC){
          eprint(rdb, __LINE__, "tcrdbtblget");
          err = true;
        }
      }
      if(rnum > 250 && i % (rnum / 250) == 0){
        iputchar('.');
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
        rdb = rdbs[myrand(rnum)%cnum];
      }
    }
  } else {
    if(!tcrdbtblsetindex(rdb, "", RDBITDECIMAL)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    if(!tcrdbtblsetindex(rdb, "str", RDBITLEXICAL)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    if(!tcrdbtblsetindex(rdb, "num", RDBITDECIMAL)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    if(!tcrdbtblsetindex(rdb, "type", RDBITDECIMAL)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    if(!tcrdbtblsetindex(rdb, "flag", RDBITTOKEN)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    if(!tcrdbtblsetindex(rdb, "text", RDBITQGRAM)){
      eprint(rdb, __LINE__, "tcrdbtblsetindex");
      err = true;
    }
    iprintf("writing:\n");
    for(int i = 1; i <= rnum && !err; i++){
      int id = (int)tcrdbtblgenuid(rdb);
      char pkbuf[RECBUFSIZ];
      int pksiz = sprintf(pkbuf, "%d", id);
      TCMAP *cols = tcmapnew2(7);
      char vbuf[RECBUFSIZ*5];
      int vsiz = sprintf(vbuf, "%d", id);
      tcmapput(cols, "str", 3, vbuf, vsiz);
      vsiz = sprintf(vbuf, "%d", myrand(i) + 1);
      tcmapput(cols, "num", 3, vbuf, vsiz);
      vsiz = sprintf(vbuf, "%d", myrand(32) + 1);
      tcmapput(cols, "type", 4, vbuf, vsiz);
      int num = myrand(5);
      int pt = 0;
      char *wp = vbuf;
      for(int j = 0; j < num; j++){
        pt += myrand(5) + 1;
        if(wp > vbuf) *(wp++) = ',';
        wp += sprintf(wp, "%d", pt);
      }
      *wp = '\0';
      if(*vbuf != '\0'){
        tcmapput(cols, "flag", 4, vbuf, wp - vbuf);
        tcmapput(cols, "text", 4, vbuf, wp - vbuf);
      }
      switch(myrand(4)){
        default:
          if(!tcrdbtblput(rdb, pkbuf, pksiz, cols)){
            eprint(rdb, __LINE__, "tcrdbtblput");
            err = true;
          }
          break;
        case 1:
          if(!tcrdbtblputkeep(rdb, pkbuf, pksiz, cols)){
            eprint(rdb, __LINE__, "tcrdbtblputkeep");
            err = true;
          }
          break;
        case 2:
          if(!tcrdbtblputcat(rdb, pkbuf, pksiz, cols)){
            eprint(rdb, __LINE__, "tcrdbtblput");
            err = true;
          }
          break;
      }
      tcmapdel(cols);
      if(rnum > 250 && i % (rnum / 250) == 0){
        iputchar('.');
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
        rdb = rdbs[myrand(rnum)%cnum];
      }
    }
    iprintf("removing:\n");
    for(int i = 1; i <= rnum && !err; i++){
      char pkbuf[RECBUFSIZ];
      int pksiz = sprintf(pkbuf, "%d", myrand(rnum) + 1);
      if(!tcrdbtblout(rdb, pkbuf, pksiz) && tcrdbecode(rdb) != TTENOREC){
        eprint(rdb, __LINE__, "tcrdbtblout");
        err = true;
      }
      if(rnum > 250 && i % (rnum / 250) == 0){
        iputchar('.');
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
        rdb = rdbs[myrand(rnum)%cnum];
      }
    }
    iprintf("reading:\n");
    for(int i = 1; i <= rnum && !err; i++){
      char pkbuf[RECBUFSIZ];
      int pksiz = sprintf(pkbuf, "%d", myrand(rnum) + 1);
      TCMAP *cols = tcrdbtblget(rdb, pkbuf, pksiz);
      if(cols){
        tcmapdel(cols);
      } else if(tcrdbecode(rdb) != TTENOREC){
        eprint(rdb, __LINE__, "tcrdbtblget");
        err = true;
      }
      if(rnum > 250 && i % (rnum / 250) == 0){
        iputchar('.');
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
        rdb = rdbs[myrand(rnum)%cnum];
      }
    }
    iprintf("searching:\n");
    const char *names[] = { "", "str", "num", "type", "flag", "text", "c1" };
    int ops[] = { RDBQCSTREQ, RDBQCSTRINC, RDBQCSTRBW, RDBQCSTREW, RDBQCSTRAND, RDBQCSTROR,
                  RDBQCSTROREQ, RDBQCSTRRX, RDBQCNUMEQ, RDBQCNUMGT, RDBQCNUMGE, RDBQCNUMLT,
                  RDBQCNUMLE, RDBQCNUMBT, RDBQCNUMOREQ };
    int ftsops[] = { RDBQCFTSPH, RDBQCFTSAND, RDBQCFTSOR, RDBQCFTSEX };
    int types[] = { RDBQOSTRASC, RDBQOSTRDESC, RDBQONUMASC, RDBQONUMDESC };
    for(int i = 1; i <= rnum && !err; i++){
      RDBQRY *qry = tcrdbqrynew(rdb);
      int condnum = myrand(4);
      if(condnum < 1 && myrand(5) != 0) condnum = 1;
      for(int j = 0; j < condnum; j++){
        const char *name = names[myrand(sizeof(names) / sizeof(*names))];
        int op = ops[myrand(sizeof(ops) / sizeof(*ops))];
        if(myrand(10) == 0) op = ftsops[myrand(sizeof(ftsops) / sizeof(*ftsops))];
        if(myrand(20) == 0) op |= RDBQCNEGATE;
        if(myrand(20) == 0) op |= RDBQCNOIDX;
        char expr[RECBUFSIZ*3];
        char *wp = expr;
        wp += sprintf(expr, "%d", myrand(i));
        if(myrand(10) == 0) wp += sprintf(wp, ",%d", myrand(i));
        if(myrand(10) == 0) wp += sprintf(wp, ",%d", myrand(i));
        tcrdbqryaddcond(qry, name, op, expr);
      }
      if(myrand(3) != 0){
        const char *name = names[myrand(sizeof(names) / sizeof(*names))];
        int type = types[myrand(sizeof(types) / sizeof(*types))];
        tcrdbqrysetorder(qry, name, type);
      }
      tcrdbqrysetlimit(qry, myrand(10), myrand(5) * 10);
      if(myrand(10) == 0){
        RDBQRY *qrys[4];
        int num = myrand(5);
        for(int j = 0; j < num; j++){
          qrys[j] = qry;
        }
        if(myrand(3) == 0){
          TCLIST *res = tcrdbparasearch(qrys, num);
          for(int j = 0; j < 3 && j < tclistnum(res); j++){
            TCMAP *cols = tcrdbqryrescols(res, j);
            if(!tcmapget2(cols, "")){
              eprint(rdb, __LINE__, "(validation)");
              err = true;
            }
            tcmapdel(cols);
          }
          tclistdel(res);
        } else {
          TCLIST *res = tcrdbmetasearch(qrys, num, RDBMSUNION + myrand(3));
          for(int j = 0; j < 3 && j < tclistnum(res); j++){
            int pksiz;
            const char *pkbuf = tclistval(res, j, &pksiz);
            TCMAP *cols = tcrdbtblget(rdb, pkbuf, pksiz);
            if(cols){
              tcmapdel(cols);
            } else {
              eprint(rdb, __LINE__, "tcrdbtblget");
              err = true;
            }
          }
          tclistdel(res);
        }
      } else {
        TCLIST *res = tcrdbqrysearch(qry);
        tclistdel(res);
      }
      tcrdbqrydel(qry);
      if(rnum > 250 && i % (rnum / 250) == 0){
        iputchar('.');
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
        rdb = rdbs[myrand(rnum)%cnum];
      }
    }
    RDBQRY *qry = tcrdbqrynew(rdb);
    tcrdbqryaddcond(qry, "", RDBQCSTRBW, "1");
    tcrdbqrysetorder(qry, "str", RDBQOSTRASC);
    tcrdbqrysetlimit(qry, 10, 0);
    TCLIST *res = tcrdbqrysearchget(qry);
    for(int i = 0; i < tclistnum(res); i++){
      TCMAP *cols = tcrdbqryrescols(res, i);
      if(!tcmapget2(cols, "")){
        eprint(rdbs[i], __LINE__, "(validation)");
        err = true;
        break;
      }
      tcmapdel(cols);
    }
    if(tclistnum(res) != tcrdbqrysearchcount(qry)){
      eprint(rdb, __LINE__, "(validation)");
      err = true;
    }
    tclistdel(res);
    tcrdbqrydel(qry);
    qry = tcrdbqrynew(rdb);
    res = tcrdbqrysearch(qry);
    if(tclistnum(res) != tcrdbrnum(rdb)){
      eprint(rdb, __LINE__, "(validation)");
      err = true;
    }
    tclistdel(res);
    tcrdbqrysearchout(qry);
    if(tcrdbrnum(rdb) != 0){
      eprint(rdb, __LINE__, "(validation)");
      err = true;
    }
    tcrdbqrydel(qry);
  }
  iprintf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
  iprintf("size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  for(int i = 0; i < cnum; i++){
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



// END OF FILE
