/*************************************************************************************************
 * The test cases of the update log API
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


#include <tculog.h>
#include "myconf.h"

#define RECBUFSIZ      32                // buffer for records

typedef struct {                         // type of structure for read thread
  TCULRD *ulrd;
  int id;
  int rnum;
} TARGREAD;


/* global variables */
const char *g_progname;                  // program name


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void iprintf(const char *format, ...);
static void eprint(TCULOG *ulog, const char *func);
static int runwrite(int argc, char **argv);
static int runread(int argc, char **argv);
static int runthread(int argc, char **argv);
static int procwrite(const char *base, int rnum, int64_t limsiz, bool as);
static int procread(const char *base, uint64_t ts, bool pm);
static int procthread(const char *base, int tnum, int rnum, int64_t limsiz, bool as);


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
  } else if(!strcmp(argv[1], "thread")){
    rv = runthread(argc, argv);
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
  fprintf(stderr, "  %s write [-lim num] [-as] base rnum\n", g_progname);
  fprintf(stderr, "  %s read [-ts num] [-pm] base\n", g_progname);
  fprintf(stderr, "  %s thread [-lim num] [-as] base tnum rnum\n", g_progname);
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
static void eprint(TCULOG *ulog, const char *func){
  fprintf(stderr, "%s: %s: error:\n", g_progname, func);
}


/* parse arguments of write command */
static int runwrite(int argc, char **argv){
  char *base = NULL;
  char *rstr = NULL;
  int64_t limsiz = 0;
  bool as = false;
  for(int i = 2; i < argc; i++){
    if(!base && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-lim")){
        if(++i >= argc) usage();
        limsiz = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-as")){
        as = true;
      } else {
        usage();
      }
    } else if(!base){
      base = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!base || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwrite(base, rnum, limsiz, as);
  return rv;
}


/* parse arguments of read command */
static int runread(int argc, char **argv){
  char *base = NULL;
  uint64_t ts = 0;
  bool pm = false;
  for(int i = 2; i < argc; i++){
    if(!base && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-ts")){
        if(++i >= argc) usage();
        ts = ttstrtots(argv[i]);
      } else if(!strcmp(argv[i], "-pm")){
        pm = true;
      } else {
        usage();
      }
    } else if(!base){
      base = argv[i];
    } else {
      usage();
    }
  }
  if(!base) usage();
  int rv = procread(base, ts, pm);
  return rv;
}


/* parse arguments of thread command */
static int runthread(int argc, char **argv){
  char *base = NULL;
  char *tstr = NULL;
  char *rstr = NULL;
  int64_t limsiz = 0;
  bool as = false;
  for(int i = 2; i < argc; i++){
    if(!base && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-lim")){
        if(++i >= argc) usage();
        limsiz = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-as")){
        as = true;
      } else {
        usage();
      }
    } else if(!base){
      base = argv[i];
    } else if(!tstr){
      tstr = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!base || !tstr || !rstr) usage();
  int tnum = tcatoi(tstr);
  int rnum = tcatoi(rstr);
  if(tnum < 1 || rnum < 1) usage();
  int rv = procthread(base, tnum, rnum, limsiz, as);
  return rv;
}


/* perform write command */
static int procwrite(const char *base, int rnum, int64_t limsiz, bool as){
  iprintf("<Writing Test>\n  base=%s  rnum=%d  limsiz=%lld  as=%d\n\n",
          base, rnum, (long long)limsiz, as);
  bool err = false;
  double stime = tctime();
  TCULOG *ulog = tculognew();
  if(as && !tculogsetaio(ulog)){
    eprint(ulog, "tculogsetaio");
    err = true;
  }
  if(!tculogopen(ulog, base, limsiz)){
    eprint(ulog, "tculogopen");
    err = true;
  }
  int sid = getpid() & UINT16_MAX;
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(!tculogwrite(ulog, 0, sid, sid, buf, len)){
      eprint(ulog, "tculogwrite");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      putchar('.');
      fflush(stdout);
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(!tculogclose(ulog)){
    eprint(ulog, "tculogclose");
    err = true;
  }
  tculogdel(ulog);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform read command */
static int procread(const char *base, uint64_t ts, bool pm){
  if(!pm)
    iprintf("<Reading Test>\n  base=%s  ts=%llu  pm=%d\n\n", base, (unsigned long long)ts, pm);
  bool err = false;
  double stime = tctime();
  TCULOG *ulog = tculognew();
  if(!tculogopen(ulog, base, 0)){
    eprint(ulog, "tculogopen");
    err = true;
  }
  TCULRD *ulrd = tculrdnew(ulog, ts);
  if(ulrd){
    const char *rbuf;
    int rsiz;
    uint64_t rts;
    uint32_t rsid, rmid;
    int i = 1;
    while((rbuf = tculrdread(ulrd, &rsiz, &rts, &rsid, &rmid)) != NULL){
      if(pm){
        printf("%llu\t%u:%u\t", (unsigned long long)rts, (unsigned int)rsid, (unsigned int)rmid);
        for(int i = 0; i < rsiz; i++){
          if(i > 0) putchar(' ');
          printf("%02X", ((unsigned char *)rbuf)[i]);
        }
        putchar('\n');
      } else {
        if(i % 1000 == 0){
          putchar('.');
          fflush(stdout);
          if(i % 25000 == 0) iprintf(" (%08d)\n", i);
        }
      }
      i++;
    }
    if(!pm) iprintf(" (%08d)\n", i - 1);
    tculrddel(ulrd);
  }
  if(!tculogclose(ulog)){
    eprint(ulog, "tculogclose");
    err = true;
  }
  tculogdel(ulog);
  if(!pm){
    iprintf("time: %.3f\n", tctime() - stime);
    iprintf("%s\n\n", err ? "error" : "ok");
  }
  return err ? 1 : 0;
}


/* thread the read function */
static void *threadread(void *targ){
  TCULRD *ulrd = ((TARGREAD *)targ)->ulrd;
  int id = ((TARGREAD *)targ)->id;
  int rnum = ((TARGREAD *)targ)->rnum;
  const char *rbuf;
  int rsiz;
  uint64_t rts;
  uint32_t rsid, rmid;
  int i = 1;
  while(i <= rnum){
    while((rbuf = tculrdread(ulrd, &rsiz, &rts, &rsid, &rmid)) != NULL){
      if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
        putchar('.');
        fflush(stdout);
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      }
      i++;
    }
    tcsleep(0.01);
  }
  return NULL;
}


/* perform thread command */
static int procthread(const char *base, int tnum, int rnum, int64_t limsiz, bool as){
  iprintf("<Threading Test>\n  base=%s  tnum=%d  rnum=%d  limsiz=%lld  as=%d\n\n",
          base, tnum, rnum, (long long)limsiz, as);
  bool err = false;
  double stime = tctime();
  TCULOG *ulog = tculognew();
  if(as && !tculogsetaio(ulog)){
    eprint(ulog, "tculogsetaio");
    err = true;
  }
  if(!tculogopen(ulog, base, limsiz)){
    eprint(ulog, "tculogopen");
    err = true;
  }
  TARGREAD targs[tnum];
  pthread_t threads[tnum];
  for(int i = 0; i < tnum; i++){
    targs[i].ulrd = tculrdnew(ulog, 0);
    targs[i].id = i;
    targs[i].rnum = rnum;
    if(!targs[i].ulrd){
      eprint(ulog, "tculrdnew");
      targs[i].id = -1;
      err = true;
    } else if(pthread_create(threads + i, NULL, threadread, targs + i) != 0){
      eprint(ulog, "pthread_create");
      targs[i].id = -1;
      err = true;
    }
  }
  int sid = getpid() & UINT16_MAX;
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(!tculogwrite(ulog, 0, sid, sid, buf, len)){
      eprint(ulog, "tculogwrite");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 10) == 0) tcsleep(0.1);
  }
  for(int i = 0; i < tnum; i++){
    if(targs[i].id == -1) continue;
    void *rv;
    if(pthread_join(threads[i], &rv) != 0){
      eprint(ulog, "pthread_join");
      err = true;
    } else if(rv){
      err = true;
    }
    tculrddel(targs[i].ulrd);
  }
  if(!tculogclose(ulog)){
    eprint(ulog, "tculogclose");
    err = true;
  }
  tculogdel(ulog);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
