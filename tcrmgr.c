/*************************************************************************************************
 * The command line utility of the remote database API
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
#include <tcrdb.h>
#include "myconf.h"

#define REQHEADMAX     32                // maximum number of request headers of HTTP
#define MINIBNUM       31                // bucket number of map for trivial use


/* global variables */
const char *g_progname;                  // program name


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void printerr(TCRDB *rdb);
static int sepstrtochr(const char *str);
static char *strtozsv(const char *str, int sep, int *sp);
static int printdata(const char *ptr, int size, bool px, int sep);
static char *mygetline(FILE *ifp);
static bool myopen(TCRDB *rdb, const char *host, int port);
static bool mysetmst(TCRDB *rdb, const char *host, int port, uint64_t ts, int opts);
static int runinform(int argc, char **argv);
static int runput(int argc, char **argv);
static int runout(int argc, char **argv);
static int runget(int argc, char **argv);
static int runmget(int argc, char **argv);
static int runlist(int argc, char **argv);
static int runext(int argc, char **argv);
static int runsync(int argc, char **argv);
static int runoptimize(int argc, char **argv);
static int runvanish(int argc, char **argv);
static int runcopy(int argc, char **argv);
static int runmisc(int argc, char **argv);
static int runimporttsv(int argc, char **argv);
static int runrestore(int argc, char **argv);
static int runsetmst(int argc, char **argv);
static int runrepl(int argc, char **argv);
static int runhttp(int argc, char **argv);
static int runversion(int argc, char **argv);
static int procinform(const char *host, int port, bool st);
static int procput(const char *host, int port, const char *kbuf, int ksiz,
                   const char *vbuf, int vsiz, int dmode, int shlw);
static int procout(const char *host, int port, const char *kbuf, int ksiz);
static int procget(const char *host, int port, const char *kbuf, int ksiz, int sep,
                   bool px, bool pz);
static int procmget(const char *host, int port, const TCLIST *keys, int sep, bool px);
static int proclist(const char *host, int port, int sep, int max, bool pv, bool px,
                    const char *fmstr);
static int procext(const char *host, int port, const char *func, int opts,
                   const char *kbuf, int ksiz, const char *vbuf, int vsiz, int sep,
                   bool px, bool pz);
static int procsync(const char *host, int port);
static int procoptimize(const char *host, int port, const char *params);
static int procvanish(const char *host, int port);
static int proccopy(const char *host, int port, const char *dpath);
static int procmisc(const char *host, int port, const char *func, int opts,
                    const TCLIST *args, int sep, bool px);
static int procimporttsv(const char *host, int port, const char *file, bool nr,
                         bool sc, int sep);
static int procrestore(const char *host, int port, const char *upath, uint64_t ts, int opts);
static int procsetmst(const char *host, int port, const char *mhost, int mport,
                      uint64_t ts, int opts);
static int procrepl(const char *host, int port, uint64_t ts, uint32_t sid, bool ph);
static int prochttp(const char *url, TCMAP *hmap, bool ih);
static int procversion(void);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  signal(SIGPIPE, SIG_IGN);
  if(argc < 2) usage();
  int rv = 0;
  if(!strcmp(argv[1], "inform")){
    rv = runinform(argc, argv);
  } else if(!strcmp(argv[1], "put")){
    rv = runput(argc, argv);
  } else if(!strcmp(argv[1], "out")){
    rv = runout(argc, argv);
  } else if(!strcmp(argv[1], "get")){
    rv = runget(argc, argv);
  } else if(!strcmp(argv[1], "mget")){
    rv = runmget(argc, argv);
  } else if(!strcmp(argv[1], "list")){
    rv = runlist(argc, argv);
  } else if(!strcmp(argv[1], "ext")){
    rv = runext(argc, argv);
  } else if(!strcmp(argv[1], "sync")){
    rv = runsync(argc, argv);
  } else if(!strcmp(argv[1], "optimize")){
    rv = runoptimize(argc, argv);
  } else if(!strcmp(argv[1], "vanish")){
    rv = runvanish(argc, argv);
  } else if(!strcmp(argv[1], "copy")){
    rv = runcopy(argc, argv);
  } else if(!strcmp(argv[1], "misc")){
    rv = runmisc(argc, argv);
  } else if(!strcmp(argv[1], "importtsv")){
    rv = runimporttsv(argc, argv);
  } else if(!strcmp(argv[1], "restore")){
    rv = runrestore(argc, argv);
  } else if(!strcmp(argv[1], "setmst")){
    rv = runsetmst(argc, argv);
  } else if(!strcmp(argv[1], "repl")){
    rv = runrepl(argc, argv);
  } else if(!strcmp(argv[1], "http")){
    rv = runhttp(argc, argv);
  } else if(!strcmp(argv[1], "version") || !strcmp(argv[1], "--version")){
    rv = runversion(argc, argv);
  } else {
    usage();
  }
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: the command line utility of the remote database API\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s inform [-port num] [-st] host\n", g_progname);
  fprintf(stderr, "  %s put [-port num] [-sx] [-sep chr] [-dk|-dc|-dai|-dad] [-ds num]"
          " host key value\n", g_progname);
  fprintf(stderr, "  %s out [-port num] [-sx] [-sep chr] host key\n", g_progname);
  fprintf(stderr, "  %s get [-port num] [-sx] [-sep chr] [-px] [-pz] host key\n", g_progname);
  fprintf(stderr, "  %s mget [-port num] [-sx] [-sep chr] [-px] host [key...]\n", g_progname);
  fprintf(stderr, "  %s list [-port num] [-sep chr] [-m num] [-pv] [-px] [-fm str] host\n",
          g_progname);
  fprintf(stderr, "  %s ext [-port num] [-xlr|-xlg] [-sx] [-sep chr] [-px] host func"
          " [key [value]]\n", g_progname);
  fprintf(stderr, "  %s sync [-port num] host\n", g_progname);
  fprintf(stderr, "  %s optimize [-port num] host [params]\n", g_progname);
  fprintf(stderr, "  %s vanish [-port num] host\n", g_progname);
  fprintf(stderr, "  %s copy [-port num] host dpath\n", g_progname);
  fprintf(stderr, "  %s misc [-port num] [-mnu] [-sx] [-sep chr] [-px] host func [arg...]\n",
          g_progname);
  fprintf(stderr, "  %s importtsv [-port num] [-nr] [-sc] [-sep chr] host [file]\n", g_progname);
  fprintf(stderr, "  %s restore [-port num] [-ts num] [-rcc] host upath\n", g_progname);
  fprintf(stderr, "  %s setmst [-port num] [-mport num] [-ts num] [-rcc] host [mhost]\n",
          g_progname);
  fprintf(stderr, "  %s repl [-port num] [-ts num] [-sid num] [-ph] host\n", g_progname);
  fprintf(stderr, "  %s http [-ah name value] [-ih] url\n", g_progname);
  fprintf(stderr, "  %s version\n", g_progname);
  fprintf(stderr, "\n");
  exit(1);
}


/* print error information */
static void printerr(TCRDB *rdb){
  int ecode = tcrdbecode(rdb);
  fprintf(stderr, "%s: error: %d: %s\n", g_progname, ecode, tcrdberrmsg(ecode));
}


/* get the character of separation string */
static int sepstrtochr(const char *str){
  if(!strcmp(str, "\\t")) return '\t';
  if(!strcmp(str, "\\r")) return '\r';
  if(!strcmp(str, "\\n")) return '\n';
  return *(unsigned char *)str;
}


/* encode a string as a zero separaterd string */
static char *strtozsv(const char *str, int sep, int *sp){
  int size = strlen(str);
  char *buf = tcmemdup(str, size);
  for(int i = 0; i < size; i++){
    if(buf[i] == sep) buf[i] = '\0';
  }
  *sp = size;
  return buf;
}


/* print record data */
static int printdata(const char *ptr, int size, bool px, int sep){
  int len = 0;
  while(size-- > 0){
    if(px){
      if(len > 0) putchar(' ');
      len += printf("%02X", *(unsigned char *)ptr);
    } else if(sep > 0){
      if(*ptr == '\0'){
        putchar(sep);
      } else {
        putchar(*ptr);
      }
      len++;
    } else {
      putchar(*ptr);
      len++;
    }
    ptr++;
  }
  return len;
}


/* read a line from a file descriptor */
static char *mygetline(FILE *ifp){
  int len = 0;
  int blen = 1024;
  char *buf = tcmalloc(blen);
  bool end = true;
  int c;
  while((c = fgetc(ifp)) != EOF){
    end = false;
    if(c == '\0') continue;
    if(blen <= len){
      blen *= 2;
      buf = tcrealloc(buf, blen + 1);
    }
    if(c == '\n' || c == '\r') c = '\0';
    buf[len++] = c;
    if(c == '\0') break;
  }
  if(end){
    tcfree(buf);
    return NULL;
  }
  buf[len] = '\0';
  return buf;
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


/* set the replication master */
static bool mysetmst(TCRDB *rdb, const char *host, int port, uint64_t ts, int opts){
  bool err = false;
  if(strchr(host, ':')){
    if(!tcrdbsetmst2(rdb, host, ts, opts)) err = true;
  } else {
    if(!tcrdbsetmst(rdb, host, port, ts, opts)) err = true;
  }
  return !err;
}


/* parse arguments of inform command */
static int runinform(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  bool st = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-st")){
        st = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procinform(host, port, st);
  return rv;
}


/* parse arguments of put command */
static int runput(int argc, char **argv){
  char *host = NULL;
  char *key = NULL;
  char *value = NULL;
  int port = TTDEFPORT;
  int dmode = 0;
  int shlw = -1;
  bool sx = false;
  int sep = -1;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-dk")){
        dmode = -1;
      } else if(!strcmp(argv[i], "-dc")){
        dmode = 1;
      } else if(!strcmp(argv[i], "-dai")){
        dmode = 10;
      } else if(!strcmp(argv[i], "-dad")){
        dmode = 11;
      } else if(!strcmp(argv[i], "-ds")){
        if(++i >= argc) usage();
        shlw = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-sx")){
        sx = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!key){
      key = argv[i];
    } else if(!value){
      value = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !key || !value) usage();
  int ksiz, vsiz;
  char *kbuf, *vbuf;
  if(sx){
    kbuf = tchexdecode(key, &ksiz);
    vbuf = tchexdecode(value, &vsiz);
  } else if(sep > 0){
    kbuf = strtozsv(key, sep, &ksiz);
    vbuf = strtozsv(value, sep, &vsiz);
  } else {
    ksiz = strlen(key);
    kbuf = tcmemdup(key, ksiz);
    vsiz = strlen(value);
    vbuf = tcmemdup(value, vsiz);
  }
  int rv = procput(host, port, kbuf, ksiz, vbuf, vsiz, dmode, shlw);
  tcfree(vbuf);
  tcfree(kbuf);
  return rv;
}


/* parse arguments of out command */
static int runout(int argc, char **argv){
  char *host = NULL;
  char *key = NULL;
  int port = TTDEFPORT;
  bool sx = false;
  int sep = -1;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-sx")){
        sx = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!key){
      key = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !key) usage();
  int ksiz;
  char *kbuf;
  if(sx){
    kbuf = tchexdecode(key, &ksiz);
  } else if(sep > 0){
    kbuf = strtozsv(key, sep, &ksiz);
  } else {
    ksiz = strlen(key);
    kbuf = tcmemdup(key, ksiz);
  }
  int rv = procout(host, port, kbuf, ksiz);
  tcfree(kbuf);
  return rv;
}


/* parse arguments of get command */
static int runget(int argc, char **argv){
  char *host = NULL;
  char *key = NULL;
  int port = TTDEFPORT;
  bool sx = false;
  int sep = -1;
  bool px = false;
  bool pz = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-sx")){
        sx = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else if(!strcmp(argv[i], "-px")){
        px = true;
      } else if(!strcmp(argv[i], "-pz")){
        pz = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!key){
      key = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !key) usage();
  int ksiz;
  char *kbuf;
  if(sx){
    kbuf = tchexdecode(key, &ksiz);
  } else if(sep > 0){
    kbuf = strtozsv(key, sep, &ksiz);
  } else {
    ksiz = strlen(key);
    kbuf = tcmemdup(key, ksiz);
  }
  int rv = procget(host, port, kbuf, ksiz, sep, px, pz);
  tcfree(kbuf);
  return rv;
}


/* parse arguments of mget command */
static int runmget(int argc, char **argv){
  char *host = NULL;
  TCLIST *keys = tcmpoollistnew(tcmpoolglobal());
  int port = TTDEFPORT;
  bool sx = false;
  int sep = -1;
  bool px = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-sx")){
        sx = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else if(!strcmp(argv[i], "-px")){
        px = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      tclistpush2(keys, argv[i]);
    }
  }
  if(!host) usage();
  if(sx){
    for(int i = 0; i < tclistnum(keys); i++){
      int ksiz;
      char *kbuf = tchexdecode(tclistval2(keys, i), &ksiz);
      tclistover(keys, i, kbuf, ksiz);
      tcfree(kbuf);
    }
  } else if(sep > 0){
    for(int i = 0; i < tclistnum(keys); i++){
      int ksiz;
      char *kbuf = strtozsv(tclistval2(keys, i), sep, &ksiz);
      tclistover(keys, i, kbuf, ksiz);
      tcfree(kbuf);
    }
  }
  int rv = procmget(host, port, keys, sep, px);
  return rv;
}


/* parse arguments of list command */
static int runlist(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  int sep = -1;
  int max = -1;
  bool pv = false;
  bool px = false;
  char *fmstr = NULL;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else if(!strcmp(argv[i], "-m")){
        if(++i >= argc) usage();
        max = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-pv")){
        pv = true;
      } else if(!strcmp(argv[i], "-px")){
        px = true;
      } else if(!strcmp(argv[i], "-fm")){
        if(++i >= argc) usage();
        fmstr = argv[i];
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = proclist(host, port, sep, max, pv, px, fmstr);
  return rv;
}


/* parse arguments of ext command */
static int runext(int argc, char **argv){
  char *host = NULL;
  char *func = NULL;
  char *key = NULL;
  char *value = NULL;
  int port = TTDEFPORT;
  int opts = 0;
  bool sx = false;
  int sep = -1;
  bool px = false;
  bool pz = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-xlr")){
        opts |= RDBXOLCKREC;
      } else if(!strcmp(argv[i], "-xlg")){
        opts |= RDBXOLCKGLB;
      } else if(!strcmp(argv[i], "-sx")){
        sx = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else if(!strcmp(argv[i], "-px")){
        px = true;
      } else if(!strcmp(argv[i], "-pz")){
        pz = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!func){
      func = argv[i];
    } else if(!key){
      key = argv[i];
    } else if(!value){
      value = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !func) usage();
  if(!key) key = "";
  if(!value) value = "";
  int ksiz, vsiz;
  char *kbuf, *vbuf;
  if(sx){
    kbuf = tchexdecode(key, &ksiz);
    vbuf = tchexdecode(value, &vsiz);
  } else if(sep > 0){
    kbuf = strtozsv(key, sep, &ksiz);
    vbuf = strtozsv(value, sep, &vsiz);
  } else {
    ksiz = strlen(key);
    kbuf = tcmemdup(key, ksiz);
    vsiz = strlen(value);
    vbuf = tcmemdup(value, vsiz);
  }
  int rv = procext(host, port, func, opts, kbuf, ksiz, vbuf, vsiz, sep, px, pz);
  tcfree(vbuf);
  tcfree(kbuf);
  return rv;
}


/* parse arguments of sync command */
static int runsync(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procsync(host, port);
  return rv;
}


/* parse arguments of optimize command */
static int runoptimize(int argc, char **argv){
  char *host = NULL;
  char *params = NULL;
  int port = TTDEFPORT;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!params){
      params = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procoptimize(host, port, params);
  return rv;
}


/* parse arguments of vanish command */
static int runvanish(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procvanish(host, port);
  return rv;
}


/* parse arguments of copy command */
static int runcopy(int argc, char **argv){
  char *host = NULL;
  char *dpath = NULL;
  int port = TTDEFPORT;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!dpath){
      dpath = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !dpath) usage();
  int rv = proccopy(host, port, dpath);
  return rv;
}


/* parse arguments of misc command */
static int runmisc(int argc, char **argv){
  char *host = NULL;
  char *func = NULL;
  TCLIST *args = tcmpoollistnew(tcmpoolglobal());
  int port = TTDEFPORT;
  int opts = 0;
  bool sx = false;
  int sep = -1;
  bool px = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-mnu")){
        opts |= RDBMONOULOG;
      } else if(!strcmp(argv[i], "-sx")){
        sx = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else if(!strcmp(argv[i], "-px")){
        px = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!func){
      func = argv[i];
    } else {
      if(sx){
        int size;
        char *buf = tchexdecode(argv[i], &size);
        tclistpush(args, buf, size);
        tcfree(buf);
      } else if(sep > 0){
        int size;
        char *buf = strtozsv(argv[i], sep, &size);
        tclistpush(args, buf, size);
        tcfree(buf);
      } else {
        tclistpush2(args, argv[i]);
      }
    }
  }
  if(!host || !func) usage();
  int rv = procmisc(host, port, func, opts, args, sep, px);
  return rv;
}


/* parse arguments of importtsv command */
static int runimporttsv(int argc, char **argv){
  char *host = NULL;
  char *file = NULL;
  int port = TTDEFPORT;
  bool nr = false;
  bool sc = false;
  int sep = -1;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nr")){
        nr = true;
      } else if(!strcmp(argv[i], "-sc")){
        sc = true;
      } else if(!strcmp(argv[i], "-sep")){
        if(++i >= argc) usage();
        sep = sepstrtochr(argv[i]);
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!file){
      file = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procimporttsv(host, port, file, nr, sc, sep);
  return rv;
}


/* parse arguments of restore command */
static int runrestore(int argc, char **argv){
  char *host = NULL;
  char *upath = NULL;
  int port = TTDEFPORT;
  uint64_t ts = 0;
  int opts = 0;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-ts")){
        if(++i >= argc) usage();
        ts = ttstrtots(argv[i]);
      } else if(!strcmp(argv[i], "-rcc")){
        opts |= RDBROCHKCON;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!upath){
      upath = argv[i];
    } else {
      usage();
    }
  }
  if(!host || !upath) usage();
  int rv = procrestore(host, port, upath, ts, opts);
  return rv;
}


/* parse arguments of setmst command */
static int runsetmst(int argc, char **argv){
  char *host = NULL;
  char *mhost = NULL;
  int port = TTDEFPORT;
  int mport = TTDEFPORT;
  uint64_t ts = 0;
  int opts = 0;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-mport")){
        if(++i >= argc) usage();
        mport = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-ts")){
        if(++i >= argc) usage();
        ts = ttstrtots(argv[i]);
      } else if(!strcmp(argv[i], "-rcc")){
        opts |= RDBROCHKCON;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else if(!mhost){
      mhost = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procsetmst(host, port, mhost, mport, ts, opts);
  return rv;
}


/* parse arguments of repl command */
static int runrepl(int argc, char **argv){
  char *host = NULL;
  int port = TTDEFPORT;
  uint64_t ts = 0;
  uint32_t sid = 0;
  bool ph = false;
  for(int i = 2; i < argc; i++){
    if(!host && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-port")){
        if(++i >= argc) usage();
        port = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-ts")){
        if(++i >= argc) usage();
        ts = ttstrtots(argv[i]);
      } else if(!strcmp(argv[i], "-sid")){
        if(++i >= argc) usage();
        sid = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-ph")){
        ph = true;
      } else {
        usage();
      }
    } else if(!host){
      host = argv[i];
    } else {
      usage();
    }
  }
  if(!host) usage();
  int rv = procrepl(host, port, ts, sid, ph);
  return rv;
}


/* parse arguments of http command */
static int runhttp(int argc, char **argv){
  char *url = NULL;
  struct {
    char *name;
    char *value;
  } heads[REQHEADMAX];
  int hnum = 0;
  bool ih = false;
  for(int i = 2; i < argc; i++){
    if(!url && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-ah")){
        if(++i >= argc) usage();
        char *name = argv[i];
        if(++i >= argc) usage();
        char *value = argv[i];
        if(hnum < REQHEADMAX){
          heads[hnum].name = name;
          heads[hnum].value = value;
          hnum++;
        }
      } else if(!strcmp(argv[i], "-ih")){
        ih = true;
      } else {
        usage();
      }
    } else if(!url){
      url = argv[i];
    } else {
      usage();
    }
  }
  if(!url) usage();
  TCMAP *hmap = tcmapnew2(hnum + 1);
  for(int i = 0; i < hnum; i++){
    tcmapput2(hmap, heads[i].name, heads[i].value);
  }
  int rv = prochttp(url, hmap, ih);
  tcmapdel(hmap);
  return rv;
}


/* parse arguments of version command */
static int runversion(int argc, char **argv){
  int rv = procversion();
  return rv;
}


/* perform inform command */
static int procinform(const char *host, int port, bool st){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(st){
    char *status = tcrdbstat(rdb);
    if(status){
      printf("%s", status);
      tcfree(status);
    } else {
      printerr(rdb);
      err = true;
    }
  } else {
    printf("record number: %llu\n", (unsigned long long)tcrdbrnum(rdb));
    printf("file size: %llu\n", (unsigned long long)tcrdbsize(rdb));
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform put command */
static int procput(const char *host, int port, const char *kbuf, int ksiz,
                   const char *vbuf, int vsiz, int dmode, int shlw){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  int inum;
  double dnum;
  switch(dmode){
    case -1:
      if(!tcrdbputkeep(rdb, kbuf, ksiz, vbuf, vsiz)){
        printerr(rdb);
        err = true;
      }
      break;
    case 1:
      if(!tcrdbputcat(rdb, kbuf, ksiz, vbuf, vsiz)){
        printerr(rdb);
        err = true;
      }
      break;
    case 10:
      inum = tcrdbaddint(rdb, kbuf, ksiz, tcatoi(vbuf));
      if(inum == INT_MIN){
        printerr(rdb);
        err = true;
      } else {
        printf("%d\n", inum);
      }
      break;
    case 11:
      dnum = tcrdbadddouble(rdb, kbuf, ksiz, tcatof(vbuf));
      if(isnan(dnum)){
        printerr(rdb);
        err = true;
      } else {
        printf("%.6f\n", dnum);
      }
      break;
    default:
      if(shlw >= 0){
        if(!tcrdbputshl(rdb, kbuf, ksiz, vbuf, vsiz, shlw)){
          printerr(rdb);
          err = true;
        }
      } else {
        if(!tcrdbput(rdb, kbuf, ksiz, vbuf, vsiz)){
          printerr(rdb);
          err = true;
        }
      }
      break;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform out command */
static int procout(const char *host, int port, const char *kbuf, int ksiz){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!tcrdbout(rdb, kbuf, ksiz)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform get command */
static int procget(const char *host, int port, const char *kbuf, int ksiz, int sep,
                   bool px, bool pz){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  int vsiz;
  char *vbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz);
  if(vbuf){
    printdata(vbuf, vsiz, px, sep);
    if(!pz) putchar('\n');
    tcfree(vbuf);
  } else {
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform mget command */
static int procmget(const char *host, int port, const TCLIST *keys, int sep, bool px){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  TCMAP *recs = tcmapnew();
  for(int i = 0; i < tclistnum(keys); i++){
    int ksiz;
    const char *kbuf = tclistval(keys, i, &ksiz);
    tcmapput(recs, kbuf, ksiz, "", 0);
  }
  if(tcrdbget3(rdb, recs)){
    tcmapiterinit(recs);
    const char *kbuf;
    int ksiz;
    while((kbuf = tcmapiternext(recs, &ksiz)) != NULL){
      int vsiz;
      const char *vbuf = tcmapiterval(kbuf, &vsiz);
      printdata(kbuf, ksiz, px, sep);
      putchar('\t');
      printdata(vbuf, vsiz, px, sep);
      putchar('\n');
    }
  } else {
    printerr(rdb);
    err = true;
  }
  tcmapdel(recs);
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform list command */
static int proclist(const char *host, int port, int sep, int max, bool pv, bool px,
                    const char *fmstr){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(fmstr){
    TCLIST *keys = tcrdbfwmkeys2(rdb, fmstr, max);
    for(int i = 0; i < tclistnum(keys); i++){
      int ksiz;
      const char *kbuf = tclistval(keys, i, &ksiz);
      printdata(kbuf, ksiz, px, sep);
      if(pv){
        int vsiz;
        char *vbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz);
        if(vbuf){
          putchar('\t');
          printdata(vbuf, vsiz, px, sep);
          tcfree(vbuf);
        }
      }
      putchar('\n');
    }
    tclistdel(keys);
  } else {
    if(!tcrdbiterinit(rdb)){
      printerr(rdb);
      err = true;
    }
    int ksiz;
    char *kbuf;
    int cnt = 0;
    while((kbuf = tcrdbiternext(rdb, &ksiz)) != NULL){
      printdata(kbuf, ksiz, px, sep);
      if(pv){
        int vsiz;
        char *vbuf = tcrdbget(rdb, kbuf, ksiz, &vsiz);
        if(vbuf){
          putchar('\t');
          printdata(vbuf, vsiz, px, sep);
          tcfree(vbuf);
        }
      }
      putchar('\n');
      tcfree(kbuf);
      if(max >= 0 && ++cnt >= max) break;
    }
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform ext command */
static int procext(const char *host, int port, const char *name, int opts,
                   const char *kbuf, int ksiz, const char *vbuf, int vsiz, int sep,
                   bool px, bool pz){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  int xsiz;
  char *xbuf = tcrdbext(rdb, name, opts, kbuf, ksiz, vbuf, vsiz, &xsiz);
  if(xbuf){
    printdata(xbuf, xsiz, px, sep);
    if(!pz) putchar('\n');
    tcfree(xbuf);
  } else {
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform sync command */
static int procsync(const char *host, int port){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!tcrdbsync(rdb)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform optimize command */
static int procoptimize(const char *host, int port, const char *param){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!tcrdboptimize(rdb, param)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform vanish command */
static int procvanish(const char *host, int port){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!tcrdbvanish(rdb)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform copy command */
static int proccopy(const char *host, int port, const char *dpath){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!tcrdbcopy(rdb, dpath)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform misc command */
static int procmisc(const char *host, int port, const char *func, int opts,
                    const TCLIST *args, int sep, bool px){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  TCLIST *res = tcrdbmisc(rdb, func, opts, args);
  if(res){
    for(int i = 0; i < tclistnum(res); i++){
      int rsiz;
      const char *rbuf = tclistval(res, i, &rsiz);
      printdata(rbuf, rsiz, px, sep);
      printf("\n");
    }
    tclistdel(res);
  } else {
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform importtsv command */
static int procimporttsv(const char *host, int port, const char *file, bool nr,
                         bool sc, int sep){
  FILE *ifp = file ? fopen(file, "rb") : stdin;
  if(!ifp){
    fprintf(stderr, "%s: could not open\n", file ? file : "(stdin)");
    return 1;
  }
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    if(ifp != stdin) fclose(ifp);
    return 1;
  }
  bool err = false;
  char *line;
  int cnt = 0;
  while(!err && (line = mygetline(ifp)) != NULL){
    char *pv = strchr(line, '\t');
    if(!pv){
      tcfree(line);
      continue;
    }
    *pv = '\0';
    if(sc) tcstrtolower(line);
    char *vbuf;
    int vsiz;
    if(sep > 0){
      vbuf = strtozsv(pv + 1, sep, &vsiz);
    } else {
      vsiz = strlen(pv + 1);
      vbuf = tcmemdup(pv + 1, vsiz);
    }
    if(nr){
      if(!tcrdbputnr(rdb, line, pv - line, vbuf, vsiz)){
        printerr(rdb);
        err = true;
      }
    } else {
      if(!tcrdbput(rdb, line, pv - line, vbuf, vsiz)){
        printerr(rdb);
        err = true;
      }
    }
    tcfree(vbuf);
    tcfree(line);
    if(cnt > 0 && cnt % 100 == 0){
      putchar('.');
      fflush(stdout);
      if(cnt % 5000 == 0) printf(" (%08d)\n", cnt);
    }
    cnt++;
  }
  printf(" (%08d)\n", cnt);
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  if(ifp != stdin) fclose(ifp);
  return err ? 1 : 0;
}


/* perform restore command */
static int procrestore(const char *host, int port, const char *upath, uint64_t ts, int opts){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!tcrdbrestore(rdb, upath, ts, opts)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform setmst command */
static int procsetmst(const char *host, int port, const char *mhost, int mport,
                      uint64_t ts, int opts){
  TCRDB *rdb = tcrdbnew();
  if(!myopen(rdb, host, port)){
    printerr(rdb);
    tcrdbdel(rdb);
    return 1;
  }
  bool err = false;
  if(!mysetmst(rdb, mhost, mport, ts, opts)){
    printerr(rdb);
    err = true;
  }
  if(!tcrdbclose(rdb)){
    if(!err) printerr(rdb);
    err = true;
  }
  tcrdbdel(rdb);
  return err ? 1 : 0;
}


/* perform repl command */
static int procrepl(const char *host, int port, uint64_t ts, uint32_t sid, bool ph){
  bool err = false;
  TCREPL *repl = tcreplnew();
  if(tcreplopen(repl, host, port, ts, sid)){
    const char *rbuf;
    int rsiz;
    uint64_t rts;
    uint32_t rsid;
    char stack[TTIOBUFSIZ];
    while((rbuf = tcreplread(repl, &rsiz, &rts, &rsid)) != NULL){
      if(rsiz < 1) continue;
      if(ph){
        printf("%llu\t%u:%u\t",
               (unsigned long long)rts, (unsigned int)rsid, (unsigned int)repl->mid);
        if(rsiz >= 2){
          printf("%s\t", ttcmdidtostr(((unsigned char *)rbuf)[1]));
          printdata(rbuf, rsiz, true, -1);
          putchar('\n');
        } else {
          printf("[broken entry]\n");
        }
        fflush(stdout);
      } else {
        int msiz = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) * 2 + rsiz;
        char *mbuf = (msiz < TTIOBUFSIZ) ? stack : tcmalloc(msiz);
        unsigned char *wp = (unsigned char *)mbuf;
        *(wp++) = TCULMAGICNUM;
        uint64_t llnum = TTHTONLL(rts);
        memcpy(wp, &llnum, sizeof(llnum));
        wp += sizeof(llnum);
        uint16_t snum = TTHTONS(rsid);
        memcpy(wp, &snum, sizeof(snum));
        wp += sizeof(snum);
        snum = TTHTONS(repl->mid);
        memcpy(wp, &snum, sizeof(snum));
        wp += sizeof(snum);
        uint32_t lnum = TTHTONL(rsiz);
        memcpy(wp, &lnum, sizeof(lnum));
        wp += sizeof(lnum);
        memcpy(wp, rbuf, rsiz);
        fwrite(mbuf, 1, msiz, stdout);
        fflush(stdout);
        if(mbuf != stack) tcfree(mbuf);
      }
    }
    tcreplclose(repl);
  } else {
    fprintf(stderr, "%s: %s:%d could not be connected\n", g_progname, host, port);
    err = true;
  }
  tcrepldel(repl);
  return err ? 1 : 0;
}


/* perform http command */
static int prochttp(const char *url, TCMAP *hmap, bool ih){
  bool err = false;
  TCMAP *resheads = ih ? tcmapnew2(MINIBNUM) : NULL;
  TCXSTR *body = tcxstrnew();
  int code = tthttpfetch(url, hmap, resheads, body);
  if(code > 0){
    if(resheads){
      printf("%s\n", tcmapget2(resheads, "STATUS"));
      tcmapiterinit(resheads);
      const char *name;
      while((name = tcmapiternext2(resheads)) != NULL){
        if(*name >= 'A' && *name <= 'Z') continue;
        char *cap = tcstrdup(name);
        tcstrtolower(cap);
        char *wp = cap;
        bool head = true;
        while(*wp != '\0'){
          if(head && *wp >= 'a' && *wp <= 'z') *wp -= 'a' - 'A';
          head = *wp == '-' || *wp == ' ';
          wp++;
        }
        printf("%s: %s\n", cap, tcmapiterval2(name));
        tcfree(cap);
      }
      printf("\n");
    }
    fwrite(tcxstrptr(body), 1, tcxstrsize(body), stdout);
  } else {
    fprintf(stderr, "%s: %s: could not be connected\n", g_progname, url);
    err = true;
  }
  tcxstrdel(body);
  if(resheads) tcmapdel(resheads);
  return err ? 1 : 0;
}


/* perform version command */
static int procversion(void){
  printf("Tokyo Tyrant version %s (%d:%s) for %s\n",
         ttversion, _TT_LIBVER, _TT_PROTVER, TTSYSNAME);
  printf("Copyright (C) 2007-2010 Mikio Hirabayashi\n");
  return 0;
}



// END OF FILE
