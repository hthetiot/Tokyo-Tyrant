/*************************************************************************************************
 * The skeleton database library working as a directory database
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


#include <tcutil.h>
#include <tcadb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>


typedef struct {                         /* type of structure for a directory database */
  char *name;                            /* name of the directory */
} TCDDB;


/* private funciton prototypes */
static TCDDB *tcddbnew(void);
static void tcddbdel(TCDDB *ddb);
static bool tcddbopen(TCDDB *ddb, const char *name);
static bool tcddbclose(TCDDB *ddb);
static bool tcddbput(TCDDB *ddb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);
static bool tcddbout(TCDDB *ddb, const void *kbuf, int ksiz);
static void *tcddbget(TCDDB *ddb, const void *kbuf, int ksiz, int *sp);
static char *makepath(TCDDB *ddb, const void *kbuf, int ksiz);



/*************************************************************************************************
 * API
 *************************************************************************************************/


bool initialize(ADBSKEL *skel){
  skel->opq = tcddbnew();
  skel->del = (void (*)(void *))tcddbdel;
  skel->open = (bool (*)(void *, const char *))tcddbopen;
  skel->close = (bool (*)(void *))tcddbclose;
  skel->put = (bool (*)(void *, const void *, int, const void *, int))tcddbput;
  skel->out = (bool (*)(void *, const void *, int))tcddbout;
  skel->get = (void *(*)(void *, const void *, int, int *))tcddbget;
  return true;
}



/*************************************************************************************************
 * private features
 *************************************************************************************************/


static TCDDB *tcddbnew(void){
  TCDDB *ddb = tcmalloc(sizeof(*ddb));
  ddb->name = NULL;
  return ddb;
}


static void tcddbdel(TCDDB *ddb){
  if(ddb->name) tcddbclose(ddb);
  tcfree(ddb);
}


static bool tcddbopen(TCDDB *ddb, const char *name){
  if(ddb->name) return false;
  struct stat sbuf;
  if(stat(name, &sbuf) == 0){
    if(!S_ISDIR(sbuf.st_mode)) return false;
  } else {
    if(mkdir(name, 0755) != 0) return false;
  }
  ddb->name = tcstrdup(name);
  return true;
}


static bool tcddbclose(TCDDB *ddb){
  if(!ddb->name) return false;
  tcfree(ddb->name);
  ddb->name = NULL;
  return true;
}


static bool tcddbput(TCDDB *ddb, const void *kbuf, int ksiz, const void *vbuf, int vsiz){
  if(!ddb->name) return false;
  bool err = false;
  char *path = makepath(ddb, kbuf, ksiz);
  if(!tcwritefile(path, vbuf, vsiz)) err = true;
  tcfree(path);
  return !err;
}


static bool tcddbout(TCDDB *ddb, const void *kbuf, int ksiz){
  if(!ddb->name) return false;
  bool err = false;
  char *path = makepath(ddb, kbuf, ksiz);
  if(unlink(path) != 0) err = true;
  tcfree(path);
  return !err;
}


static void *tcddbget(TCDDB *ddb, const void *kbuf, int ksiz, int *sp){
  if(!ddb->name) return false;
  void *vbuf;
  char *path = makepath(ddb, kbuf, ksiz);
  vbuf = tcreadfile(path, -1, sp);
  tcfree(path);
  return vbuf;
}


static char *makepath(TCDDB *ddb, const void *kbuf, int ksiz){
  char *uenc = tcurlencode(kbuf, ksiz);
  char *path = tcsprintf("%s/%s", ddb->name, uenc);
  tcfree(uenc);
  return path;
}



// END OF FILE
