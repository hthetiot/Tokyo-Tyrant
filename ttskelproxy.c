/*************************************************************************************************
 * The skeleton database library working as a proxy
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


#include <tcadb.h>
#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>


/* private funciton prototypes */
static TCLIST *mymisc(TCRDB *rdb, const char *name, const TCLIST *args);



/*************************************************************************************************
 * API
 *************************************************************************************************/


bool initialize(ADBSKEL *skel){
  skel->opq = tcrdbnew();
  skel->del = (void (*)(void *))tcrdbdel;
  skel->open = (bool (*)(void *, const char *))tcrdbopen2;
  skel->close = (bool (*)(void *))tcrdbclose;
  skel->put = (bool (*)(void *, const void *, int, const void *, int))tcrdbput;
  skel->putkeep = (bool (*)(void *, const void *, int, const void *, int))tcrdbputkeep;
  skel->putcat = (bool (*)(void *, const void *, int, const void *, int))tcrdbputcat;
  skel->out = (bool (*)(void *, const void *, int))tcrdbout;
  skel->get = (void *(*)(void *, const void *, int, int *))tcrdbget;
  skel->vsiz = (int (*)(void *, const void *, int))tcrdbvsiz;
  skel->iterinit = (bool (*)(void *))tcrdbiterinit;
  skel->iternext = (void *(*)(void *, int *))tcrdbiternext;
  skel->fwmkeys = (TCLIST *(*)(void *, const void *, int, int))tcrdbfwmkeys;
  skel->addint = (int (*)(void *, const void *, int, int))tcrdbaddint;
  skel->adddouble = (double (*)(void *, const void *, int, double))tcrdbadddouble;
  skel->sync = (bool (*)(void *))tcrdbsync;
  skel->optimize = (bool (*)(void *, const char *))tcrdboptimize;
  skel->vanish = (bool (*)(void *))tcrdbvanish;
  skel->copy = (bool (*)(void *, const char *))tcrdbcopy;
  skel->path = (const char *(*)(void *))tcrdbexpr;
  skel->rnum = (uint64_t (*)(void *))tcrdbrnum;
  skel->size = (uint64_t (*)(void *))tcrdbsize;
  skel->misc = (TCLIST *(*)(void *, const char *, const TCLIST *))mymisc;
  return true;
}



/*************************************************************************************************
 * private features
 *************************************************************************************************/


static TCLIST *mymisc(TCRDB *rdb, const char *name, const TCLIST *args){
  return tcrdbmisc(rdb, name, 0, args);
}



// END OF FILE
