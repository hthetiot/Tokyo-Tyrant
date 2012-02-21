/*************************************************************************************************
 * The mock-up skeleton database library
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
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>



/*************************************************************************************************
 * API
 *************************************************************************************************/


bool initialize(ADBSKEL *skel){
  skel->opq = tcadbnew();
  skel->del = (void (*)(void *))tcadbdel;
  skel->open = (bool (*)(void *, const char *))tcadbopen;
  skel->close = (bool (*)(void *))tcadbclose;
  skel->put = (bool (*)(void *, const void *, int, const void *, int))tcadbput;
  skel->putkeep = (bool (*)(void *, const void *, int, const void *, int))tcadbputkeep;
  skel->putcat = (bool (*)(void *, const void *, int, const void *, int))tcadbputcat;
  skel->out = (bool (*)(void *, const void *, int))tcadbout;
  skel->get = (void *(*)(void *, const void *, int, int *))tcadbget;
  skel->vsiz = (int (*)(void *, const void *, int))tcadbvsiz;
  skel->iterinit = (bool (*)(void *))tcadbiterinit;
  skel->iternext = (void *(*)(void *, int *))tcadbiternext;
  skel->fwmkeys = (TCLIST *(*)(void *, const void *, int, int))tcadbfwmkeys;
  skel->addint = (int (*)(void *, const void *, int, int))tcadbaddint;
  skel->adddouble = (double (*)(void *, const void *, int, double))tcadbadddouble;
  skel->sync = (bool (*)(void *))tcadbsync;
  skel->optimize = (bool (*)(void *, const char *))tcadboptimize;
  skel->vanish = (bool (*)(void *))tcadbvanish;
  skel->copy = (bool (*)(void *, const char *))tcadbcopy;
  skel->tranbegin = (bool (*)(void *))tcadbtranbegin;
  skel->trancommit = (bool (*)(void *))tcadbtrancommit;
  skel->tranabort = (bool (*)(void *))tcadbtranabort;
  skel->path = (const char *(*)(void *))tcadbpath;
  skel->rnum = (uint64_t (*)(void *))tcadbrnum;
  skel->size = (uint64_t (*)(void *))tcadbsize;
  skel->misc = (TCLIST *(*)(void *, const char *, const TCLIST *))tcadbmisc;
  skel->putproc =
    (bool (*)(void *, const void *, int, const void *, int, TCPDPROC, void *))tcadbputproc;
  skel->foreach = (bool (*)(void *, TCITER, void *))tcadbforeach;
  return true;
}



// END OF FILE
