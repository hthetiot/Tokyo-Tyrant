/*************************************************************************************************
 * The no-operation skeleton database library
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


/* private funciton prototypes */
static bool myopen(void *opq, const char *name);
static bool myclose(void *opq);
static bool myput(void *opq, const void *kbuf, int ksiz, const void *vbuf, int vsiz);
static bool myout(void *opq, const void *kbuf, int ksiz);
static TCLIST *mymisc(void *opq, const char *name, const TCLIST *args);



/*************************************************************************************************
 * API
 *************************************************************************************************/


bool initialize(ADBSKEL *skel){
  skel->open = myopen;
  skel->close = myclose;
  skel->put = myput;
  skel->out = myout;
  skel->misc = mymisc;
  return true;
}



/*************************************************************************************************
 * private features
 *************************************************************************************************/


static bool myopen(void *opq, const char *name){
  return true;
}


static bool myclose(void *opq){
  return true;
}


static bool myput(void *opq, const void *kbuf, int ksiz, const void *vbuf, int vsiz){
  return true;
}


static bool myout(void *opq, const void *kbuf, int ksiz){
  return true;
}


static TCLIST *mymisc(void *opq, const char *name, const TCLIST *args){
  return tclistnew2(1);
}



// END OF FILE
