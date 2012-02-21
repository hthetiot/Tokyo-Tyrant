/*************************************************************************************************
 * The update log API of Tokyo Tyrant
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


#ifndef _TCULOG_H                        /* duplication check */
#define _TCULOG_H

#if defined(__cplusplus)
#define __TCULOG_CLINKAGEBEGIN extern "C" {
#define __TCULOG_CLINKAGEEND }
#else
#define __TCULOG_CLINKAGEBEGIN
#define __TCULOG_CLINKAGEEND
#endif
__TCULOG_CLINKAGEBEGIN


#include <ttutil.h>



/*************************************************************************************************
 * API
 *************************************************************************************************/


#define TCULSUFFIX     ".ulog"           /* suffix of update log files */
#define TCULMAGICNUM   0xc9              /* magic number of each command */
#define TCULMAGICNOP   0xca              /* magic number of NOP command */
#define TCULRMTXNUM    31                /* number of mutexes of records */

typedef struct {                         /* type of structure for an update log */
  pthread_mutex_t rmtxs[TCULRMTXNUM];    /* mutex for records */
  pthread_rwlock_t rwlck;                /* mutex for operation */
  pthread_cond_t cnd;                    /* condition variable */
  pthread_mutex_t wmtx;                  /* mutex for waiting condition */
  char *base;                            /* path of the base directory */
  uint64_t limsiz;                       /* limit size */
  int max;                               /* number of maximum ID */
  int fd;                                /* current file descriptor */
  uint64_t size;                         /* current size */
  void *aiocbs;                          /* AIO tasks */
  int aiocbi;                            /* index of AIO tasks */
  uint64_t aioend;                       /* end offset of AIO tasks */
} TCULOG;

typedef struct {                         /* type of structure for a log reader */
  TCULOG *ulog;                          /* update log object */
  uint64_t ts;                           /* beginning timestamp */
  int num;                               /* number of current ID */
  int fd;                                /* current file descriptor */
  char *rbuf;                            /* record buffer */
  int rsiz;                              /* size of the record buffer */
} TCULRD;

typedef struct {                         /* type of structure for a replication */
  int fd;                                /* file descriptor */
  TTSOCK *sock;                          /* socket object */
  char *rbuf;                            /* record buffer */
  int rsiz;                              /* size of the record buffer */
  uint16_t mid;                          /* master server ID number */
} TCREPL;


/* Create an update log object.
   The return value is the new update log object. */
TCULOG *tculognew(void);


/* Delete an update log object.
   `ulog' specifies the update log object. */
void tculogdel(TCULOG *ulog);


/* Set AIO control of an update log object.
   `ulog' specifies the update log object.
   If successful, the return value is true, else, it is false. */
bool tculogsetaio(TCULOG *ulog);


/* Open files of an update log object.
   `ulog' specifies the update log object.
   `base' specifies the path of the base directory.
   `limsiz' specifies the limit size of each file.  If it is not more than 0, no limit is
   specified.
   If successful, the return value is true, else, it is false. */
bool tculogopen(TCULOG *ulog, const char *base, uint64_t limsiz);


/* Close files of an update log object.
   `ulog' specifies the update log object.
   If successful, the return value is true, else, it is false. */
bool tculogclose(TCULOG *ulog);


/* Get the mutex index of a record.
   `ulog' specifies the update log object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   The return value is the mutex index of a record. */
int tculogrmtxidx(TCULOG *ulog, const char *kbuf, int ksiz);


/* Begin the critical section of an update log object.
   `ulog' specifies the update log object.
   `idx' specifies the index of the record lock.  -1 means to lock all.
   If successful, the return value is true, else, it is false. */
bool tculogbegin(TCULOG *ulog, int idx);


/* End the critical section of an update log object.
   `ulog' specifies the update log object.
   `idx' specifies the index of the record lock.  -1 means to lock all.
   If successful, the return value is true, else, it is false. */
bool tculogend(TCULOG *ulog, int idx);


/* Write a message into an update log object.
   `ulog' specifies the update log object.
   `ts' specifies the timestamp.  If it is 0, the current time is specified.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `ptr' specifies the pointer to the region of the message.
   `size' specifies the size of the region.
   If successful, the return value is true, else, it is false. */
bool tculogwrite(TCULOG *ulog, uint64_t ts, uint32_t sid, uint32_t mid,
                 const void *ptr, int size);


/* Create a log reader object.
   `ulog' specifies the update log object.
   `ts' specifies the beginning timestamp.
   The return value is the new log reader object. */
TCULRD *tculrdnew(TCULOG *ulog, uint64_t ts);


/* Delete a log reader object.
   `ulrd' specifies the log reader object. */
void tculrddel(TCULRD *ulrd);


/* Wait the next message is written.
   `ulrd' specifies the log reader object. */
void tculrdwait(TCULRD *ulrd);


/* Read a message from a log reader object.
   `ulrd' specifies the log reader object.
   `sp' specifies the pointer to the variable into which the size of the region of the return
   value is assigned.
   `tsp' specifies the pointer to the variable into which the timestamp of the next message is
   assigned.
   `sidp' specifies the pointer to the variable into which the origin server ID of the next
   message is assigned.
   `midp' specifies the pointer to the variable into which the master server ID of the next
   message is assigned.
   If successful, the return value is the pointer to the region of the value of the next message.
   `NULL' is returned if no record is to be read. */
const void *tculrdread(TCULRD *ulrd, int *sp, uint64_t *tsp, uint32_t *sidp, uint32_t *midp);


/* Store a record into an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, it is overwritten. */
bool tculogadbput(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                  const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Store a new record into an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, this function has no effect. */
bool tculogadbputkeep(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                      const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Concatenate a value at the end of the existing record in an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tculogadbputcat(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                     const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Concatenate a value at the end of the existing record and shift it to the left.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   `width' specifies the width of the record.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tculogadbputshl(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                     const void *kbuf, int ksiz, const void *vbuf, int vsiz, int width);


/* Remove a record of an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   If successful, the return value is true, else, it is false. */
bool tculogadbout(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                  const void *kbuf, int ksiz);


/* Add an integer to a record in an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object connected as a writer.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `num' specifies the additional value.
   If successful, the return value is the summation value, else, it is `INT_MIN'.
   If the corresponding record exists, the value is treated as an integer and is added to.  If no
   record corresponds, a new record of the additional value is stored. */
int tculogadbaddint(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                    const void *kbuf, int ksiz, int num);


/* Add a real number to a record in an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object connected as a writer.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `num' specifies the additional value.
   If successful, the return value is the summation value, else, it is `NAN'.
   If the corresponding record exists, the value is treated as a real number and is added to.  If
   no record corresponds, a new record of the additional value is stored. */
double tculogadbadddouble(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                          const void *kbuf, int ksiz, double num);


/* Synchronize updated contents of an abstract database object with the file and the device.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   If successful, the return value is true, else, it is false. */
bool tculogadbsync(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb);


/* Optimize the storage of an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `params' specifies the string of the tuning parameters, which works as with the tuning
   of parameters the function `tcadbopen'.  If it is `NULL', it is not used.
   If successful, the return value is true, else, it is false. */
bool tculogadboptimize(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb, const char *params);


/* Remove all records of an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   If successful, the return value is true, else, it is false. */
bool tculogadbvanish(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb);


/* Call a versatile function for miscellaneous operations of an abstract database object.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `adb' specifies the abstract database object.
   `name' specifies the name of the function.
   `args' specifies a list object containing arguments.
   If successful, the return value is a list object of the result.  `NULL' is returned on failure.
   All databases support "putlist", "outlist", and "getlist".  "putdup" is to store records.  It
   receives keys and values one after the other, and returns an empty list.  "outlist" is to
   remove records.  It receives keys, and returns an empty list.  "getlist" is to retrieve
   records.  It receives keys, and returns values.  Because the object of the return value is
   created with the function `tclistnew', it should be deleted with the function `tclistdel' when
   it is no longer in use. */
TCLIST *tculogadbmisc(TCULOG *ulog, uint32_t sid, uint32_t mid, TCADB *adb,
                      const char *name, const TCLIST *args);


/* Restore an abstract database object.
   `adb' specifies the abstract database object.
   `path' specifies the path of the update log directory.
   `ts' specifies the beginning time stamp.
   `con' specifies whether consistency checking is performed.
   `ulog' specifies the update log object.
   If successful, the return value is true, else, it is false. */
bool tculogadbrestore(TCADB *adb, const char *path, uint64_t ts, bool con, TCULOG *ulog);


/* Redo an update log message.
   `adb' specifies the abstract database object.
   `ptr' specifies the pointer to the region of the message.
   `size' specifies the size of the region.
   `ulog' specifies the update log object.
   `sid' specifies the origin server ID of the message.
   `mid' specifies the master server ID of the message.
   `cp' specifies the pointer to the variable into which the result of consistency checking is
   assigned.
   If successful, the return value is true, else, it is false. */
bool tculogadbredo(TCADB *adb, const char *ptr, int size, TCULOG *ulog,
                   uint32_t sid, uint32_t mid, bool *cp);


/* Create a replication object.
   The return value is the new replicatoin object. */
TCREPL *tcreplnew(void);


/* Delete a replication object.
   `repl' specifies the replication object. */
void tcrepldel(TCREPL *repl);


/* Open a replication object.
   `repl' specifies the replication object.
   `host' specifies the name or the address of the server.
   `port' specifies the port number.
   `sid' specifies the server ID of self messages.
   If successful, the return value is true, else, it is false. */
bool tcreplopen(TCREPL *repl, const char *host, int port, uint64_t ts, uint32_t sid);


/* Close a remote database object.
   `rdb' specifies the remote database object.
   If successful, the return value is true, else, it is false. */
bool tcreplclose(TCREPL *repl);


/* Read a message from a replication object.
   `repl' specifies the replication object.
   `sp' specifies the pointer to the variable into which the size of the region of the return
   value is assigned.
   `tsp' specifies the pointer to the variable into which the timestamp of the next message is
   assigned.
   `sidp' specifies the pointer to the variable into which the origin server ID of the next
   message is assigned.
   If successful, the return value is the pointer to the region of the value of the next message.
   `NULL' is returned if no record is to be read.  Empty string is returned when the no-operation
   command has been received. */
const char *tcreplread(TCREPL *repl, int *sp, uint64_t *tsp, uint32_t *sidp);



__TCULOG_CLINKAGEEND
#endif                                   /* duplication check */


/* END OF FILE */
