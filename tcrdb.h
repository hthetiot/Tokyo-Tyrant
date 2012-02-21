/*************************************************************************************************
 * The remote database API of Tokyo Tyrant
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


#ifndef _TCRDB_H                         /* duplication check */
#define _TCRDB_H

#if defined(__cplusplus)
#define __TCRDB_CLINKAGEBEGIN extern "C" {
#define __TCRDB_CLINKAGEEND }
#else
#define __TCRDB_CLINKAGEBEGIN
#define __TCRDB_CLINKAGEEND
#endif
__TCRDB_CLINKAGEBEGIN


#include <ttutil.h>



/*************************************************************************************************
 * API
 *************************************************************************************************/


typedef struct {                         /* type of structure for a remote database */
  pthread_mutex_t mmtx;                  /* mutex for method */
  pthread_key_t eckey;                   /* key for thread specific error code */
  char *host;                            /* host name */
  int port;                              /* port number */
  char *expr;                            /* simple server expression */
  int fd;                                /* file descriptor */
  TTSOCK *sock;                          /* socket object */
  double timeout;                        /* timeout */
  int opts;                              /* options */
} TCRDB;

enum {                                   /* enumeration for error codes */
  TTESUCCESS,                            /* success */
  TTEINVALID,                            /* invalid operation */
  TTENOHOST,                             /* host not found */
  TTEREFUSED,                            /* connection refused */
  TTESEND,                               /* send error */
  TTERECV,                               /* recv error */
  TTEKEEP,                               /* existing record */
  TTENOREC,                              /* no record found */
  TTEMISC = 9999                         /* miscellaneous error */
};

enum {                                   /* enumeration for tuning options */
  RDBTRECON = 1 << 0                     /* reconnect automatically */
};

enum {                                   /* enumeration for scripting extension options */
  RDBXOLCKREC = 1 << 0,                  /* record locking */
  RDBXOLCKGLB = 1 << 1                   /* global locking */
};

enum {                                   /* enumeration for restore options */
  RDBROCHKCON = 1 << 0                   /* consistency checking */
};

enum {                                   /* enumeration for miscellaneous operation options */
  RDBMONOULOG = 1 << 0                   /* omission of update log */
};


/* Get the message string corresponding to an error code.
   `ecode' specifies the error code.
   The return value is the message string of the error code. */
const char *tcrdberrmsg(int ecode);


/* Create a remote database object.
   The return value is the new remote database object. */
TCRDB *tcrdbnew(void);


/* Delete a remote database object.
   `rdb' specifies the remote database object. */
void tcrdbdel(TCRDB *rdb);


/* Get the last happened error code of a remote database object.
   `rdb' specifies the remote database object.
   The return value is the last happened error code.
   The following error code is defined: `TTESUCCESS' for success, `TTEINVALID' for invalid
   operation, `TTENOHOST' for host not found, `TTEREFUSED' for connection refused, `TTESEND' for
   send error, `TTERECV' for recv error, `TTEKEEP' for existing record, `TTENOREC' for no record
   found, `TTEMISC' for miscellaneous error. */
int tcrdbecode(TCRDB *rdb);


/* Set the tuning parameters of a hash database object.
   `rdb' specifies the remote database object.
   `timeout' specifies the timeout of each query in seconds.  If it is not more than 0, the
   timeout is not specified.
   `opts' specifies options by bitwise-or: `RDBTRECON' specifies that the connection is recovered
   automatically when it is disconnected.
   If successful, the return value is true, else, it is false.
   Note that the tuning parameters should be set before the database is opened. */
bool tcrdbtune(TCRDB *rdb, double timeout, int opts);


/* Open a remote database.
   `rdb' specifies the remote database object.
   `host' specifies the name or the address of the server.
   `port' specifies the port number.  If it is not more than 0, UNIX domain socket is used and
   the path of the socket file is specified by the host parameter.
   If successful, the return value is true, else, it is false. */
bool tcrdbopen(TCRDB *rdb, const char *host, int port);


/* Open a remote database with a simple server expression.
   `rdb' specifies the remote database object.
   `expr' specifies the simple server expression.  It is composed of two substrings separated
   by ":".  The former field specifies the name or the address of the server.  The latter field
   specifies the port number.  If the latter field is omitted, the default port number is
   specified.
   If successful, the return value is true, else, it is false. */
bool tcrdbopen2(TCRDB *rdb, const char *expr);


/* Close a remote database object.
   `rdb' specifies the remote database object.
   If successful, the return value is true, else, it is false. */
bool tcrdbclose(TCRDB *rdb);


/* Store a record into a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, it is overwritten. */
bool tcrdbput(TCRDB *rdb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Store a string record into a remote object.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   `vstr' specifies the string of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, it is overwritten. */
bool tcrdbput2(TCRDB *rdb, const char *kstr, const char *vstr);


/* Store a new record into a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, this function has no effect. */
bool tcrdbputkeep(TCRDB *rdb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Store a new string record into a remote database object.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   `vstr' specifies the string of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, this function has no effect. */
bool tcrdbputkeep2(TCRDB *rdb, const char *kstr, const char *vstr);


/* Concatenate a value at the end of the existing record in a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tcrdbputcat(TCRDB *rdb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Concatenate a string value at the end of the existing record in a remote database object.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   `vstr' specifies the string of the value.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tcrdbputcat2(TCRDB *rdb, const char *kstr, const char *vstr);


/* Concatenate a value at the end of the existing record and shift it to the left.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   `width' specifies the width of the record.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tcrdbputshl(TCRDB *rdb, const void *kbuf, int ksiz, const void *vbuf, int vsiz, int width);


/* Concatenate a string value at the end of the existing record and shift it to the left.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   `vstr' specifies the string of the value.
   `width' specifies the width of the record.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tcrdbputshl2(TCRDB *rdb, const char *kstr, const char *vstr, int width);


/* Store a record into a remote database object without response from the server.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, it is overwritten. */
bool tcrdbputnr(TCRDB *rdb, const void *kbuf, int ksiz, const void *vbuf, int vsiz);


/* Store a string record into a remote object without response from the server.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   `vstr' specifies the string of the value.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, it is overwritten. */
bool tcrdbputnr2(TCRDB *rdb, const char *kstr, const char *vstr);


/* Remove a record of a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   If successful, the return value is true, else, it is false. */
bool tcrdbout(TCRDB *rdb, const void *kbuf, int ksiz);


/* Remove a string record of a remote database object.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   If successful, the return value is true, else, it is false. */
bool tcrdbout2(TCRDB *rdb, const char *kstr);


/* Retrieve a record in a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `sp' specifies the pointer to the variable into which the size of the region of the return
   value is assigned.
   If successful, the return value is the pointer to the region of the value of the corresponding
   record.  `NULL' is returned if no record corresponds.
   Because an additional zero code is appended at the end of the region of the return value,
   the return value can be treated as a character string.  Because the region of the return
   value is allocated with the `malloc' call, it should be released with the `free' call when
   it is no longer in use. */
void *tcrdbget(TCRDB *rdb, const void *kbuf, int ksiz, int *sp);


/* Retrieve a string record in a remote database object.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   If successful, the return value is the string of the value of the corresponding record.
   `NULL' is returned if no record corresponds.
   Because the region of the return value is allocated with the `malloc' call, it should be
   released with the `free' call when it is no longer in use. */
char *tcrdbget2(TCRDB *rdb, const char *kstr);


/* Retrieve records in a remote database object.
   `rdb' specifies the remote database object.
   `recs' specifies a map object containing the retrieval keys.  As a result of this function,
   keys existing in the database have the corresponding values and keys not existing in the
   database are removed.
   If successful, the return value is true, else, it is false. */
bool tcrdbget3(TCRDB *rdb, TCMAP *recs);


/* Get the size of the value of a record in a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   If successful, the return value is the size of the value of the corresponding record, else,
   it is -1. */
int tcrdbvsiz(TCRDB *rdb, const void *kbuf, int ksiz);


/* Get the size of the value of a string record in a remote database object.
   `rdb' specifies the remote database object.
   `kstr' specifies the string of the key.
   If successful, the return value is the size of the value of the corresponding record, else,
   it is -1. */
int tcrdbvsiz2(TCRDB *rdb, const char *kstr);


/* Initialize the iterator of a remote database object.
   `rdb' specifies the remote database object.
   If successful, the return value is true, else, it is false.
   The iterator is used in order to access the key of every record stored in a database. */
bool tcrdbiterinit(TCRDB *rdb);


/* Get the next key of the iterator of a remote database object.
   `rdb' specifies the remote database object.
   `sp' specifies the pointer to the variable into which the size of the region of the return
   value is assigned.
   If successful, the return value is the pointer to the region of the next key, else, it is
   `NULL'.  `NULL' is returned when no record is to be get out of the iterator.
   Because an additional zero code is appended at the end of the region of the return value, the
   return value can be treated as a character string.  Because the region of the return value is
   allocated with the `malloc' call, it should be released with the `free' call when it is no
   longer in use.  The iterator can be updated by multiple connections and then it is not assured
   that every record is traversed. */
void *tcrdbiternext(TCRDB *rdb, int *sp);


/* Get the next key string of the iterator of a remote database object.
   `rdb' specifies the remote database object.
   If successful, the return value is the string of the next key, else, it is `NULL'.  `NULL' is
   returned when no record is to be get out of the iterator.
   Because the region of the return value is allocated with the `malloc' call, it should be
   released with the `free' call when it is no longer in use.  The iterator can be updated by
   multiple connections and then it is not assured that every record is traversed. */
char *tcrdbiternext2(TCRDB *rdb);


/* Get forward matching keys in a remote database object.
   `rdb' specifies the remote database object.
   `pbuf' specifies the pointer to the region of the prefix.
   `psiz' specifies the size of the region of the prefix.
   `max' specifies the maximum number of keys to be fetched.  If it is negative, no limit is
   specified.
   The return value is a list object of the corresponding keys.  This function does never fail.
   It returns an empty list even if no key corresponds.
   Because the object of the return value is created with the function `tclistnew', it should be
   deleted with the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbfwmkeys(TCRDB *rdb, const void *pbuf, int psiz, int max);


/* Get forward matching string keys in a remote database object.
   `rdb' specifies the remote database object.
   `pstr' specifies the string of the prefix.
   `max' specifies the maximum number of keys to be fetched.  If it is negative, no limit is
   specified.
   The return value is a list object of the corresponding keys.  This function does never fail.
   It returns an empty list even if no key corresponds.
   Because the object of the return value is created with the function `tclistnew', it should be
   deleted with the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbfwmkeys2(TCRDB *rdb, const char *pstr, int max);


/* Add an integer to a record in a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `num' specifies the additional value.
   If successful, the return value is the summation value, else, it is `INT_MIN'.
   If the corresponding record exists, the value is treated as an integer and is added to.  If no
   record corresponds, a new record of the additional value is stored. */
int tcrdbaddint(TCRDB *rdb, const void *kbuf, int ksiz, int num);


/* Add a real number to a record in a remote database object.
   `rdb' specifies the remote database object.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `num' specifies the additional value.
   If successful, the return value is the summation value, else, it is Not-a-Number.
   If the corresponding record exists, the value is treated as a real number and is added to.  If
   no record corresponds, a new record of the additional value is stored. */
double tcrdbadddouble(TCRDB *rdb, const void *kbuf, int ksiz, double num);


/* Call a function of the scripting language extension.
   `rdb' specifies the remote database object.
   `name' specifies the function name.
   `opts' specifies options by bitwise-or: `RDBXOLCKREC' for record locking, `RDBXOLCKGLB' for
   global locking.
   `kbuf' specifies the pointer to the region of the key.
   `ksiz' specifies the size of the region of the key.
   `vbuf' specifies the pointer to the region of the value.
   `vsiz' specifies the size of the region of the value.
   `sp' specifies the pointer to the variable into which the size of the region of the return
   value is assigned.
   If successful, the return value is the pointer to the region of the value of the response.
   `NULL' is returned on failure.
   Because an additional zero code is appended at the end of the region of the return value,
   the return value can be treated as a character string.  Because the region of the return
   value is allocated with the `malloc' call, it should be released with the `free' call when
   it is no longer in use. */
void *tcrdbext(TCRDB *rdb, const char *name, int opts,
               const void *kbuf, int ksiz, const void *vbuf, int vsiz, int *sp);


/* Call a function of the scripting language extension with string parameters.
   `rdb' specifies the remote database object.
   `name' specifies the function name.
   `opts' specifies options by bitwise-or: `RDBXOLCKREC' for record locking, `RDBXOLCKGLB' for
   global locking.
   `kstr' specifies the string of the key.
   `vstr' specifies the string of the value.
   If successful, the return value is the string of the value of the response.  `NULL' is
   returned on failure.
   Because the region of the return value is allocated with the `malloc' call, it should be
   released with the `free' call when it is no longer in use. */
char *tcrdbext2(TCRDB *rdb, const char *name, int opts, const char *kstr, const char *vstr);


/* Synchronize updated contents of a remote database object with the file and the device.
   `rdb' specifies the remote database object.
   If successful, the return value is true, else, it is false. */
bool tcrdbsync(TCRDB *rdb);


/* Optimize the storage of a remove database object.
   `rdb' specifies the remote database object.
   `params' specifies the string of the tuning parameters.  If it is `NULL', it is not used.
   If successful, the return value is true, else, it is false. */
bool tcrdboptimize(TCRDB *rdb, const char *params);


/* Remove all records of a remote database object.
   `rdb' specifies the remote database object.
   If successful, the return value is true, else, it is false. */
bool tcrdbvanish(TCRDB *rdb);


/* Copy the database file of a remote database object.
   `rdb' specifies the remote database object.
   `path' specifies the path of the destination file.  If it begins with `@', the trailing
   substring is executed as a command line.
   If successful, the return value is true, else, it is false.  False is returned if the executed
   command returns non-zero code.
   The database file is assured to be kept synchronized and not modified while the copying or
   executing operation is in progress.  So, this function is useful to create a backup file of
   the database file. */
bool tcrdbcopy(TCRDB *rdb, const char *path);


/* Restore the database file of a remote database object from the update log.
   `rdb' specifies the remote database object.
   `path' specifies the path of the update log directory.
   `ts' specifies the beginning timestamp in microseconds.
   `opts' specifies options by bitwise-or: `RDBROCHKCON' for consistency checking.
   If successful, the return value is true, else, it is false. */
bool tcrdbrestore(TCRDB *rdb, const char *path, uint64_t ts, int opts);


/* Set the replication master of a remote database object.
   `rdb' specifies the remote database object.
   `host' specifies the name or the address of the server.  If it is `NULL', replication of the
   database is disabled.
   `port' specifies the port number.
   `ts' specifies the beginning timestamp in microseconds.
   `opts' specifies options by bitwise-or: `RDBROCHKCON' for consistency checking.
   If successful, the return value is true, else, it is false. */
bool tcrdbsetmst(TCRDB *rdb, const char *host, int port, uint64_t ts, int opts);


/* Set the replication master of a remote database object with a simple server expression.
   `rdb' specifies the remote database object.
   `expr' specifies the simple server expression.  It is composed of two substrings separated
   by ":".  The former field specifies the name or the address of the server.  The latter field
   specifies the port number.  If the latter field is omitted, the default port number is
   specified.
   `ts' specifies the beginning timestamp in microseconds.
   `opts' specifies options by bitwise-or: `RDBROCHKCON' for consistency checking.
   If successful, the return value is true, else, it is false. */
bool tcrdbsetmst2(TCRDB *rdb, const char *expr, uint64_t ts, int opts);


/* Get the simple server expression of an abstract database object.
   `rdb' specifies the remote database object.
   The return value is the simple server expression or `NULL' if the object does not connect to
   any database server. */
const char *tcrdbexpr(TCRDB *rdb);


/* Get the number of records of a remote database object.
   `rdb' specifies the remote database object.
   The return value is the number of records or 0 if the object does not connect to any database
   server. */
uint64_t tcrdbrnum(TCRDB *rdb);


/* Get the size of the database of a remote database object.
   `rdb' specifies the remote database object.
   The return value is the size of the database or 0 if the object does not connect to any
   database server. */
uint64_t tcrdbsize(TCRDB *rdb);


/* Get the status string of the database of a remote database object.
   `rdb' specifies the remote database object.
   The return value is the status message of the database or `NULL' if the object does not
   connect to any database server.  The message format is TSV.  The first field of each line
   means the parameter name and the second field means the value.
   Because the region of the return value is allocated with the `malloc' call, it should be
   released with the `free' call when it is no longer in use. */
char *tcrdbstat(TCRDB *rdb);


/* Call a versatile function for miscellaneous operations of a remote database object.
   `rdb' specifies the remote database object.
   `name' specifies the name of the function.  All databases support "putlist", "outlist", and
   "getlist".  "putlist" is to store records.  It receives keys and values one after the other,
   and returns an empty list.  "outlist" is to remove records.  It receives keys, and returns an
   empty list.  "getlist" is to retrieve records.  It receives keys, and returns keys and values
   of corresponding records one after the other.  Table database supports "setindex", "search",
   and "genuid".
   `opts' specifies options by bitwise-or: `RDBMONOULOG' for omission of the update log.
   `args' specifies a list object containing arguments.
   If successful, the return value is a list object of the result.  `NULL' is returned on failure.
   Because the object of the return value is created with the function `tclistnew', it
   should be deleted with the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbmisc(TCRDB *rdb, const char *name, int opts, const TCLIST *args);



/*************************************************************************************************
 * table extension
 *************************************************************************************************/


enum {                                   /* enumeration for index types */
  RDBITLEXICAL = TDBITLEXICAL,           /* lexical string */
  RDBITDECIMAL = TDBITDECIMAL,           /* decimal string */
  RDBITTOKEN = TDBITTOKEN,               /* token inverted index */
  RDBITQGRAM = TDBITQGRAM,               /* q-gram inverted index */
  RDBITOPT = TDBITOPT,                   /* optimize */
  RDBITVOID = TDBITVOID,                 /* void */
  RDBITKEEP = TDBITKEEP                  /* keep existing index */
};

typedef struct {                         /* type of structure for a query */
  TCRDB *rdb;                            /* database object */
  TCLIST *args;                          /* arguments for the method */
  TCXSTR *hint;                          /* hint string */
} RDBQRY;

enum {                                   /* enumeration for query conditions */
  RDBQCSTREQ = TDBQCSTREQ,               /* string is equal to */
  RDBQCSTRINC = TDBQCSTRINC,             /* string is included in */
  RDBQCSTRBW = TDBQCSTRBW,               /* string begins with */
  RDBQCSTREW = TDBQCSTREW,               /* string ends with */
  RDBQCSTRAND = TDBQCSTRAND,             /* string includes all tokens in */
  RDBQCSTROR = TDBQCSTROR,               /* string includes at least one token in */
  RDBQCSTROREQ = TDBQCSTROREQ,           /* string is equal to at least one token in */
  RDBQCSTRRX = TDBQCSTRRX,               /* string matches regular expressions of */
  RDBQCNUMEQ = TDBQCNUMEQ,               /* number is equal to */
  RDBQCNUMGT = TDBQCNUMGT,               /* number is greater than */
  RDBQCNUMGE = TDBQCNUMGE,               /* number is greater than or equal to */
  RDBQCNUMLT = TDBQCNUMLT,               /* number is less than */
  RDBQCNUMLE = TDBQCNUMLE,               /* number is less than or equal to */
  RDBQCNUMBT = TDBQCNUMBT,               /* number is between two tokens of */
  RDBQCNUMOREQ = TDBQCNUMOREQ,           /* number is equal to at least one token in */
  RDBQCFTSPH = TDBQCFTSPH,               /* full-text search with the phrase of */
  RDBQCFTSAND = TDBQCFTSAND,             /* full-text search with all tokens in */
  RDBQCFTSOR = TDBQCFTSOR,               /* full-text search with at least one token in */
  RDBQCFTSEX = TDBQCFTSEX,               /* full-text search with the compound expression of */
  RDBQCNEGATE = TDBQCNEGATE,             /* negation flag */
  RDBQCNOIDX = TDBQCNOIDX                /* no index flag */
};

enum {                                   /* enumeration for order types */
  RDBQOSTRASC = TDBQOSTRASC,             /* string ascending */
  RDBQOSTRDESC = TDBQOSTRDESC,           /* string descending */
  RDBQONUMASC = TDBQONUMASC,             /* number ascending */
  RDBQONUMDESC = TDBQONUMDESC            /* number descending */
};

enum {                                   /* enumeration for set operation types */
  RDBMSUNION = TDBMSUNION,               /* union */
  RDBMSISECT = TDBMSISECT,               /* intersection */
  RDBMSDIFF = TDBMSDIFF                  /* difference */
};


/* Store a record into a remote database object.
   `rdb' specifies the remote database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `cols' specifies a map object containing columns.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, it is overwritten. */
bool tcrdbtblput(TCRDB *rdb, const void *pkbuf, int pksiz, TCMAP *cols);


/* Store a new record into a remote database object.
   `rdb' specifies the remote database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `cols' specifies a map object containing columns.
   If successful, the return value is true, else, it is false.
   If a record with the same key exists in the database, this function has no effect. */
bool tcrdbtblputkeep(TCRDB *rdb, const void *pkbuf, int pksiz, TCMAP *cols);


/* Concatenate columns of the existing record in a remote database object.
   `rdb' specifies the remote database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `cols' specifies a map object containing columns.
   If successful, the return value is true, else, it is false.
   If there is no corresponding record, a new record is created. */
bool tcrdbtblputcat(TCRDB *rdb, const void *pkbuf, int pksiz, TCMAP *cols);


/* Remove a record of a remote database object.
   `rdb' specifies the remote database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   If successful, the return value is true, else, it is false. */
bool tcrdbtblout(TCRDB *rdb, const void *pkbuf, int pksiz);


/* Retrieve a record in a remote database object.
   `rdb' specifies the remote database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   If successful, the return value is a map object of the columns of the corresponding record.
   `NULL' is returned if no record corresponds.
   Because the object of the return value is created with the function `tcmapnew', it should be
   deleted with the function `tcmapdel' when it is no longer in use. */
TCMAP *tcrdbtblget(TCRDB *rdb, const void *pkbuf, int pksiz);


/* Set a column index to a remote database object.
   `rdb' specifies the remote database object.
   `name' specifies the name of a column.  If the name of an existing index is specified, the
   index is rebuilt.  An empty string means the primary key.
   `type' specifies the index type: `RDBITLEXICAL' for lexical string, `RDBITDECIMAL' for decimal
   string, `RDBITTOKEN' for token inverted index, `RDBITQGRAM' for q-gram inverted index.  If it
   is `RDBITOPT', the index is optimized.  If it is `RDBITVOID', the index is removed.  If
   `RDBITKEEP' is added by bitwise-or and the index exists, this function merely returns failure.
   If successful, the return value is true, else, it is false. */
bool tcrdbtblsetindex(TCRDB *rdb, const char *name, int type);


/* Generate a unique ID number of a remote database object.
   `rdb' specifies the remote database object.
   The return value is the new unique ID number or -1 on failure. */
int64_t tcrdbtblgenuid(TCRDB *rdb);


/* Create a query object.
   `rdb' specifies the remote database object.
   The return value is the new query object. */
RDBQRY *tcrdbqrynew(TCRDB *rdb);


/* Delete a query object.
   `qry' specifies the query object. */
void tcrdbqrydel(RDBQRY *qry);


/* Add a narrowing condition to a query object.
   `qry' specifies the query object.
   `name' specifies the name of a column.  An empty string means the primary key.
   `op' specifies an operation type: `RDBQCSTREQ' for string which is equal to the expression,
   `RDBQCSTRINC' for string which is included in the expression, `RDBQCSTRBW' for string which
   begins with the expression, `RDBQCSTREW' for string which ends with the expression,
   `RDBQCSTRAND' for string which includes all tokens in the expression, `RDBQCSTROR' for string
   which includes at least one token in the expression, `RDBQCSTROREQ' for string which is equal
   to at least one token in the expression, `RDBQCSTRRX' for string which matches regular
   expressions of the expression, `RDBQCNUMEQ' for number which is equal to the expression,
   `RDBQCNUMGT' for number which is greater than the expression, `RDBQCNUMGE' for number which is
   greater than or equal to the expression, `RDBQCNUMLT' for number which is less than the
   expression, `RDBQCNUMLE' for number which is less than or equal to the expression, `RDBQCNUMBT'
   for number which is between two tokens of the expression, `RDBQCNUMOREQ' for number which is
   equal to at least one token in the expression, `RDBQCFTSPH' for full-text search with the
   phrase of the expression, `RDBQCFTSAND' for full-text search with all tokens in the expression,
   `RDBQCFTSOR' for full-text search with at least one token in the expression, `RDBQCFTSEX' for
   full-text search with the compound expression.  All operations can be flagged by bitwise-or:
   `RDBQCNEGATE' for negation, `RDBQCNOIDX' for using no index.
   `expr' specifies an operand exression. */
void tcrdbqryaddcond(RDBQRY *qry, const char *name, int op, const char *expr);


/* Set the order of a query object.
   `qry' specifies the query object.
   `name' specifies the name of a column.  An empty string means the primary key.
   `type' specifies the order type: `RDBQOSTRASC' for string ascending, `RDBQOSTRDESC' for
   string descending, `RDBQONUMASC' for number ascending, `RDBQONUMDESC' for number descending. */
void tcrdbqrysetorder(RDBQRY *qry, const char *name, int type);


/* Set the limit number of records of the result of a query object.
   `qry' specifies the query object.
   `max' specifies the maximum number of records of the result.  If it is negative, no limit is
   specified.
   `skip' specifies the number of skipped records of the result.  If it is not more than 0, no
   record is skipped. */
void tcrdbqrysetlimit(RDBQRY *qry, int max, int skip);


/* Execute the search of a query object.
   `qry' specifies the query object.
   The return value is a list object of the primary keys of the corresponding records.  This
   function does never fail.  It returns an empty list even if no record corresponds.
   Because the object of the return value is created with the function `tclistnew', it should
   be deleted with the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbqrysearch(RDBQRY *qry);


/* Remove each record corresponding to a query object.
   `qry' specifies the query object of the database.
   If successful, the return value is true, else, it is false. */
bool tcrdbqrysearchout(RDBQRY *qry);


/* Get records corresponding to the search of a query object.
   `qry' specifies the query object.
   The return value is a list object of zero separated columns of the corresponding records.
   This function does never fail.  It returns an empty list even if no record corresponds.
   Each element of the list can be treated with the function `tcrdbqryrescols'.  Because the
   object of the return value is created with the function `tclistnew', it should be deleted with
   the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbqrysearchget(RDBQRY *qry);


/* Get columns of a record in a search result.
   `res' specifies a list of zero separated columns of the search result.
   `index' the index of a element of the search result.
   The return value is a map object containing columns.
   Because the object of the return value is created with the function `tcmapnew', it should be
   deleted with the function `tcmapdel' when it is no longer in use. */
TCMAP *tcrdbqryrescols(TCLIST *res, int index);


/* Get the count of corresponding records of a query object.
   `qry' specifies the query object.
   The return value is the count of corresponding records or 0 on failure. */
int tcrdbqrysearchcount(RDBQRY *qry);


/* Get the hint string of a query object.
   `qry' specifies the query object.
   The return value is the hint string.
   This function should be called after the query execution by `tcrdbqrysearch' and so on.  The
   region of the return value is overwritten when this function is called again. */
const char *tcrdbqryhint(RDBQRY *qry);


/* Retrieve records with multiple query objects and get the set of the result.
   `qrys' specifies an array of the query objects.
   `num' specifies the number of elements of the array.
   `type' specifies a set operation type: `RDBMSUNION' for the union set, `RDBMSISECT' for the
   intersection set, `RDBMSDIFF' for the difference set.
   The return value is a list object of the primary keys of the corresponding records.  This
   function does never fail.  It returns an empty list even if no record corresponds.
   If the first query object has the order setting, the result array is sorted by the order.
   Because the object of the return value is created with the function `tclistnew', it should be
   deleted with the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbmetasearch(RDBQRY **qrys, int num, int type);


/* Search for multiple servers in parallel.
   `qrys' specifies an array of the query objects.
   `num' specifies the number of elements of the array.
   The return value is a list object of zero separated columns of the corresponding records.
   This function does never fail.  It returns an empty list even if no record corresponds.
   Each element of the list can be treated with the function `tcrdbqryrescols'.  Because the
   object of the return value is created with the function `tclistnew', it should be deleted with
   the function `tclistdel' when it is no longer in use. */
TCLIST *tcrdbparasearch(RDBQRY **qrys, int num);



/*************************************************************************************************
 * features for experts
 *************************************************************************************************/


/* Set the error code of a remote database object.
   `rdb' specifies the remote database object.
   `ecode' specifies the error code. */
void tcrdbsetecode(TCRDB *rdb, int ecode);



__TCRDB_CLINKAGEEND
#endif                                   /* duplication check */


/* END OF FILE */
