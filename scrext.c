/*************************************************************************************************
 * Scripting language extension of Tokyo Tyrant
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


#include "scrext.h"



/*************************************************************************************************
 * by default
 *************************************************************************************************/


#if defined(TTNOEXT)


typedef struct _SCREXT {                 // type of structure of the script extension
  struct _SCREXT **screxts;              // script extension objects
  int thnum;                             // number of native threads
  int thid;                              // thread ID
  char *path;                            // path of the initializing script
  TCADB *adb;                            // abstract database object
  TCULOG *ulog;                          // update log object
  uint32_t sid;                          // server ID
  TCMDB *stash;                          // global stash object
  TCMDB *lock;                           // global lock object
  void (*logger)(int, const char *, void *);  // logging function
  void *logopq;                          // opaque pointer for the logging function
  bool term;                             // terminate flag
} SCREXT;


/* Initialize the global scripting language extension. */
void *scrextnew(void **screxts, int thnum, int thid, const char *path,
                TCADB *adb, TCULOG *ulog, uint32_t sid, TCMDB *stash, TCMDB *lock,
                void (*logger)(int, const char *, void *), void *logopq){
  SCREXT *scr = tcmalloc(sizeof(*scr));
  scr->screxts = (SCREXT **)screxts;
  scr->thnum = thnum;
  scr->thid = thid;
  scr->path = tcstrdup(path);
  scr->adb = adb;
  scr->ulog = ulog;
  scr->sid = sid;
  scr->stash = stash;
  scr->lock = lock;
  scr->logger = logger;
  scr->logopq = logopq;
  scr->term = false;
  return scr;
}


/* Destroy the global scripting language extension. */
bool scrextdel(void *scr){
  SCREXT *myscr = scr;
  tcfree(myscr->path);
  tcfree(myscr);
  return true;
}


/* Call a method of the scripting language extension. */
char *scrextcallmethod(void *scr, const char *name,
                       const void *kbuf, int ksiz, const void *vbuf, int vsiz, int *sp){
  SCREXT *myscr = scr;
  if(!strcmp(name, "put")){
    if(!tculogadbput(myscr->ulog, myscr->sid, 0, myscr->adb, kbuf, ksiz, vbuf, vsiz))
      return NULL;
    char *msg = tcstrdup("ok");
    *sp = strlen(msg);
    return msg;
  } else if(!strcmp(name, "putkeep")){
    if(!tculogadbputkeep(myscr->ulog, myscr->sid, 0, myscr->adb, kbuf, ksiz, vbuf, vsiz))
      return NULL;
    char *msg = tcstrdup("ok");
    *sp = strlen(msg);
    return msg;
  } else if(!strcmp(name, "putcat")){
    if(!tculogadbputcat(myscr->ulog, myscr->sid, 0, myscr->adb, kbuf, ksiz, vbuf, vsiz))
      return NULL;
    char *msg = tcstrdup("ok");
    *sp = strlen(msg);
    return msg;
  } else if(!strcmp(name, "out")){
    if(!tculogadbout(myscr->ulog, myscr->sid, 0, myscr->adb, kbuf, ksiz)) return NULL;
    char *msg = tcstrdup("ok");
    *sp = strlen(msg);
    return msg;
  } else if(!strcmp(name, "get")){
    return tcadbget(myscr->adb, kbuf, ksiz, sp);
  } else if(!strcmp(name, "log")){
    char *msg = tcmemdup(kbuf, ksiz);
    myscr->logger(TTLOGINFO, msg, myscr->logopq);
    tcfree(msg);
    msg = tcstrdup("ok");
    *sp = strlen(msg);
    return msg;
  }
  int psiz = strlen(myscr->path);
  int nsiz = strlen(name);
  char *msg = tcmalloc(psiz + nsiz + ksiz + vsiz + 4);
  char *wp = msg;
  memcpy(wp, myscr->path, psiz);
  wp += psiz;
  *(wp++) = ':';
  memcpy(wp, name, nsiz);
  wp += nsiz;
  *(wp++) = ':';
  memcpy(wp, kbuf, ksiz);
  wp += ksiz;
  *(wp++) = ':';
  memcpy(wp, vbuf, vsiz);
  wp += vsiz;
  *sp = wp - msg;
  return msg;
}


/* Send the terminate signal to the scripting language extension */
bool scrextkill(void *scr){
  SCREXT *myscr = scr;
  myscr->term = true;
  return true;
}


#endif



/*************************************************************************************************
 * for Lua
 *************************************************************************************************/


#if defined(TTLUAEXT)


#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"

#define SERVVAR      "_serv_"            // global variable name for server resources
#define ITERVAR      "_iter_"            // global variable name for iterator
#define MRMAPVAR     "_mrmap_"           // global variable name for mapreduce mapper
#define MRREDVAR     "_mrred_"           // global variable name for mapreduce reducer
#define MRPOOLVAR    "_mrpool_"          // global variable name for mapreduce pool

typedef struct {                         // type of structure of the script extension
  lua_State *lua;                        // Lua environment
  int thnum;                             // number of native threads
  int thid;                              // thread ID
} SCREXT;

typedef struct {                         // type of structure of the server data
  SCREXT **screxts;                      // script extension objects
  int thnum;                             // number of native threads
  int thid;                              // thread ID
  TCADB *adb;                            // abstract database object
  TCULOG *ulog;                          // update log object
  uint32_t sid;                          // server ID
  TCMDB *stash;                          // global stash object
  TCMDB *lock;                           // global lock object
  pthread_mutex_t *lcks;                 // mutex for user locks
  int lcknum;                            // number of user locks
  void (*logger)(int, const char *, void *);  // logging function
  void *logopq;                          // opaque pointer for the logging function
  bool term;                             // terminate flag
} SERV;


/* private function prototypes */
static void reporterror(lua_State *lua);
static bool iterrec(const void *kbuf, int ksiz, const void *vbuf, int vsiz, lua_State *lua);
static int serv_eval(lua_State *lua);
static int serv_log(lua_State *lua);
static int serv_put(lua_State *lua);
static int serv_putkeep(lua_State *lua);
static int serv_putcat(lua_State *lua);
static int serv_out(lua_State *lua);
static int serv_get(lua_State *lua);
static int serv_vsiz(lua_State *lua);
static int serv_iterinit(lua_State *lua);
static int serv_iternext(lua_State *lua);
static int serv_fwmkeys(lua_State *lua);
static int serv_addint(lua_State *lua);
static int serv_adddouble(lua_State *lua);
static int serv_vanish(lua_State *lua);
static int serv_rnum(lua_State *lua);
static int serv_size(lua_State *lua);
static int serv_misc(lua_State *lua);
static int serv_foreach(lua_State *lua);
static int serv_mapreduce(lua_State *lua);
static int serv_mapreducemapemit(lua_State *lua);
static int serv_stashput(lua_State *lua);
static int serv_stashputkeep(lua_State *lua);
static int serv_stashputcat(lua_State *lua);
static int serv_stashout(lua_State *lua);
static int serv_stashget(lua_State *lua);
static int serv_stashvanish(lua_State *lua);
static int serv_stashforeach(lua_State *lua);
static int serv_lock(lua_State *lua);
static int serv_unlock(lua_State *lua);
static int serv_pack(lua_State *lua);
static int serv_unpack(lua_State *lua);
static int serv_split(lua_State *lua);
static int serv_codec(lua_State *lua);
static int serv_hash(lua_State *lua);
static int serv_bit(lua_State *lua);
static int serv_strstr(lua_State *lua);
static int serv_regex(lua_State *lua);
static int serv_ucs(lua_State *lua);
static int serv_dist(lua_State *lua);
static int serv_isect(lua_State *lua);
static int serv_union(lua_State *lua);
static int serv_time(lua_State *lua);
static int serv_sleep(lua_State *lua);
static int serv_stat(lua_State *lua);
static int serv_glob(lua_State *lua);
static int serv_remove(lua_State *lua);
static int serv_mkdir(lua_State *lua);


/* Initialize the global scripting language extension. */
void *scrextnew(void **screxts, int thnum, int thid, const char *path,
                TCADB *adb, TCULOG *ulog, uint32_t sid, TCMDB *stash, TCMDB *lock,
                void (*logger)(int, const char *, void *), void *logopq){
  char *ibuf;
  int isiz;
  if(*path == '@'){
    ibuf = tcstrdup(path + 1);
    isiz = strlen(ibuf);
  } else if(*path != '\0'){
    ibuf = tcreadfile(path, 0, &isiz);
  } else {
    ibuf = tcmemdup("", 0);
    isiz = 0;
  }
  if(!ibuf) return NULL;
  lua_State *lua = luaL_newstate();
  if(!lua){
    tcfree(ibuf);
    return NULL;
  }
  luaL_openlibs(lua);
  lua_settop(lua, 0);
  SERV *serv = lua_newuserdata(lua, sizeof(*serv));
  serv->screxts = (SCREXT **)screxts;
  serv->thnum = thnum;
  serv->thid = thid;
  serv->adb = adb;
  serv->ulog = ulog;
  serv->sid = sid;
  serv->stash = stash;
  serv->lock = lock;
  serv->logger = logger;
  serv->logopq = logopq;
  serv->term = false;
  lua_setglobal(lua, SERVVAR);
  lua_register(lua, "_eval", serv_eval);
  lua_register(lua, "_log", serv_log);
  lua_register(lua, "_put", serv_put);
  lua_register(lua, "_putkeep", serv_putkeep);
  lua_register(lua, "_putcat", serv_putcat);
  lua_register(lua, "_out", serv_out);
  lua_register(lua, "_get", serv_get);
  lua_register(lua, "_vsiz", serv_vsiz);
  lua_register(lua, "_iterinit", serv_iterinit);
  lua_register(lua, "_iternext", serv_iternext);
  lua_register(lua, "_fwmkeys", serv_fwmkeys);
  lua_register(lua, "_addint", serv_addint);
  lua_register(lua, "_adddouble", serv_adddouble);
  lua_register(lua, "_vanish", serv_vanish);
  lua_register(lua, "_rnum", serv_rnum);
  lua_register(lua, "_size", serv_size);
  lua_register(lua, "_misc", serv_misc);
  lua_register(lua, "_foreach", serv_foreach);
  lua_register(lua, "_mapreduce", serv_mapreduce);
  lua_register(lua, "_stashput", serv_stashput);
  lua_register(lua, "_stashputkeep", serv_stashputkeep);
  lua_register(lua, "_stashputcat", serv_stashputcat);
  lua_register(lua, "_stashout", serv_stashout);
  lua_register(lua, "_stashget", serv_stashget);
  lua_register(lua, "_stashvanish", serv_stashvanish);
  lua_register(lua, "_stashforeach", serv_stashforeach);
  lua_register(lua, "_lock", serv_lock);
  lua_register(lua, "_unlock", serv_unlock);
  lua_register(lua, "_pack", serv_pack);
  lua_register(lua, "_unpack", serv_unpack);
  lua_register(lua, "_split", serv_split);
  lua_register(lua, "_codec", serv_codec);
  lua_register(lua, "_hash", serv_hash);
  lua_register(lua, "_bit", serv_bit);
  lua_register(lua, "_strstr", serv_strstr);
  lua_register(lua, "_regex", serv_regex);
  lua_register(lua, "_ucs", serv_ucs);
  lua_register(lua, "_dist", serv_dist);
  lua_register(lua, "_isect", serv_isect);
  lua_register(lua, "_union", serv_union);
  lua_register(lua, "_time", serv_time);
  lua_register(lua, "_sleep", serv_sleep);
  lua_register(lua, "_stat", serv_stat);
  lua_register(lua, "_glob", serv_glob);
  lua_register(lua, "_remove", serv_remove);
  lua_register(lua, "_mkdir", serv_mkdir);
  lua_pushstring(lua, ttversion);
  lua_setglobal(lua, "_version");
  lua_pushinteger(lua, getpid());
  lua_setglobal(lua, "_pid");
  lua_pushinteger(lua, sid);
  lua_setglobal(lua, "_sid");
  lua_pushinteger(lua, thnum);
  lua_setglobal(lua, "_thnum");
  lua_pushinteger(lua, thid + 1);
  lua_setglobal(lua, "_thid");
  lua_settop(lua, 0);
  if(luaL_loadstring(lua, ibuf) != 0 || lua_pcall(lua, 0, 0, 0) != 0) reporterror(lua);
  tcfree(ibuf);
  if(thid == 0){
    lua_getglobal(lua, "_begin");
    if(lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0) reporterror(lua);
  }
  lua_settop(lua, 0);
  SCREXT *scr = tcmalloc(sizeof(*scr));
  scr->lua = lua;
  scr->thnum = thnum;
  scr->thid = thid;
  return scr;
}


/* Destroy the global scripting language extension. */
bool scrextdel(void *scr){
  SCREXT *myscr = scr;
  lua_State *lua = myscr->lua;
  if(myscr->thid == 0){
    lua_getglobal(lua, "_end");
    if(lua_isfunction(lua, -1) && lua_pcall(lua, 0, 0, 0) != 0) reporterror(lua);
  }
  lua_close(lua);
  tcfree(scr);
  return true;
}


/* Call a method of the scripting language extension. */
char *scrextcallmethod(void *scr, const char *name,
                       const void *kbuf, int ksiz, const void *vbuf, int vsiz, int *sp){
  SCREXT *myscr = scr;
  lua_State *lua = myscr->lua;
  if(*name == '_') return NULL;
  lua_getglobal(lua, name);
  if(lua_gettop(lua) != 1 || !lua_isfunction(lua, 1)){
    lua_settop(lua, 0);
    return NULL;
  }
  lua_pushlstring(lua, kbuf, ksiz);
  lua_pushlstring(lua, vbuf, vsiz);
  if(lua_pcall(lua, 2, 1, 0) != 0){
    reporterror(lua);
    lua_settop(lua, 0);
    return NULL;
  }
  if(lua_gettop(lua) < 1) return NULL;
  const char *rbuf = NULL;
  size_t rsiz;
  switch(lua_type(lua, 1)){
    case LUA_TNUMBER:
    case LUA_TSTRING:
      rbuf = lua_tolstring(lua, 1, &rsiz);
      break;
    case LUA_TBOOLEAN:
      if(lua_toboolean(lua, 1)){
        rbuf = "true";
        rsiz = strlen(rbuf);
      }
      break;
    case LUA_TTABLE:
      if(lua_objlen(lua, 1) > 0){
        lua_rawgeti(lua, 1, 1);
        switch(lua_type(lua, -1)){
          case LUA_TNUMBER:
          case LUA_TSTRING:
            rbuf = lua_tolstring(lua, -1, &rsiz);
            break;
          case LUA_TBOOLEAN:
            if(lua_toboolean(lua, -1)){
              rbuf = "true";
              rsiz = strlen(rbuf);
            }
            break;
        }
        lua_pop(lua, 1);
      }
      break;
  }
  if(!rbuf){
    lua_settop(lua, 0);
    return NULL;
  }
  char *rv = tcmemdup(rbuf, rsiz);
  *sp = rsiz;
  lua_settop(lua, 0);
  return rv;
}


/* Send the terminate signal to the scripting language extension */
bool scrextkill(void *scr){
  SCREXT *myscr = scr;
  lua_State *lua = myscr->lua;
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  serv->term = true;
  return true;
}


/* report an error of Lua program */
static void reporterror(lua_State *lua){
  int argc = lua_gettop(lua);
  char *msg = tcsprintf("Lua error: %s", argc > 0 ? lua_tostring(lua, argc) : "unknown");
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  serv->logger(TTLOGERROR, msg, serv->logopq);
  tcfree(msg);
}


/* call function for each record */
static bool iterrec(const void *kbuf, int ksiz, const void *vbuf, int vsiz, lua_State *lua){
  int top = lua_gettop(lua);
  lua_getglobal(lua, ITERVAR);
  lua_pushlstring(lua, kbuf, ksiz);
  lua_pushlstring(lua, vbuf, vsiz);
  bool err = false;
  if(lua_pcall(lua, 2, 1, 0) == 0){
    if(lua_gettop(lua) <= top || !lua_toboolean(lua, -1)) err = true;
  } else {
    reporterror(lua);
    err = true;
  }
  lua_settop(lua, top);
  return !err;
}


/* call function to map records for mapreduce */
static bool maprec(void *map, const void *kbuf, int ksiz, const void *vbuf, int vsiz,
                   lua_State *lua){
  lua_pushlightuserdata(lua, map);
  lua_setglobal(lua, MRPOOLVAR);
  int top = lua_gettop(lua);
  lua_getglobal(lua, MRMAPVAR);
  lua_pushlstring(lua, kbuf, ksiz);
  lua_pushlstring(lua, vbuf, vsiz);
  lua_pushcfunction(lua, serv_mapreducemapemit);
  bool err = false;
  if(lua_pcall(lua, 3, 1, 0) == 0){
    if(lua_gettop(lua) < 1 && !lua_toboolean(lua, 1)) err = true;
  } else {
    reporterror(lua);
    err = true;
  }
  lua_settop(lua, top);
  return !err;
}


/* for _eval function */
static int serv_eval(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_eval: invalid arguments");
    lua_error(lua);
  }
  const char *expr = lua_tostring(lua, 1);
  if(!expr){
    lua_pushstring(lua, "_eval: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  SCREXT **screxts = serv->screxts;
  int thnum = serv->thnum;
  bool err = false;
  for(int i = 0; i < thnum; i++){
    if(!screxts[i]){
      lua_pushstring(lua, "_eval: not ready");
      lua_error(lua);
    }
  }
  for(int i = 0; i < thnum; i++){
    lua_State *elua = screxts[i]->lua;
    if(luaL_loadstring(elua, expr) != 0 || lua_pcall(elua, 0, 0, 0) != 0) reporterror(elua);
  }
  lua_settop(lua, 0);
  lua_pushboolean(lua, !err);
  return 1;
}


/* for _log function */
static int serv_log(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "_log: invalid arguments");
    lua_error(lua);
  }
  const char *msg = lua_tostring(lua, 1);
  if(!msg){
    lua_pushstring(lua, "_log: invalid arguments");
    lua_error(lua);
  }
  int level = TTLOGINFO;
  if(argc > 1) level = lua_tointeger(lua, 2);
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  serv->logger(level, msg, serv->logopq);
  lua_settop(lua, 0);
  return 0;
}


/* for _put function */
static int serv_put(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_put: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_put: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tculogadbput(serv->ulog, serv->sid, 0, serv->adb, kbuf, ksiz, vbuf, vsiz);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _putkeep function */
static int serv_putkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_putkeep: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_putkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tculogadbputkeep(serv->ulog, serv->sid, 0, serv->adb, kbuf, ksiz, vbuf, vsiz);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _putcat function */
static int serv_putcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_putcat: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_putcat: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tculogadbputcat(serv->ulog, serv->sid, 0, serv->adb, kbuf, ksiz, vbuf, vsiz);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _putout function */
static int serv_out(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_out: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_out: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tculogadbout(serv->ulog, serv->sid, 0, serv->adb, kbuf, ksiz);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _get function */
static int serv_get(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_get: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_get: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  int vsiz;
  char *vbuf = tcadbget(serv->adb, kbuf, ksiz, &vsiz);
  lua_settop(lua, 0);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for _vsiz function */
static int serv_vsiz(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_vsiz: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_vsiz: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  int vsiz = tcadbvsiz(serv->adb, kbuf, ksiz);
  lua_settop(lua, 0);
  lua_pushnumber(lua, vsiz);
  return 1;
}


/* for _iterinit function */
static int serv_iterinit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_iterinit: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tcadbiterinit(serv->adb);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _iternext function */
static int serv_iternext(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_iternext: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  int vsiz;
  char *vbuf = tcadbiternext(serv->adb, &vsiz);
  lua_settop(lua, 0);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for _fwmkeys function */
static int serv_fwmkeys(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "_fwmkeys: invalid arguments");
    lua_error(lua);
  }
  size_t psiz;
  const char *pbuf = lua_tolstring(lua, 1, &psiz);
  if(!pbuf){
    lua_pushstring(lua, "_fwmkeys: invalid arguments");
    lua_error(lua);
  }
  int max = argc > 1 && lua_isnumber(lua, 2) ? lua_tonumber(lua, 2) : -1;
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  TCLIST *keys = tcadbfwmkeys(serv->adb, pbuf, psiz, max);
  lua_settop(lua, 0);
  int knum = tclistnum(keys);
  lua_createtable(lua, knum, 0);
  for(int i = 0; i < knum; i++){
    int ksiz;
    const char *kbuf = tclistval(keys, i, &ksiz);
    lua_pushlstring(lua, kbuf, ksiz);
    lua_rawseti(lua, 1, i + 1);
  }
  tclistdel(keys);
  return 1;
}


/* for _addint function */
static int serv_addint(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_addint: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  int num = lua_tonumber(lua, 2);
  if(!kbuf || !lua_isnumber(lua, 2)){
    lua_pushstring(lua, "_addint: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  int rv = tculogadbaddint(serv->ulog, serv->sid, 0, serv->adb, kbuf, ksiz, num);
  lua_settop(lua, 0);
  if(rv == INT_MIN){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, rv);
  }
  return 1;
}


/* for _adddouble function */
static int serv_adddouble(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_adddouble: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  double num = lua_tonumber(lua, 2);
  if(!kbuf || !lua_isnumber(lua, 2)){
    lua_pushstring(lua, "_adddouble: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  double rv = tculogadbadddouble(serv->ulog, serv->sid, 0, serv->adb, kbuf, ksiz, num);
  lua_settop(lua, 0);
  if(isnan(rv)){
    lua_pushnil(lua);
  } else {
    lua_pushnumber(lua, rv);
  }
  return 1;
}


/* for _vanish function */
static int serv_vanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_vanish: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tculogadbvanish(serv->ulog, serv->sid, 0, serv->adb);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _rnum function */
static int serv_rnum(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_rnum: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  uint64_t rnum = tcadbrnum(serv->adb);
  lua_settop(lua, 0);
  lua_pushnumber(lua, rnum);
  return 1;
}


/* for _size function */
static int serv_size(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_size: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  uint64_t size = tcadbsize(serv->adb);
  lua_settop(lua, 0);
  lua_pushnumber(lua, size);
  return 1;
}


/* for _misc function */
static int serv_misc(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "_misc: invalid arguments");
    lua_error(lua);
  }
  const char *name = lua_tostring(lua, 1);
  if(!name){
    lua_pushstring(lua, "_misc: invalid arguments");
    lua_error(lua);
  }
  bool ulog = true;
  if(*name == '$'){
    name++;
    ulog = false;
  }
  TCLIST *args = tclistnew();
  for(int i = 2; i <= argc; i++){
    const char *aptr;
    size_t asiz;
    int len;
    switch(lua_type(lua, i)){
      case LUA_TNUMBER:
      case LUA_TSTRING:
        aptr = lua_tolstring(lua, i, &asiz);
        tclistpush(args, aptr, asiz);
        break;
      case LUA_TTABLE:
        len = lua_objlen(lua, i);
        for(int j = 1; j <= len; j++){
          lua_rawgeti(lua, i, j);
          switch(lua_type(lua, -1)){
            case LUA_TNUMBER:
            case LUA_TSTRING:
              aptr = lua_tolstring(lua, -1, &asiz);
              tclistpush(args, aptr, asiz);
              break;
          }
          lua_pop(lua, 1);
        }
        break;
    }
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  TCLIST *res = ulog ? tculogadbmisc(serv->ulog, serv->sid, 0, serv->adb, name, args) :
    tcadbmisc(serv->adb, name, args);
  lua_settop(lua, 0);
  if(res){
    int rnum = tclistnum(res);
    lua_createtable(lua, rnum, 0);
    for(int i = 0; i < rnum; i++){
      int rsiz;
      const char *rbuf = tclistval(res, i, &rsiz);
      lua_pushlstring(lua, rbuf, rsiz);
      lua_rawseti(lua, 1, i + 1);
    }
    tclistdel(res);
  } else {
    lua_pushnil(lua);
  }
  tclistdel(args);
  return 1;
}


/* for _foreach function */
static int serv_foreach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_foreach: invalid arguments");
    lua_error(lua);
  }
  if(!lua_isfunction(lua, 1)){
    lua_pushstring(lua, "_foreach: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  lua_pushvalue(lua, 1);
  lua_setglobal(lua, ITERVAR);
  bool err = false;
  if(!tcadbforeach(serv->adb, (TCITER)iterrec, lua)) err = true;
  lua_pushnil(lua);
  lua_setglobal(lua, ITERVAR);
  lua_settop(lua, 0);
  lua_pushboolean(lua, !err);
  return 1;
}


/* for _mapreduce function */
static int serv_mapreduce(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "_mapreduce: invalid arguments");
    lua_error(lua);
  }
  if(!lua_isfunction(lua, 1) || !lua_isfunction(lua, 2)){
    lua_pushstring(lua, "_mapreduce: invalid arguments");
    lua_error(lua);
  }
  lua_pushvalue(lua, 1);
  lua_setglobal(lua, MRMAPVAR);
  lua_pushvalue(lua, 2);
  lua_setglobal(lua, MRREDVAR);
  TCLIST *keys = NULL;
  if(argc > 2){
    const char *kbuf;
    size_t ksiz;
    int len;
    switch(lua_type(lua, 3)){
      case LUA_TNUMBER:
      case LUA_TSTRING:
        keys = tclistnew2(1);
        kbuf = lua_tolstring(lua, 3, &ksiz);
        tclistpush(keys, kbuf, ksiz);
        break;
      case LUA_TTABLE:
        len = lua_objlen(lua, 3);
        keys = tclistnew2(len);
        for(int i = 1; i <= len; i++){
          lua_rawgeti(lua, 3, i);
          switch(lua_type(lua, -1)){
            case LUA_TNUMBER:
            case LUA_TSTRING:
              kbuf = lua_tolstring(lua, -1, &ksiz);
              tclistpush(keys, kbuf, ksiz);
              break;
          }
          lua_pop(lua, 1);
        }
        break;
    }
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool err = false;
  TCBDB *bdb = tcbdbnew();
  lua_getglobal(lua, "_tmpdir_");
  const char *tmpdir = lua_tostring(lua, -1);
  if(!tmpdir) tmpdir = "/tmp";
  char *path = tcsprintf("%s%c%s-%d-%u",
                         tmpdir, MYPATHCHR, "mapbdb", getpid(), (unsigned int)(tctime() * 1000));
  unlink(path);
  if(!tcbdbopen(bdb, path, BDBOWRITER | BDBOCREAT | BDBOTRUNC)) err = true;
  unlink(path);
  tcfree(path);
  if(!tcadbmapbdb(serv->adb, keys, bdb, (ADBMAPPROC)maprec, lua, -1)) err = true;
  if(!err){
    BDBCUR *cur = tcbdbcurnew(bdb);
    tcbdbcurfirst(cur);
    const char *lbuf = NULL;
    int lsiz = 0;
    int lnum = 0;
    const char *kbuf;
    int ksiz;
    while(!err && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      int vsiz;
      const char *vbuf = tcbdbcurval3(cur, &vsiz);
      if(lbuf && lsiz == ksiz && !memcmp(lbuf, kbuf, lsiz)){
        lua_pushlstring(lua, vbuf, vsiz);
        lua_rawseti(lua, -2, ++lnum);
      } else {
        if(lbuf){
          if(lua_pcall(lua, 2, 1, 0) != 0){
            reporterror(lua);
            err = true;
          } else if(lua_gettop(lua) < 1 || !lua_toboolean(lua, 1)){
            err = true;
          }
        }
        lua_settop(lua, 0);
        lua_getglobal(lua, MRREDVAR);
        lua_pushlstring(lua, kbuf, ksiz);
        lua_newtable(lua);
        lnum = 1;
        lua_pushlstring(lua, vbuf, vsiz);
        lua_rawseti(lua, -2, lnum);
      }
      lbuf = kbuf;
      lsiz = ksiz;
      tcbdbcurnext(cur);
    }
    if(lbuf){
      if(lua_pcall(lua, 2, 1, 0) != 0){
        reporterror(lua);
        err = true;
      } else if(lua_gettop(lua) < 1 || !lua_toboolean(lua, 1)){
        err = true;
      }
      lua_settop(lua, 0);
    }
    tcbdbcurdel(cur);
  }
  if(!tcbdbclose(bdb)) err = true;
  tcbdbdel(bdb);
  if(keys) tclistdel(keys);
  lua_pushnil(lua);
  lua_setglobal(lua, MRREDVAR);
  lua_pushnil(lua);
  lua_setglobal(lua, MRMAPVAR);
  lua_settop(lua, 0);
  lua_pushboolean(lua, !err);
  return 1;
}


/* for _mapreduce function */
static int serv_mapreducemapemit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_mapreducemapemit: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_mapreducemapemit: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, MRPOOLVAR);
  void *map = lua_touserdata(lua, -1);
  bool rv = tcadbmapbdbemit(map, kbuf, ksiz, vbuf, vsiz);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _stashput function */
static int serv_stashput(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_stashput: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_stashput: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  tcmdbput(serv->stash, kbuf, ksiz, vbuf, vsiz);
  lua_pushboolean(lua, true);
  return 1;
}


/* for _stashputkeep function */
static int serv_stashputkeep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_stashputkeep: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_stashputkeep: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  tcmdbputkeep(serv->stash, kbuf, ksiz, vbuf, vsiz);
  lua_pushboolean(lua, true);
  return 1;
}


/* for _stashputcat function */
static int serv_stashputcat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_stashputcat: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  size_t vsiz;
  const char *vbuf = lua_tolstring(lua, 2, &vsiz);
  if(!kbuf || !vbuf){
    lua_pushstring(lua, "_stashputcat: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  tcmdbputcat(serv->stash, kbuf, ksiz, vbuf, vsiz);
  lua_pushboolean(lua, true);
  return 1;
}


/* for _stashout function */
static int serv_stashout(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_stashout: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_stashout: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tcmdbout(serv->stash, kbuf, ksiz);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _stashget function */
static int serv_stashget(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_stashget: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_stashget: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  int vsiz;
  char *vbuf = tcmdbget(serv->stash, kbuf, ksiz, &vsiz);
  if(vbuf){
    lua_pushlstring(lua, vbuf, vsiz);
    tcfree(vbuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for _stashvanish function */
static int serv_stashvanish(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_stashvanish: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  tcmdbvanish(serv->stash);
  return 0;
}


/* for _stashforeach function */
static int serv_stashforeach(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_stashforeach: invalid arguments");
    lua_error(lua);
  }
  if(!lua_isfunction(lua, 1)){
    lua_pushstring(lua, "_stashforeach: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  lua_pushvalue(lua, 1);
  lua_setglobal(lua, ITERVAR);
  tcmdbforeach(serv->stash, (TCITER)iterrec, lua);
  lua_pushnil(lua);
  lua_setglobal(lua, ITERVAR);
  return 0;
}


/* for _lock function */
static int serv_lock(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_lock: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_lock: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = true;
  while(!tcmdbputkeep(serv->lock, kbuf, ksiz, "", 0)){
    tcsleep(0.1);
    if(serv->term){
      rv = false;
      break;
    }
  }
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _unlock function */
static int serv_unlock(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_unlock: invalid arguments");
    lua_error(lua);
  }
  size_t ksiz;
  const char *kbuf = lua_tolstring(lua, 1, &ksiz);
  if(!kbuf){
    lua_pushstring(lua, "_unlock: invalid arguments");
    lua_error(lua);
  }
  lua_getglobal(lua, SERVVAR);
  SERV *serv = lua_touserdata(lua, -1);
  bool rv = tcmdbout(serv->lock, kbuf, ksiz);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _pack function */
static int serv_pack(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "_pack: invalid arguments");
    lua_error(lua);
  }
  const char *format = lua_tostring(lua, 1);
  if(!format){
    lua_pushstring(lua, "_pack: invalid arguments");
    lua_error(lua);
  }
  lua_newtable(lua);
  int aidx = argc + 1;
  int eidx = 1;
  for(int i = 2; i <= argc; i++){
    int len;
    switch(lua_type(lua, i)){
      case LUA_TNUMBER:
      case LUA_TSTRING:
        lua_pushvalue(lua, i);
        lua_rawseti(lua, aidx, eidx++);
        break;
      case LUA_TTABLE:
        len = lua_objlen(lua, i);
        for(int j = 1; j <= len; j++){
          lua_rawgeti(lua, i, j);
          lua_rawseti(lua, aidx, eidx++);
        }
        break;
      default:
        lua_pushnumber(lua, 0);
        lua_rawseti(lua, aidx, eidx++);
        break;
    }
  }
  lua_replace(lua, 2);
  lua_settop(lua, 2);
  TCXSTR *xstr = tcxstrnew();
  int emax = eidx - 1;
  eidx = 1;
  while(*format != '\0'){
    int c = *format;
    int loop = 1;
    if(format[1] == '*'){
      loop = INT_MAX;
      format++;
    } else if(format[1] >= '0' && format[1] <= '9'){
      format++;
      loop = 0;
      while(*format >= '0' && *format <= '9'){
        loop = loop * 10 + *format - '0';
        format++;
      }
      format--;
    }
    loop = tclmin(loop, emax);
    int end = tclmin(eidx + loop - 1, emax);
    while(eidx <= end){
      lua_rawgeti(lua, 2, eidx);
      double num = lua_tonumber(lua, 3);
      lua_pop(lua, 1);
      uint8_t cnum;
      uint16_t snum;
      uint32_t inum;
      uint64_t lnum;
      double dnum;
      float fnum;
      uint64_t wnum;
      char wbuf[TTNUMBUFSIZ], *wp;
      switch(c){
        case 'c':
        case 'C':
          cnum = num;
          tcxstrcat(xstr, &cnum, sizeof(cnum));
          break;
        case 's':
        case 'S':
          snum = num;
          tcxstrcat(xstr, &snum, sizeof(snum));
          break;
        case 'i':
        case 'I':
          inum = num;
          tcxstrcat(xstr, &inum, sizeof(inum));
          break;
        case 'l':
        case 'L':
          lnum = num;
          tcxstrcat(xstr, &lnum, sizeof(lnum));
          break;
        case 'f':
        case 'F':
          fnum = num;
          tcxstrcat(xstr, &fnum, sizeof(fnum));
          break;
        case 'd':
        case 'D':
          dnum = num;
          tcxstrcat(xstr, &dnum, sizeof(dnum));
          break;
        case 'n':
          snum = num;
          snum = TTHTONS(snum);
          tcxstrcat(xstr, &snum, sizeof(snum));
          break;
        case 'N':
          inum = num;
          inum = TTHTONL(inum);
          tcxstrcat(xstr, &inum, sizeof(inum));
          break;
        case 'M':
          lnum = num;
          lnum = TTHTONLL(lnum);
          tcxstrcat(xstr, &lnum, sizeof(lnum));
          break;
        case 'w':
        case 'W':
          wnum = num;
          wp = wbuf;
          if(wnum < (1ULL << 7)){
            *(wp++) = wnum;
          } else if(wnum < (1ULL << 14)){
            *(wp++) = (wnum >> 7) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if(wnum < (1ULL << 21)){
            *(wp++) = (wnum >> 14) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if(wnum < (1ULL << 28)){
            *(wp++) = (wnum >> 21) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if(wnum < (1ULL << 35)){
            *(wp++) = (wnum >> 28) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if(wnum < (1ULL << 42)){
            *(wp++) = (wnum >> 35) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if(wnum < (1ULL << 49)){
            *(wp++) = (wnum >> 42) | 0x80;
            *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else if(wnum < (1ULL << 56)){
            *(wp++) = (wnum >> 49) | 0x80;
            *(wp++) = ((wnum >> 42) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          } else {
            *(wp++) = (wnum >> 63) | 0x80;
            *(wp++) = ((wnum >> 49) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 42) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 35) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 28) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 21) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 14) & 0x7f) | 0x80;
            *(wp++) = ((wnum >> 7) & 0x7f) | 0x80;
            *(wp++) = wnum & 0x7f;
          }
          tcxstrcat(xstr, wbuf, wp - wbuf);
          break;
      }
      eidx++;
    }
    format++;
    if(eidx > emax) break;
  }
  lua_settop(lua, 0);
  lua_pushlstring(lua, tcxstrptr(xstr), tcxstrsize(xstr));
  tcxstrdel(xstr);
  return 1;
}


/* for _unpack function */
static int serv_unpack(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_unpack: invalid arguments");
    lua_error(lua);
  }
  const char *format = lua_tostring(lua, 1);
  size_t size;
  const char *buf = lua_tolstring(lua, 2, &size);
  if(!format){
    lua_pushstring(lua, "_unpack: invalid arguments");
    lua_error(lua);
  }
  if(!buf){
    buf = "";
    size = 0;
  }
  lua_newtable(lua);
  const char *rp = buf;
  int eidx = 1;
  while(*format != '\0'){
    int c = *format;
    int loop = 1;
    if(format[1] == '*'){
      loop = INT_MAX;
      format++;
    } else if(format[1] >= '0' && format[1] <= '9'){
      format++;
      loop = 0;
      while(*format >= '0' && *format <= '9'){
        loop = loop * 10 + *format - '0';
        format++;
      }
      format--;
    }
    loop = tclmin(loop, size);
    for(int i = 0; i < loop && size > 0; i++){
      uint8_t cnum;
      uint16_t snum;
      uint32_t inum;
      uint64_t lnum;
      float fnum;
      double dnum;
      uint64_t wnum;
      switch(c){
        case 'c':
          if(size >= sizeof(cnum)){
            memcpy(&cnum, rp, sizeof(cnum));
            lua_pushnumber(lua, (int8_t)cnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(cnum);
            size -= sizeof(cnum);
          } else {
            size = 0;
          }
          break;
        case 'C':
          if(size >= sizeof(cnum)){
            memcpy(&cnum, rp, sizeof(cnum));
            lua_pushnumber(lua, (uint8_t)cnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(cnum);
            size -= sizeof(cnum);
          } else {
            size = 0;
          }
          break;
        case 's':
          if(size >= sizeof(snum)){
            memcpy(&snum, rp, sizeof(snum));
            lua_pushnumber(lua, (int16_t)snum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(snum);
            size -= sizeof(snum);
          } else {
            size = 0;
          }
          break;
        case 'S':
          if(size >= sizeof(snum)){
            memcpy(&snum, rp, sizeof(snum));
            lua_pushnumber(lua, (uint16_t)snum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(snum);
            size -= sizeof(snum);
          } else {
            size = 0;
          }
          break;
        case 'i':
          if(size >= sizeof(inum)){
            memcpy(&inum, rp, sizeof(inum));
            lua_pushnumber(lua, (int32_t)inum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(inum);
            size -= sizeof(inum);
          } else {
            size = 0;
          }
          break;
        case 'I':
          if(size >= sizeof(inum)){
            memcpy(&inum, rp, sizeof(inum));
            lua_pushnumber(lua, (uint32_t)inum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(inum);
            size -= sizeof(inum);
          } else {
            size = 0;
          }
          break;
        case 'l':
          if(size >= sizeof(lnum)){
            memcpy(&lnum, rp, sizeof(lnum));
            lua_pushnumber(lua, (int64_t)lnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(lnum);
            size -= sizeof(lnum);
          } else {
            size = 0;
          }
          break;
        case 'L':
          if(size >= sizeof(lnum)){
            memcpy(&lnum, rp, sizeof(lnum));
            lua_pushnumber(lua, (uint64_t)lnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(lnum);
            size -= sizeof(lnum);
          } else {
            size = 0;
          }
          break;
        case 'f':
        case 'F':
          if(size >= sizeof(fnum)){
            memcpy(&fnum, rp, sizeof(fnum));
            lua_pushnumber(lua, (float)fnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(fnum);
            size -= sizeof(fnum);
          } else {
            size = 0;
          }
          break;
        case 'd':
        case 'D':
          if(size >= sizeof(dnum)){
            memcpy(&dnum, rp, sizeof(dnum));
            lua_pushnumber(lua, (double)dnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(dnum);
            size -= sizeof(dnum);
          } else {
            size = 0;
          }
          break;
        case 'n':
          if(size >= sizeof(snum)){
            memcpy(&snum, rp, sizeof(snum));
            snum = TTNTOHS(snum);
            lua_pushnumber(lua, (uint16_t)snum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(snum);
            size -= sizeof(snum);
          } else {
            size = 0;
          }
          break;
        case 'N':
          if(size >= sizeof(inum)){
            memcpy(&inum, rp, sizeof(inum));
            inum = TTNTOHL(inum);
            lua_pushnumber(lua, (uint32_t)inum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(inum);
            size -= sizeof(inum);
          } else {
            size = 0;
          }
          break;
        case 'M':
          if(size >= sizeof(lnum)){
            memcpy(&lnum, rp, sizeof(lnum));
            lnum = TTNTOHLL(lnum);
            lua_pushnumber(lua, (uint64_t)lnum);
            lua_rawseti(lua, 3, eidx++);
            rp += sizeof(lnum);
            size -= sizeof(lnum);
          } else {
            size = 0;
          }
          break;
        case 'w':
        case 'W':
          wnum = 0;
          do {
            inum = *(unsigned char *)rp;
            wnum = wnum * 0x80 + (inum & 0x7f);
            rp++;
            size--;
          } while(inum >= 0x80 && size > 0);
          lua_pushnumber(lua, wnum);
          lua_rawseti(lua, 3, eidx++);
          break;
      }
    }
    format++;
    if(size < 1) break;
  }
  lua_replace(lua, 1);
  lua_settop(lua, 1);
  return 1;
}


/* for _split function */
static int serv_split(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 1){
    lua_pushstring(lua, "_split: invalid arguments");
    lua_error(lua);
  }
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 1, &isiz);
  if(!ibuf){
    lua_pushstring(lua, "_split: invalid arguments");
    lua_error(lua);
  }
  const char *delims = argc > 1 ? lua_tostring(lua, 2) : NULL;
  lua_newtable(lua);
  int lnum = 1;
  if(delims){
    const char *str = ibuf;
    while(true){
      const char *sp = str;
      while(*str != '\0' && !strchr(delims, *str)){
        str++;
      }
      lua_pushlstring(lua, sp, str - sp);
      lua_rawseti(lua, -2, lnum++);
      if(*str == '\0') break;
      str++;
    }
  } else {
    const char *ptr = ibuf;
    int size = isiz;
    while(size >= 0){
      const char *rp = ptr;
      const char *ep = ptr + size;
      while(rp < ep){
        if(*rp == '\0') break;
        rp++;
      }
      lua_pushlstring(lua, ptr, rp - ptr);
      lua_rawseti(lua, -2, lnum++);
      rp++;
      size -= rp - ptr;
      ptr = rp;
    }
  }
  lua_replace(lua, 1);
  lua_settop(lua, 1);
  return 1;
}


/* for _codec function */
static int serv_codec(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_codec: invalid arguments");
    lua_error(lua);
  }
  const char *mode = lua_tostring(lua, 1);
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 2, &isiz);
  if(!mode || !ibuf){
    lua_pushstring(lua, "_codec: invalid arguments");
    lua_error(lua);
  }
  char *obuf = NULL;
  int osiz = 0;
  if(*mode == '~'){
    mode++;
    if(!tcstricmp(mode, "url")){
      obuf = tcurldecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "base")){
      obuf = tcbasedecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "quote")){
      obuf = tcquotedecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "hex")){
      obuf = tchexdecode(ibuf, &osiz);
    } else if(!tcstricmp(mode, "pack")){
      obuf = tcpackdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "tcbs")){
      obuf = tcbsdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "deflate")){
      obuf = tcinflate(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "gzip")){
      obuf = tcgzipdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "bzip")){
      obuf = tcbzipdecode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "xml")){
      obuf = tcxmlunescape(ibuf);
      osiz = obuf ? strlen(obuf) : 0;
    }
  } else {
    if(!tcstricmp(mode, "url")){
      obuf = tcurlencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "base")){
      obuf = tcbaseencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "quote")){
      obuf = tcquoteencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "hex")){
      obuf = tchexencode(ibuf, isiz);
      osiz = obuf ? strlen(obuf) : 0;
    } else if(!tcstricmp(mode, "pack")){
      obuf = tcpackencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "tcbs")){
      obuf = tcbsencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "deflate")){
      obuf = tcdeflate(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "gzip")){
      obuf = tcgzipencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "bzip")){
      obuf = tcbzipencode(ibuf, isiz, &osiz);
    } else if(!tcstricmp(mode, "xml")){
      obuf = tcxmlescape(ibuf);
      osiz = obuf ? strlen(obuf) : 0;
    }
  }
  lua_settop(lua, 0);
  if(obuf){
    lua_pushlstring(lua, obuf, osiz);
    tcfree(obuf);
  } else {
    lua_pushnil(lua);
  }
  return 1;
}


/* for _hash function */
static int serv_hash(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 2){
    lua_pushstring(lua, "_hash: invalid arguments");
    lua_error(lua);
  }
  const char *mode = lua_tostring(lua, 1);
  size_t isiz;
  const char *ibuf = lua_tolstring(lua, 2, &isiz);
  if(!mode || !ibuf){
    lua_pushstring(lua, "_hash: invalid arguments");
    lua_error(lua);
  }
  if(!tcstricmp(mode, "md5")){
    char obuf[48];
    tcmd5hash(ibuf, isiz, obuf);
    lua_settop(lua, 0);
    lua_pushstring(lua, obuf);
  } else if(!tcstricmp(mode, "md5raw")){
    char obuf[48];
    tcmd5hash(ibuf, isiz, obuf);
    int esiz;
    char *ebuf = tchexdecode(obuf, &esiz);
    lua_settop(lua, 0);
    lua_pushlstring(lua, ebuf, esiz);
    tcfree(ebuf);
  } else if(!tcstricmp(mode, "crc32")){
    uint32_t crc = tcgetcrc(ibuf, isiz);
    lua_settop(lua, 0);
    lua_pushnumber(lua, crc);
  } else {
    lua_settop(lua, 0);
    lua_pushnil(lua);
  }
  return 1;
}


/* for _bit function */
static int serv_bit(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "_bit: invalid arguments");
    lua_error(lua);
  }
  const char *mode = lua_tostring(lua, 1);
  uint32_t num = lua_tonumber(lua, 2);
  uint32_t aux = argc > 2 ? lua_tonumber(lua, 3) : 0;
  if(!mode){
    lua_pushstring(lua, "_bit: invalid arguments");
    lua_error(lua);
  } else if(!tcstricmp(mode, "and")){
    num &= aux;
  } else if(!tcstricmp(mode, "or")){
    num |= aux;
  } else if(!tcstricmp(mode, "xor")){
    num ^= aux;
  } else if(!tcstricmp(mode, "not")){
    num = ~num;
  } else if(!tcstricmp(mode, "left")){
    num <<= aux;
  } else if(!tcstricmp(mode, "right")){
    num >>= aux;
  } else {
    lua_pushstring(lua, "_bit: invalid arguments");
    lua_error(lua);
  }
  lua_settop(lua, 0);
  lua_pushnumber(lua, num);
  return 1;
}


/* for _strstr function */
static int serv_strstr(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "_strstr: invalid arguments");
    lua_error(lua);
  }
  const char *str = lua_tostring(lua, 1);
  const char *pat = lua_tostring(lua, 2);
  if(!str || !pat){
    lua_pushstring(lua, "_strstr: invalid arguments");
    lua_error(lua);
  }
  const char *alt = argc > 2 ? lua_tostring(lua, 3) : NULL;
  if(alt){
    TCXSTR *xstr = tcxstrnew();
    int plen = strlen(pat);
    int alen = strlen(alt);
    if(plen > 0){
      char *pv;
      while((pv = strstr(str, pat)) != NULL){
        tcxstrcat(xstr, str, pv - str);
        tcxstrcat(xstr, alt, alen);
        str = pv + plen;
      }
    }
    tcxstrcat2(xstr, str);
    lua_settop(lua, 0);
    lua_pushstring(lua, tcxstrptr(xstr));
    tcxstrdel(xstr);
  } else {
    char *pv = strstr(str, pat);
    if(pv){
      int idx = pv - str + 1;
      lua_settop(lua, 0);
      lua_pushinteger(lua, idx);
    } else {
      lua_settop(lua, 0);
      lua_pushinteger(lua, 0);
    }
  }
  return 1;
}


/* for _regex function */
static int serv_regex(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "_regex: invalid arguments");
    lua_error(lua);
  }
  const char *str = lua_tostring(lua, 1);
  const char *regex = lua_tostring(lua, 2);
  if(!str || !regex){
    lua_pushstring(lua, "_regex: invalid arguments");
    lua_error(lua);
  }
  const char *alt = argc > 2 ? lua_tostring(lua, 3) : NULL;
  if(alt){
    char *res = tcregexreplace(str, regex, alt);
    lua_settop(lua, 0);
    lua_pushstring(lua, res);
    tcfree(res);
  } else {
    if(tcregexmatch(str, regex)){
      lua_settop(lua, 0);
      lua_pushboolean(lua, true);
    } else {
      lua_settop(lua, 0);
      lua_pushboolean(lua, false);
    }
  }
  return 1;
}


/* for _ucs function */
static int serv_ucs(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "ucs: invalid arguments");
    lua_error(lua);
  }
  if(lua_type(lua, 1) == LUA_TTABLE){
    int anum = lua_objlen(lua, 1);
    uint16_t *ary = tcmalloc(sizeof(*ary) * anum + 1);
    for(int i = 1; i <= anum; i++){
      lua_rawgeti(lua, 1, i);
      ary[i-1] = lua_tointeger(lua, 2);
      lua_pop(lua, 1);
    }
    char *str = tcmalloc(anum * 3 + 1);
    tcstrucstoutf(ary, anum, str);
    lua_settop(lua, 0);
    lua_pushstring(lua, str);
    tcfree(str);
    tcfree(ary);
  } else {
    size_t len;
    const char *str = lua_tolstring(lua, 1, &len);
    if(!str){
      lua_pushstring(lua, "ucs: invalid arguments");
      lua_error(lua);
    }
    uint16_t *ary = tcmalloc(sizeof(*ary) * len + 1);
    int anum;
    tcstrutftoucs(str, ary, &anum);
    lua_settop(lua, 0);
    lua_createtable(lua, anum, 0);
    for(int i = 0; i < anum; i++){
      lua_pushinteger(lua, ary[i]);
      lua_rawseti(lua, 1, i + 1);
    }
    tcfree(ary);
  }
  return 1;
}


/* for _dist function */
static int serv_dist(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc < 2){
    lua_pushstring(lua, "dist: invalid arguments");
    lua_error(lua);
  }
  const char *astr = lua_tostring(lua, 1);
  const char *bstr = lua_tostring(lua, 2);
  bool utf = argc > 2 ? lua_toboolean(lua, 3) : false;
  if(!astr || !bstr){
    lua_pushstring(lua, "dist: invalid arguments");
    lua_error(lua);
  }
  int rv = utf ? tcstrdistutf(astr, bstr) : tcstrdist(astr, bstr);
  lua_settop(lua, 0);
  lua_pushnumber(lua, rv);
  return 1;
}


/* for _isect function */
static int serv_isect(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc == 1 && lua_type(lua, 1) == LUA_TTABLE){
    int len = lua_objlen(lua, 1);
    for(int i = 1; i <= len; i++){
      lua_rawgeti(lua, 1, i);
      if(lua_type(lua, -1) == LUA_TTABLE){
        argc++;
      } else {
        lua_pop(lua, 1);
        break;
      }
    }
    if(argc > 1){
      lua_remove(lua, 1);
      argc--;
    }
  }
  int tnum = 0;
  int rnum = 0;
  for(int i = 1; i <= argc; i++){
    if(lua_type(lua, i) != LUA_TTABLE) continue;
    int len = lua_objlen(lua, i);
    if(len < 1){
      lua_settop(lua, 0);
      lua_newtable(lua);
      return 1;
    }
    tnum++;
    rnum += len;
  }
  if(tnum == 2){
    TCMAP *former = NULL;
    TCMAP *latter = NULL;
    for(int i = 1; i <= argc; i++){
      if(lua_type(lua, i) != LUA_TTABLE) continue;
      int len = lua_objlen(lua, i);
      if(former){
        latter = tcmapnew2(tclmin(len, tcmaprnum(former)));
        for(int j = 1; j <= len; j++){
          lua_rawgeti(lua, i, j);
          size_t size;
          const char *ptr = lua_tolstring(lua, -1, &size);
          if(ptr){
            int vsiz;
            if(tcmapget(former, ptr, size, &vsiz)) tcmapput(latter, ptr, size, "", 0);
          }
          lua_pop(lua, 1);
        }
        break;
      } else {
        former = tcmapnew2(len);
        for(int j = 1; j <= len; j++){
          lua_rawgeti(lua, i, j);
          size_t size;
          const char *ptr = lua_tolstring(lua, -1, &size);
          if(ptr) tcmapput(former, ptr, size, "", 0);
          lua_pop(lua, 1);
        }
      }
    }
    lua_settop(lua, 0);
    if(latter){
      lua_createtable(lua, (int)tcmaprnum(latter), 0);
      tcmapiterinit(latter);
      int ridx = 1;
      const char *kbuf;
      int ksiz;
      while((kbuf = tcmapiternext(latter, &ksiz)) != NULL){
        lua_pushlstring(lua, kbuf, ksiz);
        lua_rawseti(lua, 1, ridx++);
      }
      tcmapdel(latter);
    } else {
      lua_newtable(lua);
    }
    if(former) tcmapdel(former);
  } else {
    TCMAP *freq = tcmapnew2(rnum);
    for(int i = 1; i <= argc; i++){
      if(lua_type(lua, i) != LUA_TTABLE) continue;
      int len = lua_objlen(lua, i);
      TCMAP *uniq = tcmapnew2(len);
      for(int j = 1; j <= len; j++){
        lua_rawgeti(lua, i, j);
        size_t size;
        const char *ptr = lua_tolstring(lua, -1, &size);
        if(ptr){
          int vsiz;
          if(!tcmapget(uniq, ptr, size, &vsiz)){
            tcmapaddint(freq, ptr, size, 1);
            tcmapput(uniq, ptr, size, "", 0);
          }
        }
        lua_pop(lua, 1);
      }
      tcmapdel(uniq);
    }
    lua_settop(lua, 0);
    lua_createtable(lua, (int)tcmaprnum(freq), 0);
    tcmapiterinit(freq);
    int ridx = 1;
    const char *kbuf;
    int ksiz;
    while((kbuf = tcmapiternext(freq, &ksiz)) != NULL){
      int vsiz;
      const char *vbuf = tcmapiterval(kbuf, &vsiz);
      int num = *(int *)vbuf;
      if(num != tnum) continue;
      lua_pushlstring(lua, kbuf, ksiz);
      lua_rawseti(lua, 1, ridx++);
    }
    tcmapdel(freq);
  }
  return 1;
}


/* for _union function */
static int serv_union(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc == 1 && lua_type(lua, 1) == LUA_TTABLE){
    int len = lua_objlen(lua, 1);
    for(int i = 1; i <= len; i++){
      lua_rawgeti(lua, 1, i);
      if(lua_type(lua, -1) == LUA_TTABLE){
        argc++;
      } else {
        lua_pop(lua, 1);
        break;
      }
    }
    if(argc > 1){
      lua_remove(lua, 1);
      argc--;
    }
  }
  int rnum = 0;
  for(int i = 1; i <= argc; i++){
    if(lua_type(lua, i) != LUA_TTABLE) continue;
    rnum += lua_objlen(lua, i);
  }
  TCMAP *result = tcmapnew2(rnum);
  for(int i = 1; i <= argc; i++){
    if(lua_type(lua, i) != LUA_TTABLE) continue;
    int len = lua_objlen(lua, i);
    for(int j = 1; j <= len; j++){
      lua_rawgeti(lua, i, j);
      size_t size;
      const char *ptr = lua_tolstring(lua, -1, &size);
      if(ptr) tcmapput(result, ptr, size, "", 0);
      lua_pop(lua, 1);
    }
  }
  lua_settop(lua, 0);
  lua_createtable(lua, (int)tcmaprnum(result), 0);
  tcmapiterinit(result);
  int ridx = 1;
  const char *kbuf;
  int ksiz;
  while((kbuf = tcmapiternext(result, &ksiz)) != NULL){
    lua_pushlstring(lua, kbuf, ksiz);
    lua_rawseti(lua, 1, ridx++);
  }
  tcmapdel(result);
  return 1;
}


/* for _time function */
static int serv_time(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 0){
    lua_pushstring(lua, "_time: invalid arguments");
    lua_error(lua);
  }
  lua_settop(lua, 0);
  lua_pushnumber(lua, tctime());
  return 1;
}


/* for _sleep function */
static int serv_sleep(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_sleep: invalid arguments");
    lua_error(lua);
  }
  double sec = lua_tonumber(lua, 1);
  if(!lua_isnumber(lua, 1)){
    lua_pushstring(lua, "_sleep: invalid arguments");
    lua_error(lua);
  }
  lua_settop(lua, 0);
  lua_pushboolean(lua, tcsleep(sec));
  return 1;
}


/* for stat function */
static int serv_stat(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_stat: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "_stat: invalid arguments");
    lua_error(lua);
  }
  struct stat sbuf;
  if(lstat(path, &sbuf) == 0){
    lua_settop(lua, 0);
    lua_newtable(lua);
    lua_pushnumber(lua, sbuf.st_dev);
    lua_setfield(lua, -2, "dev");
    lua_pushnumber(lua, sbuf.st_ino);
    lua_setfield(lua, -2, "ino");
    lua_pushnumber(lua, sbuf.st_mode);
    lua_setfield(lua, -2, "mode");
    lua_pushnumber(lua, sbuf.st_nlink);
    lua_setfield(lua, -2, "nlink");
    lua_pushnumber(lua, sbuf.st_uid);
    lua_setfield(lua, -2, "uid");
    lua_pushnumber(lua, sbuf.st_gid);
    lua_setfield(lua, -2, "gid");
    lua_pushnumber(lua, sbuf.st_rdev);
    lua_setfield(lua, -2, "rdev");
    lua_pushnumber(lua, sbuf.st_size);
    lua_setfield(lua, -2, "size");
    lua_pushnumber(lua, sbuf.st_blksize);
    lua_setfield(lua, -2, "blksize");
    lua_pushnumber(lua, sbuf.st_blocks);
    lua_setfield(lua, -2, "blocks");
    lua_pushnumber(lua, sbuf.st_atime);
    lua_setfield(lua, -2, "atime");
    lua_pushnumber(lua, sbuf.st_mtime);
    lua_setfield(lua, -2, "mtime");
    lua_pushnumber(lua, sbuf.st_ctime);
    lua_setfield(lua, -2, "ctime");
    lua_pushboolean(lua, S_ISREG(sbuf.st_mode));
    lua_setfield(lua, -2, "_regular");
    lua_pushboolean(lua, S_ISDIR(sbuf.st_mode));
    lua_setfield(lua, -2, "_directory");
    bool readable = false;
    bool writable = false;
    bool executable = false;
    if(sbuf.st_uid == geteuid()){
      if(sbuf.st_mode & S_IRUSR) readable = true;
      if(sbuf.st_mode & S_IWUSR) writable = true;
      if(sbuf.st_mode & S_IXUSR) executable = true;
    }
    if(sbuf.st_gid == getegid()){
      if(sbuf.st_mode & S_IRGRP) readable = true;
      if(sbuf.st_mode & S_IWGRP) writable = true;
      if(sbuf.st_mode & S_IXGRP) executable = true;
    }
    if(sbuf.st_mode & S_IROTH) readable = true;
    if(sbuf.st_mode & S_IWOTH) writable = true;
    if(sbuf.st_mode & S_IXOTH) executable = true;
    lua_pushboolean(lua, readable);
    lua_setfield(lua, -2, "_readable");
    lua_pushboolean(lua, writable);
    lua_setfield(lua, -2, "_writable");
    lua_pushboolean(lua, executable);
    lua_setfield(lua, -2, "_executable");
    char *rpath = tcrealpath(path);
    if(rpath){
      lua_pushstring(lua, rpath);
      lua_setfield(lua, -2, "_realpath");
      tcfree(rpath);
    }
  } else {
    lua_settop(lua, 0);
    lua_pushnil(lua);
  }
  return 1;
}


/* for _glob function */
static int serv_glob(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_glob: invalid arguments");
    lua_error(lua);
  }
  const char *pattern = lua_tostring(lua, 1);
  if(!pattern){
    lua_pushstring(lua, "_glob: invalid arguments");
    lua_error(lua);
  }
  TCLIST *paths = tcglobpat(pattern);
  int pnum = tclistnum(paths);
  lua_settop(lua, 0);
  lua_createtable(lua, pnum, 0);
  for(int i = 0; i < pnum; i++){
    int size;
    const char *buf = tclistval(paths, i, &size);
    lua_pushlstring(lua, buf, size);
    lua_rawseti(lua, -2, i + 1);
  }
  tclistdel(paths);
  return 1;
}


/* for _remove function */
static int serv_remove(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_remove: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "_remove: invalid arguments");
    lua_error(lua);
  }
  bool rv = tcremovelink(path);
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


/* for _mkdir function */
static int serv_mkdir(lua_State *lua){
  int argc = lua_gettop(lua);
  if(argc != 1){
    lua_pushstring(lua, "_mkdir: invalid arguments");
    lua_error(lua);
  }
  const char *path = lua_tostring(lua, 1);
  if(!path){
    lua_pushstring(lua, "_mkdir: invalid arguments");
    lua_error(lua);
  }
  bool rv = mkdir(path, 00755) == 0;
  lua_settop(lua, 0);
  lua_pushboolean(lua, rv);
  return 1;
}


#endif



// END OF FILE
