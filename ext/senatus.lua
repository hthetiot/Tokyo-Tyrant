--
-- Sample functions of the Lua extension of Tokyo Tyrant
--



-- echo back the key and the value
function echo(key, value)
   return key .. "\t" .. value .. "\n"
end


-- store a record
function put(key, value)
   if not _put(key, value) then
      return nil
   end
   return "ok"
end


-- store a new record
function putkeep(key, value)
   if not _putkeep(key, value) then
      return nil
   end
   return "ok"
end


-- concatenate a value at the end of the existing record
function putcat(key, value)
   if not _putcat(key, value) then
      return nil
   end
   return "ok"
end


-- remove a record
function out(key, value)
   if not _out(key) then
      return nil
   end
   return "ok"
end


-- retrieve a record
function get(key, value)
   return _get(key)
end


-- get the size of the value of a record
function vsiz(key, value)
   local vsiz = _vsiz(key)
   if vsiz < 0 then
      return nil
   end
   return vsiz
end


-- initialize the iterator
function iterinit(key, value)
   if not _iterinit() then
      return nil
   end
   return "ok"
end


-- get the next key of the iterator
function iternext(key, value)
   return _iternext()
end


-- get forward matching keys
function fwmkeys(key, value)
   value = tonumber(value)
   local keys = _fwmkeys(key, value)
   local res = ""
   for i = 1, #keys do
      res = res .. keys[i] .. "\n"
   end
   return res
end


-- add an integer to a record
function addint(key, value)
   value = tonumber(value)
   if not value then
      return nil
   end
   return _addint(key, value)
end


-- add a real number to a record
function adddouble(key, value)
   value = tonumber(value)
   if not value then
      return nil
   end
   return _adddouble(key, value)
end


-- remove all records
function vanish(key, value)
   if not _vanish() then
      return nil
   end
   return "ok"
end


-- get the number of records
function rnum(key, value)
   return _rnum()
end


-- get the size of the database
function size(key, value)
   return _size()
end


-- get records whose key matches a pattern
function find(key, value)
   local res = ""
   function proc(tkey, tvalue)
      if string.match(tkey, key) then
         res = res .. tkey .. "\t" .. tvalue .. "\n"
      end
      return true
   end
   _foreach(proc)
   return res
end


-- get records whose key matches a pattern
function finddist(key, value)
   value = tonumber(value)
   if not value then
      value = 0
   end
   local res = ""
   function proc(tkey, tvalue)
      if _dist(tkey, key) <= value then
         res = res .. tkey .. "\t" .. tvalue .. "\n"
      end
      return true
   end
   _foreach(proc)
   return res
end


-- store a record into tha stash
function stashput(key, value)
   if not _stashput(key, value) then
      return nil
   end
   return "ok"
end


-- remove a record of the stash
function stashout(key, value)
   if not _stashout(key) then
      return nil
   end
   return "ok"
end


-- retrieve a record in the stash
function stashget(key, value)
   return _stashget(key)
end


-- retrieve all records in the stash
function stashlist(key, value)
   key = tonumber(key)
   if not key or key < 0 then
      key = math.pow(2, 31)
   end
   local res = ""
   function proc(tkey, tvalue)
      if key < 1 then
        return false
      end
      key = key - 1
      res = res .. tkey .. "\t" .. tvalue .. "\n"
      return true
   end
   _stashforeach(proc)
   return res
end


-- lock an arbitrary key
function lock(key, value)
   if not _lock(key) then
      return nil
   end
   return "ok"
end


-- unlock an arbitrary key
function unlock(key, value)
   if not _unlock(key) then
      return nil
   end
   return "ok"
end


-- get the status information
function stat(key, value)
   local msg = ""
   msg = msg .. "version\t" .. _version .. "\n"
   msg = msg .. "time\t" .. string.format("%.6f", _time()) .. "\n"
   msg = msg .. "pid\t" .. _pid ..  "\n"
   msg = msg .. "sid\t" .. _sid .. "\n"
   msg = msg .. "thid\t" .. _thid .. "\n"
   msg = msg .. "rnum\t" .. _rnum() .. "\n"
   msg = msg .. "size\t" .. _size() .. "\n"
   return msg
end


-- increment the value as decimal number, store as string
function incr(key, value)
   value = tonumber(value)
   if not value then
      return nil
   end
   local old = tonumber(_get(key))
   if old then
      value = value + old
   end
   if not _put(key, value) then
      return nil
   end
   return value
end


-- increment the value as decimal number, store as int32 binary
function incrint32(key, value)
   value = tonumber(value)
   if not value then
      return nil
   end
   local old = _unpack("i", _get(key))
   if old and #old == 1 then
      value = value + old[1]
   end
   if not _put(key, _pack("i", value)) then
      return nil
   end
   return value
end


-- increment the value as decimal number, store as int64 binary
function incrint64(key, value)
   value = tonumber(value)
   if not value then
      return nil
   end
   local old = _unpack("l", _get(key))
   if old and #old == 1 then
      value = value + old[1]
   end
   if not _put(key, _pack("l", value)) then
      return nil
   end
   return value
end


-- increment the value as decimal number, store as double binary
function incrdouble(key, value)
   value = tonumber(value)
   if not value then
      return nil
   end
   local old = _unpack("d", _get(key))
   if old and #old == 1 then
      value = value + old[1]
   end
   if not _put(key, _pack("d", value)) then
      return nil
   end
   return value
end


-- call a versatile function of the misc function
function misc(key, value)
   local args = {}
   for arg in string.gmatch(value, "[^\t\n]+") do
      table.insert(args, arg)
   end
   local res = _misc(key, args)
   if not res then
      return nil
   end
   local rbuf = ""
   for i = 1, #res do
      rbuf = rbuf .. res[i] .. "\n"
   end
   return rbuf
end


-- split a string
function split(key, value)
   if #value < 1 then
      value = nil
   end
   local elems = _split(key, value)
   local res = ""
   for i = 1, #elems do
      res = res .. elems[i] .. "\n"
   end
   return res
end


-- encode or decode a string
function codec(key, value)
   if key == "ucs" then
      local ary = _ucs(value)
      return _pack("n*", ary)
   elseif key == "~ucs" then
      local ary = _unpack("n*", value)
      return _ucs(ary)
   end
   return _codec(key, value)
end


-- get the hash value
function hash(key, value)
   return _hash(key, value)
end


-- get the bitwise-and of two integers
function bitand(key, value)
   key = tonumber(key)
   value = tonumber(value)
   if not key or not value then
      return nil
   end
   return _bit("and", key, value)
end


-- check substring matching
function strstr(key, value)
   return tostring(_strstr(key, value))
end


-- check matching with regular expressions
function regex(key, value)
   return tostring(_regex(key, value))
end


-- calculate the edit distance
function dist(key, value)
   return _dist(key, value)
end


-- get the currnet time
function time(key, value)
   return string.format("%.6f",_time())
end


-- sleep and notice
function sleep(key, value)
   key = tonumber(key)
   if not key or key < 0 then
      return nil
   end
   _sleep(key)
   return "ok"
end


-- get status of a file
function statfile(key, value)
   local stat = _stat(key)
   if not stat then
      return nil
   end
   local res = ""
   for tkey, tval in pairs(stat) do
      res = res .. tkey .. "\t" .. tostring(tval) .. "\n"
   end
   return res
end


-- find pathnames matching a pattern
function glob(key, value)
   local paths = _glob(key)
   local res = ""
   for i = 1, #paths do
      res = res .. paths[i] .. "\n"
   end
   return res
end


-- remove a file or a directory and its sub ones recursively
function remove(key, value)
   if not _remove(key) then
      return nil
   end
   return "ok"
end


-- create a directory
function mkdir(key, value)
   if not _mkdir(key) then
      return nil
   end
   return "ok"
end


-- evaluate a string
function eval(key, value)
   if not _eval(key) then
      return nil
   end
   return "ok"
end


-- log a string
function log(key, value)
   local level = tonumber(value)
   if not level then
      level = 1
   end
   _log(key, level)
   return "ok"
end


-- log the current time
function logtime(key, value)
   _log("current time: " .. _time())
end


-- remove expired records of a table database
function expire()
   local args = {};
   local cdate = string.format("%d", _time())
   table.insert(args, "addcond\0x\0NUMLE\0" .. cdate)
   table.insert(args, "out")
   local res = _misc("search", args)
   if not res then
      _log("expiration was failed", 2)
   end
end


-- remove expired records of a table database
function expire2()
   local args = {};
   local xdate = string.format("%d", _time() - 3600)
   table.insert(args, "addcond\0d\0NUMLE\0" .. xdate)
   table.insert(args, "out")
   local res = _misc("search", args)
   if not res then
      _log("expiration was failed", 2)
   end
end


-- defrag the database file
function defrag()
   local time = tonumber(os.date("%H%M"))
   if time >= 2230 or time <= 0130 then
      return nil
   end
   if not _misc("defrag") then
      _log("defragmentation was failed", 2)
   end
end



----------------------------------------------------------------
-- private functions
----------------------------------------------------------------


-- call back function when starting
function _begin()
   _log("Lua processor started")
end


-- call back function when ending
function _end()
   _log("Lua processor finished")
end



-- END OF FILE
