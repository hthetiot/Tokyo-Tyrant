--
-- Inverted index by the Lua extension of Tokyo Tyrant
--


-- constants
DELIMS = " \t\r\n"   -- delimiters of tokenizing
LIMNUM = 2000        -- limit number of kept occurrence
DEFMAX = 10          -- default maximum number of search
IDPREFIX = "\t#"     -- prefix to get the ID from the URI
URIPREFIX = "\t@"    -- prefix to get the URI from the ID
TEXTPREFIX = "\t|"   -- prefix of get the text from the ID



----------------------------------------------------------------
-- public functions
----------------------------------------------------------------


-- register a record with the ID and the text
function put(id, text)
   id = tonumber(id)
   if not id or id < 1 then
      return nil
   end
   if not text then
      return nil
   end
   if not _putimpl(id, text) then
      return nil
   end
   return "ok"
end


-- remove a record with the ID and the text
function out(id, text)
   id = tonumber(id)
   if not id or id < 1 then
      return nil
   end
   if not text then
      return nil
   end
   if not _outimpl(id, text) then
      return nil
   end
   return "ok"
end


-- replace a record with the ID and the text before and the text after
function replace(id, befaft)
   id = tonumber(id)
   if not id or id < 1 then
      return nil
   end
   if not befaft then
      return nil
   end
   local pivot = string.find(befaft, "\n", 1, true)
   if not pivot then
      return nil
   end
   local bef = string.sub(befaft, 1, pivot - 1)
   local aft = string.sub(befaft, pivot + 1)
   if not _outimpl(id, bef) then
      return nil
   end
   if not _putimpl(id, aft) then
      return nil
   end
   return "ok"
end


-- search with a phrase of intersection
function search(phrase, max)
   if not phrase then
      return nil
   end
   max = tonumber(max)
   if not max or max < 0 then
      max = DEFMAX
   end
   local result = _searchimpl(phrase, false)
   local rtxt = #result .. "\n"
   local bot = #result - max + 1
   if bot < 1 then
      bot = 1
   end
   for i = #result, bot, -1 do
      rtxt = rtxt .. result[i] .. "\n"
   end
   return rtxt
end


-- register a record with the URI and the text
function xput(uri, text)
   if not uri or #uri < 1 or not text then
      return nil
   end
   if not _xputimpl(uri, text) then
      return nil
   end
   return "ok"
end


-- remove a record with the URI
function xout(uri)
   if not uri or #uri < 1 then
      return nil
   end
   if not _xoutimpl(uri) then
      return nil
   end
   return "ok"
end


-- retrieve a record with the URI
function xget(uri)
   if not uri or #uri < 1 then
      return nil
   end
   local id = _get(IDPREFIX .. uri)
   id = tonumber(id)
   if not id then
      return nil
   end
   local text = _get(TEXTPREFIX .. id)
   if not text then
      return nil
   end
   return text
end


-- search for URIs with a phrase of intersection
function xsearch(phrase, max)
   if not phrase then
      return nil
   end
   max = tonumber(max)
   if not max or max < 0 then
      max = DEFMAX
   end
   local result = _searchimpl(phrase, false)
   local rtxt = #result .. "\n"
   for i = #result, 1, -1 do
      if max < 1 then
         break
      end
      local uri = _get(URIPREFIX .. result[i])
      if uri then
         rtxt = rtxt .. uri .. "\n"
         max = max - 1
      end
   end
   return rtxt
end


-- make the stash of suggestion words
function stashmake(minhit)
   minhit = tonumber(minhit)
   if not minhit or minhit < 0 then
      minhit = 0
   end
   _stashvanish()
   function proc(word, idsel)
      if not string.match(word, "\t") then
         local result = _unpack("w*", idsel)
         if #result >= minhit then
            _stashput(word, #result)
         end
      end
      return true
   end
   _foreach(proc)
   return "ok"
end


-- retrieve similar words in the stash
function stashlist(word, max)
   max = tonumber(max)
   if not max or max < 0 then
      max = DEFMAX
   end
   local result = {}
   function proc(tword, hnum)
      local dist = _dist(tword, word, true)
      if dist <= 3 then
         table.insert(result, { tword, dist, tonumber(hnum) })
      end
      return true
   end
   _stashforeach(proc)
   function reccmp(a, b)
      if a[2] > b[2] then
         return true
      end
      if a[2] == b[2] and a[3] < b[3] then
         return true
      end
      return false
   end
   table.sort(result, reccmp)
   local rtxt = ""
   local bot = #result - max + 1
   if bot < 1 then
      bot = 1
   end
   for i = #result, bot, -1 do
      local rec = result[i]
      rtxt = rtxt .. rec[1] .. "\t" .. rec[2] .. "\t" .. rec[3] .. "\n"
   end
   return rtxt
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


-- register a record with the ID and the text
function _putimpl(id, text)
   local tokens = _tokenize(text)
   if math.random() < 5 / LIMNUM then
      for i = 1, #tokens do
         token = tokens[i]
         if not _lock(token) then
            _log("lock error")
            return false
         end
         local ids = {}
         local idsel = _get(token)
         if idsel then
            ids = _unpack("w*", idsel)
         end
         local nids = {}
         local top = #ids - LIMNUM + 2
         if top < 1 then
            top = 1
         end
         for j = top, #ids do
            table.insert(nids, ids[j])
         end
         table.insert(nids, id)
         idsel = _pack("w*", nids)
         if not _put(token, idsel) then
            _log("put error")
            _unlock(token)
            return false
         end
         _unlock(token)
      end
   else
      local idsel = _pack("w", id)
      for i = 1, #tokens do
         token = tokens[i]
         if not _lock(token) then
            _log("lock error")
            return false
         end
         if not _putcat(token, idsel) then
            _log("putcat error")
            _unlock(token)
            return false
         end
         _unlock(token)
      end
   end
   return true
end


-- remove a record with the ID and the text
function _outimpl(id, text)
   local tokens = _tokenize(text)
   for i = 1, #tokens do
      token = tokens[i]
      if not _lock(token) then
         _log("lock error")
         return false
      end
      local ids = {}
      local idsel = _get(token)
      if idsel then
         ids = _unpack("w*", idsel)
      end
      local nids = {}
      for j = 1, #ids do
         if ids[j] ~= id then
            table.insert(nids, ids[j])
         end
      end
      idsel = _pack("w*", nids)
      if not _put(token, idsel) then
         _log("put error")
         _unlock(token)
         return false
      end
      _unlock(token)
   end
   return true
end


-- search with a phrase of intersection
function _searchimpl(phrase, union)
   local tokens = _tokenize(phrase)
   local tnum = #tokens
   if tnum < 1 then
      return {}
   end
   local idsel = _get(tokens[1])
   local result = _unpack("w*", idsel)
   if union then
      local rsets = {}
      table.insert(rsets, result)
      for i = 2, tnum do
         idsel = _get(tokens[i])
         local ids = _unpack("w*", idsel)
         table.insert(rsets, ids)
      end
      result = _union(rsets)
   else
      for i = 2, tnum do
         idsel = _get(tokens[i])
         local ids = _unpack("w*", idsel)
         result = _isect(result, ids)
      end
   end
   table.sort(result)
   return result
end


-- register a record with the URI and the text
function _xputimpl(uri, text)
   local id = _get(IDPREFIX .. uri)
   id = tonumber(id)
   if id then
      local otext = _get(TEXTPREFIX .. id)
      if otext and not _outimpl(id, otext) then
         return false
      end
   else
      id = _addint(IDPREFIX, 1)
      if not id then
         _log("addint error")
         return false
      end
   end
   if not _putimpl(id, text) then
      return false
   end
   if not _put(IDPREFIX .. uri, id) then
      _log("put error")
      return false
   end
   if not _put(URIPREFIX .. id, uri) then
      _log("put error")
      return false
   end
   if not _put(TEXTPREFIX .. id, text) then
      _log("put error")
      return false
   end
   return true
end


-- remove a record with the URI
function _xoutimpl(uri)
   local id = _get(IDPREFIX .. uri)
   id = tonumber(id)
   if not id then
      return true
   end
   local text = _get(TEXTPREFIX .. id)
   if not text then
      return true
   end
   if not _outimpl(id, text) then
      return false
   end
   if not _out(IDPREFIX .. uri) then
      _log("out error")
      return false
   end
   if not _out(URIPREFIX .. id) then
      _log("out error")
      return false
   end
   if not _out(TEXTPREFIX .. id) then
      _log("out error")
      return false
   end
   return true
end


-- break a text into an array of tokens
function _tokenize(text)
   local tokens = {}
   local uniq = {}
   for token in string.gmatch(text, "[^" .. DELIMS .. "]+") do
      if #token > 0 and not uniq[token] then
         table.insert(tokens, token)
         uniq[token] = true
      end
   end
   return tokens
end



-- END OF FILE
