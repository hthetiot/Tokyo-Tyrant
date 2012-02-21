--
-- MapReduce implementation by the Lua extension of Tokyo Tyrant
--



----------------------------------------------------------------
-- public functions
----------------------------------------------------------------


-- count words of all records
function wordcount(texpr)
   local targets = nil
   if texpr and #texpr > 0 then
      targets = {}
      table.insert(targets, texpr)
   end
   function mapper(key, value, mapemit)
      for word in string.gmatch(string.lower(value), "%w+") do
         mapemit(word, 1)
      end
      return true
   end
   local res = ""
   function reducer(key, values)
      res = res .. key .. "\t" .. #values .. "\n"
      return true
   end
   if not _mapreduce(mapper, reducer, targets) then
      res = nil
   end
   return res
end



----------------------------------------------------------------
-- private functions
----------------------------------------------------------------


-- call back function when starting
function _begin()
   _log("Lua processor started")
   _tmpdir_ = "/tmp"
end


-- call back function when ending
function _end()
   _log("Lua processor finished")
end



-- END OF FILE
