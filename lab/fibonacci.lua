function fibonacci(key, value)
   local num = tonumber(key)
   local large = math.pow((1 + math.sqrt(5)) / 2, num)
   local small = math.pow((1 - math.sqrt(5)) / 2, num)
   return (large - small) / math.sqrt(5)
end

function fibnext(key, value)
   local cur = tonumber(_get("fibcur"))
   if not cur then
      _put("fibold", 0)
      _put("fibcur", 1)
      return 1
   end
   local old = tonumber(_get("fibold"))
   _put("fibold", cur)
   cur = old + cur
   _put("fibcur", cur)
   return cur
end
