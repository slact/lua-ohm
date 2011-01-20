local print, debug, type, error = print, debug, type, error
local assert, coroutine, table, pairs, ipairs = assert, coroutine, table, pairs, ipairs
local tcopy = function(t)
	local res = {}
	for i, v in pairs(t) do
		res[i]=v
	end
	return res
end