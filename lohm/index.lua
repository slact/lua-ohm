module "lohm.index"
local indexf = "lohm.index:%s:%%s"

local indices = {
	hash = {
		update = function(self, key, newval, oldval)
			if(oldval) then
				redis:srem(self:getKey(oldval), key)
			end
			redis:sadd(self:getKey(newval), key)
		end,

		getkey = function(self, val)
			return self.keyf:format(hash(val))
		end
	}
}

function allIndices(sep)
	local t = {}
	for i, v in pairs(indices) do
		table.insert(t, i)
	end
	return table.concat(t, sep or ", ")
end

function new(indexType, model)
	if not indices[indexType] then
		error("No index type '" .. tostring(indexType) .. "' exists. There are")
	end
	
	return setmetatable({
		keyf = indexf:format(model:makeKey(indexType))
	}, {__index=indices[indexType]})
end	

function lookup(self, indextable, limit, offset, lazy)
		local reskey = redis:randomkey()
		local finishFromSet
		local res, err = assert(redis:transaction(function()
			for index, value in pairs(indextable) do
				redis:sunionstore(reskey, index:getKey(value))
			end
			if not lazy then
				finishFromSet = model:fromSetDelayed(reskey, limit, offset)
			end
		end))
		if not lazy then
			res, err = finishFromSet(res[#res])
		else
			res, err = model:fromSetLazily(reskey)
		end
		redis:del(randomkey)
		return assert(res, err)
	end
end