module "lohm.index"
local indexf = "lohm.index:%s:%s:%%s"

local indices = {
	hash = {
		update = function(self, redis, key, newval, oldval)
			if(oldval~=nil) then
				redis:srem(self:getKey(oldval), key)
			end
			if(newval~=nil) then
				redis:sadd(self:getKey(newval), key)
			end
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

function new(indexType, model, attr)
	if not indices[indexType] then
		error("No index type '" .. tostring(indexType) .. "' exists. There are")
	end
	assert(type(attr)=='string', 'What do you want indexed? (attr parameter is incorrect)')
	return setmetatable({
		keyf = indexf:format(model:makeKey(indexType), attr)
		end,
	}, {__index=indices[indexType]})
end	

function lookup(self, redis, indextable, limit, offset, lazy)
		local reskey = redis:randomkey()
		local finishFromSet
		local res, err = assert(redis:transaction(function(r)
			for index, value in pairs(indextable) do
				r:sunionstore(reskey, index:getKey(value))
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