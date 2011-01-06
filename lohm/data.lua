local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next
local require, rawset = require, rawset

local debug = debug
local function I(...) return ... end
local datatypes = {hash=true, set=true, string=false, list=false, zset=false}
module ("lohm.data", function(t)
	setmetatable(t, { 
		__call = function(self, ...) return t.new(...) end, 
		__index = function(self, k)
			if datatypes[k] then
				return function(...) 
					return self.new(k, ...)
				end
			end
		end
	})
end)

function new(datatype, model, ...)
	assert(datatypes[datatype], ("%s is an invalid redis data type, or it hasn't been implemented in lohm yet."):format(datatype))
	
	local ids = setmetatable({}, { __mode='k'})
	local keys = setmetatable({}, { __mode='k'})
	
	local data_prototype = {
		setId = function(self, id)
			if not ids[self] then
				ids[self]=id
				keys[self]=model:key(id)
			else
				error("Object id is already set (" .. ids[self] .. "). Can't change it -- yet.")
			end
			return self
		end,
		
		setKey = function(self, key)
			--sanity check first
			return self:setId(self, assert(model.id(key), "That looks like a mighty invalid key"))
		end,
		
		getKey = function(self)
			return keys[self]
		end,
		
		getId = function(self)
			return ids[self]
		end,
		
		getModel = function(self)
			return model
		end, 
		
		save = function(self, what)
			local res, err = model.redis:transaction({cas=true, watch=key}, self:save_transaction(what))
			if res then
				return self
			else 
				return nil, err
			end
		end,

		delete = function(self)
			local key = assert(self:getKey(), "Cannot delete without a key")
			local res, err = model.redis:transaction({cas=true, watch=key}, self:delete_transaction())
			if not res or not res[#res] then error(err) end
			return self
		end
	}
	
	local obj = require("lohm." .. datatype)
	local newobj, obj_prototype = obj.new(model, ...)
	--now the common object methods. we're copying values instead of using   metatables for runtime efficiency
	for k, v in pairs(data_prototype) do
		rawset(obj_prototype, k, v)
	end
	return newobj
end