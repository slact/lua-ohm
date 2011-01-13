local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next

local debug = debug
local function I(...) return ... end
local Index = require "lohm.index"
module "lohm.hash"

function new(model, prototype, arg)

	local callbacks = {
		load={function(self, redis, id, load_now)
			if not id then return nil, "No id given, can't load hash from redis." end
			self:setId(id)
			redis:milti()
			if load_now then
				local loaded_data, err = redis:hgetall(self:getKey())
				assert(loaded_data.queued==true)
				loaded_data, err = coroutine.yield()
				if not next(loaded_data) then
					return nil, "Redis hash at " .. self:getKey() .. " not found."
				else
					for k, v in pairs(loaded_data) do
						self[k]=v
					end
				end
			end
		end},
		save={ function(self, redis)
			local key = self:getKey()
			
			if not key then
				--a new id is needed
				local id = model:withRedis(redis, function(r)
					return r:reserveNextId()
				end)
				self:setId(id)
				key = self:getKey()
			end
			assert(key, "Tried to save data without a key or key assignment scheme. You can't do that.")
			local id = self:getId()
			
			redis:multi()

			local hash_change = {}
			for i,v in pairs(self) do
				if type(v)~='table' then
					hash_change[i]=v
				end
			end
			if next(hash_change) then --make sure changeset is non-empty
				redis:hmset(key, self)
			end
		end },
		delete={ function(self, redis)
			redis:multi()
			
			redis:del(key)
		end	}
	}

	--custom attribute stuff
	local attributes, indices = arg.attributes or {}, {}
	setmetatable(attributes, {__index=function(t,k)
		return {
			load=function(redis, self, key, attr)
				return redis:hget(key, attr)
			end, 
			save=function(redis, self, key, attr, val)
				return redis:hset(key, attr, val)
			end,
			delete=I
		}
	end})
	
	local unparsed_indices = arg.index or arg.indices
	if unparsed_indices and next(unparsed_indices) then
		local defaultIndex = Index:getDefault()
		for attr, indexType in pairs(unparsed_indices) do
			if type(attr)~="string" then 
				attr, indexType = indexType, defaultIndex
			end
			
			local index = Index:new(indexType, model, attr)
			indices[attr] = index
			
			table.insert(callbacks.save, function(self, redis)
				local savedval = redis:hget(self:getKey(), attr)
				assert(index:update(self, redis, id, self[attr], savedval))
			end)

			table.insert(callbacks.save, function(self, redis)
				local savedval = redis:hget(self:getKey(), attr)
				assert(index:update(self, redis, id, nil, savedval))
			end)
		
		end
	end

	for attr, cb in pairs(attributes) do
		for i, when in pairs {"save", "load", "delete"} do
			local callback = cb[when]
			if callback then
				table.insert(callbacks[when], callback)
			end
		end
	end

	local hash_prototype = {
		
		get = function(self, attr, force)
			local res = rawget(self, attr)
			if force or not res then 
				res = attributes[attr].load(redis, self:getKey(), attr, self)
				self[attr]=res
			end
			return res
		end,

		set = function(self, attr, val)
			self[attr]=val
			return self
		end
	}
	
	--merge that shit. aww yeah.
	for i, v in pairs(prototype or {}) do
		if not hash_prototype[i] then
			hash_prototype[i]=v
		else
			error(("%s is a built-in %s, and cannot be overridden by a custom object prototype... yet."):format(i, type(v)))
		end
	end

	local hash_meta = { __index = function hash_ondemand_loader(self, attr)
		local proto = hash_prototype[attr]
		if proto then
			return proto
		else
			local key = self:getKey()
			if key then
				local res = attributes[attr].load(model.redis, self, key, attr)
				self[attr]=res
				return res
			end
			return nil
		end
	end }

	return hash_meta, hash_prototype, callbacks
end