local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next

local debug = debug
local function I(...) return ... end
local Index = require "lohm.index"
module "lohm.hash"

function new(model, prototype, arg)

	local callbacks = {load={},save={},delete={}}

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
			indices[attr] = Index:new(indexType, model, attr)
		end
	end

	for attr, index in pairs(indices) do
		table.insert(callbacks.save, function(self, redis)
			local savedval = redis:hget(self:getKey(), attr)
			assert(index:update(self, redis, id, self[attr], savedval))
		end)

		table.insert(callbacks.save, function(self, redis)
			local savedval = redis:hget(self:getKey(), attr)
			assert(index:update(self, redis, id, nil, savedval))
		end)
	end

	for attr, cb in pairs(attributes) do
		--TODO: attribute!!!
	end

	local hash_prototype = {
		save_transaction = function(self, redis)
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
		end,
		
		delete_transaction = function(self, redis)
			local current_indexed = {}
			if #indexed>0 then
				current_indexed = r:hmget(key, indexed)
			end
			redis:multi()
			
			redis:del(key)
		end,
		
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

	--TODO: should find a better way to do this. metatable metatables aren't quite as good a solution as I had hoped.
	local function hash_ondemand_loader(self, attr)
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
	end
	
	--merge that shit. aww yeah.
	for i, v in pairs(prototype or {}) do
		if not hash_prototype[i] then
			hash_prototype[i]=v
		else
			error(("%s is a built-in %s, and cannot be overridden by a custom object prototype... yet."):format(i, type(v)))
		end
	end
	
	local hash_meta = { __index = hash_ondemand_loader }

	--return a factory and the hash prototype
	return function(self, redis, id, load_now)
		local obj =  setmetatable(data or {}, hash_meta)
		if id then
			obj:setId(id)
			if load_now then
				local loaded_data, err = redis:hgetall(obj:getKey())
				if not next(loaded_data) then
					return nil, "Redis hash at " .. obj:getKey() .. " not found."
				else
					for k, v in pairs(loaded_data) do
						obj[k]=v
					end
				end
			end
		else
			return nil, "no id given"
		end
		return obj
	end, hash_prototype, callbacks
end