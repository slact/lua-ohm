local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next

local debug = debug
local function I(...) return ... end
module "lohm.hash"

function new(model, prototype, attributes)

	--custom attribute stuff
	attributes = attributes or {}
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
	local function customattr(self, redis, custom, these_attributes)
		local afterYield = {}
		local key = self:getKey()
		for attr, fn in pairs(these_attributes or attributes) do
			local coro = coroutine.create(fn[custom])
			assert(coroutine.resume(coro, redis, self, key, attr, rawget(self, attr)))
			if coroutine.status(coro)=='suspended' then
				table.insert(afterYield, coro)
			end
		end
		return function(...)
			for i, coro in pairs(afterYield) do
				assert(coroutine.resume(coro, ...))
			end
		end
	end
	
	local indices = model.indices
	local indexed = {}
	for index_name, index_table in pairs(indices) do
		table.insert(indexed, index_name)
	end

	local hash_prototype = {
		save_coroutine = function(self, what)
			local key = self:getKey()
			if not key then
				local id = model:reserveNextId()
				self:setId(id)
				key = self:getKey()
			end
			if type(what) == "string" then
				what = { what }
			end
			assert(key, "Tried to save data without a key or key assignment scheme. You can't do that.")
			local id = self:getId()
			--NOTE: this is probably inefficient. do it better.
			return function(r)
				--take care of indexing
				local change, old_indexed_attr = {}, {}
				if not what then
					change = self
				elseif type(what)=='table' then
					for i, k in pairs(what) do
						change[k]=self[k]
					end
				end
				
				local hash_change, custom_change = {}, {}
				--get the _raw_ attribute values that are being updated and are indexed. While we're at it, see what custom attributes we need to save
				for k, v in  pairs(change) do
					local customattr = rawget(attributes, k) --rawget is needed here. so long as attributes' metatable has that fancy default __index
					if customattr then
						custom_change[k]=customattr
					else
						hash_change[k]=v
						if indices[k] then
							--TODO: hmget will probably be faster
							old_indexed_attr[k] = r:hget(key, k)
						end
					end
				end
				
				local after = customattr(self, r, 'save', custom_change)
				coroutine.yield() --MULTI
				after()
				--update indices
				for k, v in pairs(hash_change) do
					local index = indices[k]
					if index then
						indices[k]:update(r, id, v, old_indexed_attr[k])
					end
				end
				if next(hash_change) then --make sure changeset is non-empty
					r:hmset(key, hash_change)
				end
			end
		end,
	
		delete_coroutine = function(self)
			--get old values 
			local key, id = self:getKey(), self:getId()
			--NOTE: this is probably inefficient. do it better.
			return function(r)
				local current_indexed = {}
				if #indexed>0 then
					current_indexed = r:hmget(key, indexed)
				end
				local after = customattr(self, r, 'delete')
				coroutine.yield()
				--MULTI
				after()
				local id = self:getId()
				for attr, val in pairs(current_indexed) do
					indices[attr]:update(self, id, nil, val)
				end
				
				r:del(key)
			end
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
	return function(data, id, load_now)
		local obj =  setmetatable(data or {}, hash_meta)
		if id then
			obj:setId(id)
			if load_now then
				local loaded_data = model.redis:hgetall(obj:getKey())
				for k, v in pairs(loaded_data) do
					obj[k]=v
				end
			end
			customattr(obj, model.redis, 'load')
		end
		return obj
	end, hash_prototype
end