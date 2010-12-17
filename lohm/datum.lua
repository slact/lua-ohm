local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack
local function I(...) return ... end
module "lohm.datum"

function new(prototype, model, attributes)
	local ids = setmetatable({}, { __mode='k'})
	local keys = setmetatable({}, { __mode='k'})
	
	--custom attribute stuff
	attributes = attributes or {}
	setmetatable(attributes, {__index={
		load=function(redis, self, key, attr)
			return redis:hget(key, attr)
		end, 
		save=function(redis, self, key, attr, val)
			return redis:hset(key, attr, val)
		end,
		delete=I
	}})
	
	local function customattr(self, custom)
		local afterYield = {}
		local key = self:getKey()
		for attr, fn in pairs(attributes) do
			local coro = coroutine.create(fn[custom])
			assert(coroutine.resume(coro, model.redis, self, rawget(self, attr)))
			if coroutine.status(coro)=='suspended' then
				table.insert(afterYield, coro)
			end
		end
		return function(...)
			for i, coro in pairs(afterYield) do
				coroutine.resume(coro, ...)
			end
		end
	end
	
	local indices = model.indices
	local indexed = {}
	for index_name, index_table in pairs(indices) do
		table.insert(indexed, index_name)
	end

	local datum_prototype = setmetatable({
		setId = function(self, id)
			if not ids[self] then
				ids[self]=id
				keys[self]=model:key(id)
			else
				error("Object id is already set (" .. ids[self] .. "). Can't change it -- yet.")
			end
			return self
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

		save_coroutine = function(self, what)
			local key = keys[self]
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
				
				--get the _raw_ attribute values that are being updated and are indexed. While we're at ir, see what custom attributes we need to save
				for k, v in  pairs(change) do
					if indices[k] then
						--TODO: hmget will probably be faster
						old_indexed_attr[k] = r:hget(key, k)
					end
				end
				local after = customattr(self, 'save')
				coroutine.yield() --MULTI
				after()
				--update indices
				for k, v in pairs(change) do
					local index = indices[k]
					if index then
						indices[k]:update(r, id, v, old_indexed_attr[k])
					end
				end
				r:hmset(key, change)
			end
		end,
		
		save = function(self, what)
			local res, err = model.redis:check_and_set(key, self:save_coroutine(what))
			if res then
				return self
			else 
				return nil, err
			end
		end,

		delete_coroutine = function(self, r) 
			--get old values 
			local key, id = self:getKey(), self:getId()
			--NOTE: this is probably inefficient. do it better.
			return function(r)
				local current = {}
				if #indexed>0 then
					current = r:hmget(key, indexed)
				end
				local after = customattr(self, 'delete')
				coroutine.yield()
				--MULTI
				after()
				local id = self:getId()
				for attr, val in pairs(current) do
					indices[attr]:update(r, id, nil, val)
				end
				
				r:del(key)
			end
		end,

		delete = function(self)
			local key = assert(self:getKey(), "Cannot delete without a key")
			local res, err = model.redis:check_and_set(key, self:delete_coroutine())
			if not res or not res[#res] then error(err) end
			return self
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
	}, {__index = datum_ondemand_loader })
	
	--merge that shit. aww yeah.
	for i, v in pairs(prototype or {}) do
		if not datum_prototype[i] then
			datum_prototype[i]=v
		else
			error(("%s is a built-in %s, and cannot be overridden by a custom object prototype... yet."):format(i, type(v)))
		end
	end
	
	local datum_meta = { __index = datum_prototype }

	--return a factory.
	return function(data, id)
		local obj =  setmetatable(data or {}, datum_meta)
		if(id) then
			obj:setId(id)
		end
		return obj
	end
end