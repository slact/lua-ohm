local pairs, ipairs, table = pairs, ipairs, table
module "tredis.datum"

function new(prototype, model)
	local ids = setmetatable({}, { __mode='k'})
	local keys = setmetatable({}, { __mode='k'})

	local indices = model.indices
	local indexed = {}
	for i, v in pairs(indices) do
		table.insert(indexed, i)
	end

	local datum_prototype = {
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

		save = function(self, what)
			local key = keys[self]
			if not key then
				local id = model:reserveNextId()
				self:setId(key)
				key = self:getKey()
			end
			if type(what) == "string" then
				what = { what }
			end
			assert(key, "Tried to save data without a key or key assignment scheme. You can't do that.")

			local res, err = redis:check_and_set(key, function(r)
				--take care of indexing
				local change, old_indexed_attr = {}, {}
				if not what then
					change = self
				elseif type(what)=='table' then
					for i, k in pairs(what) do
						change[k]=self[k]
					end
				end
				
				--get the old attribute values that are being updated and are indexed.
				for k, v in  pairs(change) do
					if indices[k] then
						--TODO: hmget will probably be faster
						old_indexed_attr[k] = r:hget(k)
					end
				end
				
				coroutine.yield() --MULTI

				--update indices
				for k, v in pairs(old_indexed_attr)
					indices[k]:update(r, key, change[k], v)
				end
				redis:hmset(key, change)
			end)
			if res then
				return self
			else 
				return nil, err
			end
		end,

		delete = function(self)
			local key = assert(self:getKey(), "Cannot delete without a key")
			local res, err = redis:check_and_set(self:getKey(), function(r)
				--WATCH key
				--get old values 
				local current = r:hmget(unpack(indexed))
				coroutine.yield()
				--MULTI
				for attr, val in pairs(current) do
					indices[attr]:update(r, key, nil, val)
				end
				redis:del(key)
				--EXEC
			end)
			if not res or not res[#res] then error(err) end
			
			return self
		end,

		get = function(self, attr)
			local res = rawget(self, attr)
			if not res then 
				res = assert(redis:hget(self:getKey(), attr))
				self[attr]=res
			end
			return res
		end
	}	
	
	--merge that shit. aww yeah.
	for i, v in pairs(prototype)
		if not datum_prototype[i] then
			datum_prototype[i]=v
		else
			error(("%s is a built-in %s, and cannot be overridden by a custom object prototype... yet."):format(i, type(v)))
		end
	end
	
	local datum_meta = { __index = datum_prototype }

	--return a factory.
	return function(self, data, id)
		local obj =  setmetatable(data or {}, datum_meta)
		if(id) then
			obj:setId(id)
		end
		return obj
	end
end