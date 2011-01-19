local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next

local debug = debug
local function I(...) return ... end
local Index = require "lohm.index"
module("lohm.hash", function(t,k)
	setmetatable(t, {
		__call = function(arg, redis)
			arg.type='hash'
			return Model.new(arg, redis)
		end
	})
end)

function initialize(prototype, arg)
	local model = prototype:getModel()
	prototype:addCallback('load', function(self, redis)
		local id = self:getId()
		if not id then return nil, "No id given, can't load hash from redis." end
		redis:multi()
		local hgetall_res, err = redis:hgetall(self:getKey())
		assert(hgetall_res.queued==true)
		local raw_loaded_data = coroutine.yield()
		local loaded_data = raw_loaded_data[1]

		if not next(loaded_data) then
			return nil, "Redis hash at " .. self:getKey() .. " not found."
		else
			for k, v in pairs(loaded_data) do
				self[k]=v
			end
		end
		return self
	end):addCallback('save', function(self, redis)
		local key = self:getKey()
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
		return self
	end):addCallback('delete', function(self, redis)
		redis:multi()
		redis:del(self:getKey())
		return self
	end)

	--custom attribute stuff
	local attributes, indices = arg.attributes or {}, {}
	setmetatable(attributes, {__index=function(t,k)
		return {
			load=function(self, redis, attr)
				return redis:hget(self:getKey(), attr)
			end, 
			save=function(self, redis, attr, val)
				return redis:hset(self:getKey(), attr, val)
			end,
			delete=I,
			getCallbacks = function() return {} end
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
			
			prototype:addCallback('save', function(self, redis)
				local savedval = redis:hget(self:getKey(), attr)
				assert(index:update(self, redis, self:getId(), self[attr], savedval))
			end)

			prototype:addCallback('delete', function(self, redis)
				local savedval = redis:hget(self:getKey(), attr)
				assert(index:update(self, redis, self:getId(), nil, savedval))
			end)
		end
	end

	function prototype:get(attr, force)
		local res = rawget(self, attr)
		if force or not res then 
			res = attributes[attr].load(self, model.redis, attr)
			self[attr]=res
		end
		return res
	end

	function prototype:set(attr, val)
		self[attr]=val
		return self
	end
	
	local hash_meta = { __index = function(self, attr)
		local proto = prototype[attr]
		if proto then
			return proto
		else
			local key = self:getKey()
			print(key, attr)
			if key then
				local res = attributes[attr].load(self, model.redis, attr)
				self[attr]=res
				return res
			end
			return nil
		end
	end }
	
	local function append(t1, t2)
		for i,v in pairs(t2) do
			table.insert(t1, v)
		end
		return t1
	end
	
	hash_meta.getCallbacks = function(operation)
		--TODO: memoize this instead of doing it on the fly.
		local ret = append({}, prototype:getCallbacks(operation))
		for i, obj_table in ipairs{attributes, indices} do
			for j, obj in pairs(obj_table) do
				append(ret, obj:getCallbacks(operation))
			end
		end
		return ret
	end
	
	return function(data, id, load_now)
		local obj = setmetatable(data or {}, hash_meta)
		if id then 
			obj:setId(id) 
		end
		if load_now then
			return obj:load()
		else
			return obj
		end
	end
end