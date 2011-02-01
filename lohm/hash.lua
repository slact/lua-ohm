local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next
local lohm = require "lohm"
local Query = require "lohm.index".query
local Index = require "lohm.index"
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
	local references, indices
	--custom attribute stuff
	local attributes, indices = arg.attributes or {}, {}

	prototype:addCallback('load', function(self, redis)
		if not self then return nil, "No hash to load..." end
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
				if not attributes[k] then
					self[k]=v
				end
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
				--[[ tables are assumed to be special values and must be 
				handled by declaring them upfront when creating a lohm 
				model. (the alternative would be too dynamic for a fixed data structure)]]
				hash_change[i]=v
			end
		end
		if next(hash_change) then --make sure changeset is non-empty
			redis:hmset(key, hash_change)
		end
		--IMPORTANT TODO: attribute removal.
		return self
	end):addCallback('delete', function(self, redis)
		redis:multi()
		redis:del(self:getKey())
		return self
	end)

	setmetatable(attributes, {__index=function(t,k)
		return {
			load=function(self, redis, attr)
				assert(type(attr)~='table')
				return redis:hget(self:getKey(), attr)
			end, 
			save=function(self, redis, attr, val)
				return redis:hset(self:getKey(), attr, val)
			end,
			delete=I,
			getCallbacks = function() return {} end
		}
	end})
	for attr_name, attr_model in pairs(attributes) do
		if type(attr_model)=='table' then
			attributes[attr_name] = {
				load = function(self, redis, attr)
					local val = rawget(self, attr)
					assert(type(val)~='table')
					self[attr_name]=assert(attr_model:findById(val))
				end,
				model = attr_model
			}

			--everything cascades here
			local attr_obj = attr_model:new()
			for i,event in pairs{'load', 'save', 'delete'} do
				for j, callback in pairs(attr_obj:getCallbacks(event)) do
					prototype:addCallback(event, function(self, redis)
						local val = rawget(self, attr_name) or redis:hget(self:getKey(), attr_name)
						
						if type(val)=='table' then
							if not val:getId() then
								local id = redis:hget(self:getKey(), attr_name)
								if id then val:setId(id) end
							end
							assert(rawget(val,queued) ~= true) --redis-lua queued indicator
							if val.getKey then
								local key = val:getKey()
								if not key then
									val:setId(assert(attr_model:reserveNextId( redis )))
									key = val:getKey()
								end
								if key then redis:watch(val:getKey()) end 
							end
						elseif val then
							redis:watch(attr_model:key(val))
							val = attr_model:new({}, val, false)
							self[attr_name]=val
						end
						assert(val==rawget(self, attr_name))
						return callback(val, redis)
					end)
				end
			end
			
			prototype:addCallback('save', function(self, redis)
				--this is a transaction coroutine, mind you.
				local  ref_obj = self[attr_name]
				if not ref_obj then return nil end
				local id = ref_obj:getId()
				redis:multi()
				if id then
					redis:hset(self:getKey(), attr_name, id)
				end
				coroutine.yield()
			end)
		end
	end

	local unparsed_indices = arg.index or arg.indices
	if unparsed_indices and next(unparsed_indices) then
		for i, attr in pairs(unparsed_indices) do
			--we accept {"foo","bar"} and {foo=Index(...), bar=Index(...), "baz", bax"} tables
			local index
			if type(attr)=='table' then
				attr, index = i, attr
			else
				index = Index.new(model, attr)
			end

			indices[attr] = index
			
			prototype:addCallback('save', function(self, redis)
				local savedval = redis:hget(self:getKey(), attr)
				local newval = rawget(self, attr) --mustn't trigger autoloader
				if type(newval)=='table' then 
					newval = newval:getId()
				end
				assert(type(newval)~='table')
				assert(self:getId())
				assert(index:update(self:getId(), newval, savedval))
			end)

			prototype:addCallback('delete', function(self, redis)
				local savedval = redis:hget(self:getKey(), attr)
				assert(index:update(self:getId(), nil, savedval))
			end)
		end
	end
	model.findByAttr = function(self, arg)
		local query
		for attr, val in pairs(arg) do
			local this_set = indices[attr](val)
			if query then
				query:intersect(this_set)
			else
				query = Query(this_set, model)
			end
		end
		model.redis:echo("FOO")
		return query:exec()
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
	
	return function(data, id, load_now)
		local obj = setmetatable(data or {}, hash_meta)
		if id then 
			obj:setId(id)
		end
		for name, attr in pairs(attributes) do
			if not rawget(obj, attr) then
				obj[name]=attr.model:new()
			end
		end
		
		if load_now then
			return obj:load()
		else
			return obj
		end
	end
end