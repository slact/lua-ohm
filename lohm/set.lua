local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next
local debug = debug
local tostring = tostring
local transactionize = require("lohm.data").transactionize
local Model = require "lohm.model"
module("lohm.set", function(t,k)
	setmetatable(t, {
		__call = function(arg, redis)
			arg.type='set'
			return Model.new(arg, redis)
		end
	})
end)
--[[
local function setKey(self, set)
	if type(set)=='string' then 
		return set
	elseif type(set)=='table' then
		return set:getKey()
	end
end
local function memberId(self, member)
	if type(member)=='table' then 
		return member:getId()
	else
		return member
	end
local function self(self, foo)
	return self
end

local function sc(command, transform_return, transform_a, transform_b, transform_c, transform_return)
	return function(self, redis, a,b,c)
		if transform_a then a = transform_a(self, a) end
		if transform_b then b = transform_b(self, b) end
		if transform_c then c = transform_c(self, c) end
		local ret, err = redis[command](redis, self:getKey(), a,b,c)
		if transform_return then 
			return transform_return(self, ret), err
		else
			return ret, err
		end
	end
end

	sc('sadd', self, setKey)
	sc('scard')
	sc('sdiff', nil)
	sc('sdiffstore', newSet, setKey)
	sc('sinter', setKey)
	sc('sinterstore', newSet, setKey)
	smembers = {}, 
	smove = {'set', 'member'}, 
	spop = {}, 
	srandmember = {},
	srem = {'member'}, 
	sunion = {'set'}, "sunionstore" }

]]
local function tcopy(t)
	local ret = {}
	for i,v in pairs(t) do
		ret[i]=v
	end
	return ret
end

function initialize(prototype, attributes)
	local savedset, delta = {}, { add={}, rem={} }

	local model = prototype:getModel()
	local ref_model = attributes.reference
	local set_prototype = {
		add = function(self, ...)
			local redis = assert(model.redis)
			for i,member in pairs{...} do
				table.insert(self, type(member)~='table' and tostring(member) or member:getId())
			end
			return self
		end,
		remove = function(self, m, ...)
			if not m then return self end
			if type(m)=='table' then m = tostring(m:getId()) end
			local redis = assert(model.redis)
			for i, member in ipairs(self) do
				if type(member)=='table' and self.getId then
					member = tostring(member:getId())
				end
				if member==m then
					table.remove(self, i)
					return self:remove(...)
				end
			end
			return self:remove(...)
		end
	}
	for i, v in pairs(prototype) do
		assert(not set_prototype[i])
		set_prototype[i]=v
	end

	prototype:addCallback('save', function(self, redis)
			
		local saved = tcopy(savedset)
		debug.print("SAVEY", saved, savedset)
		local key = self:getKey()
		local member
		for i,v in pairs(self) do
			if(type(v))=='table' and ref_model then
				member = v:getId() 
				if not member then
					assert(v:setId(ref_model:reserveNextId(redis)))
					member = v:getId()
				end
			else
				member = v
			end
			if not saved[member] then
				delta.add[member]=true
			else
				saved[member]=nil
			end
		end
		
		redis:multi()
		
		--leftovers to be removed
		debug.print(saved)
		for i,v in pairs(saved) do
			print(i,v)
			delta.rem[v]=true
		end
		debug.print(delta)
		for v, _ in pairs(delta.rem) do
			redis:srem(key, v)
		end
		for v,_ in pairs(delta.add) do
			redis:sadd(key, v)
		end

		delta.add, delta.rem = {}, {}
	end)

	prototype:addCallback('load', function(self, redis)
		if not self then return nil, "No set to load..." end
		local key = self:getKey()
		if not key then return nil, "No id given, can't load set from redis." end
		
		--clear set
		for i,v in ipairs(self) do
			self[i]=nil
		end
		savedset = {}
		local res = redis:smembers(key)
		if ref_model then
			
		end
		redis:multi()
		coroutine.yield()
		for i,v in pairs(res) do
			savedset[v]=v
			if ref_model then
				v = ref_model:new(nil, v, true)
			end
			table.insert(self, v)
		end
	end)
	
	if ref_model then
		prototype:addCallback('delete', function(self, redis)
			redis:multi()
			coroutine.yield()
			for i,v in pairs(self) do
				v:delete()
			end
		end)
	end

	local set_meta = {__index = set_prototype }
	
	return function(data, id, load_now)
		local set = setmetatable(data or {}, set_meta)
		if id then 
			set:setId(id) 
		end
		if load_now then
			set:load()
		end
		return set
	end
	
end