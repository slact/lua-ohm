local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next
local debug = debug
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
function initialize(prototype, attributes)
	local set, delta = {}, { add={}, rem={} }
	prototype:addCallback('save', function(self, redis)
		local key = self:getKey()
		redis:multi()
		for v, _ in pairs(delta.rem) do
			redis:srem(key, v)
		end
		for v,_ in pairs(delta.add) do
			redis:sadd(key, v)
		end
		delta.add, delta.rem = {}, {}
		return self
	end)

	local model = prototype:getModel()

	local set_prototype = {
		add = function(self, ...)
			local redis = assert(model.redis)
			for i,member in pairs{...} do
				delta.add[member]=true
			end
			return self
		end,
		remove = function(self, ...)
			local redis = assert(model.redis)
			for i, member in pairs{...} do
				delta.rem[member]=true
			end
			return self
		end
	}
	for i, v in pairs(prototype) do
		assert(not set_prototype[i])
		set_prototype[i]=v
	end
	local set_meta = {__index = set_prototype }
	
	return function(data, id, load_now)
		local set = setmetatable(data or {}, set_meta)
		if id then 
			set:setId(id) 
		end
		return set
	end
	
end