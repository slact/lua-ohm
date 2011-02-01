local print, type, assert, pcall, require, table, tostring, pairs, ipairs, setmetatable = print, type, assert, pcall, require, table, tostring, pairs, ipairs, setmetatable
local error = error
local debug = debug
local lohm = require "lohm"
module("lohm.index", function(t, k)
	setmetatable(t, {
		__call = function(self)
			
		end
	})
end)

local hash
do
	local s,  sha1 = pcall(require, "sha1")
	if s then
		hash = sha1.digest
	else
		local s, crypto = pcall(require, "crypto")
		if not s then 
			error("Can't find a sha1 library (tried sha1 (lmd5) and crypto (LuaCrypto))")
		else
			local digest = (require "crypto.evp").digest
			hash = function(input)
				return digest("sha1", input)
			end
		end
	end
end

function allIndices(sep)
	local t = {}
	for i, v in pairs(indices) do
		table.insert(t, i)
	end
	return table.concat(t, sep or ", ")
end

function getDefault()
	return "hash"
end


local query_prototype = {
	union = function(self, set)
		table.insert(self.queue, {command='sunionstore', key=set:getKey()})
		return self
	end,

	intersect = function(self, set)
		table.insert(self.queue, {command='sinterstore', key=set:getKey()})
		return self
	end,
	
	exec = function(self)
		local temp_key, base_set_key = nil, self.set:getKey()
		local res, err = self.redis:transaction({cas=true}, function(redis)
			local queue = self.queue
			--[[for i, cmd in ipairs(queue) do
				redis:watch(cmd.key)
			end]]
			if #queue >= 1 then
				local TempSet = self.temp_set_model
				local temp = TempSet:new(nil, TempSet:reserveNextId(), false)
				redis:multi()
				temp_key = temp:getKey()
				self.set = temp
				for i=1, #queue do
					local cmd = queue[i]
					assert(cmd.key)
					redis[cmd.command](redis, temp_key, cmd.key, base_set_key or temp_key)
					base_set_key = nil
				end
				redis:expire(temp_key, 30)
			else
				redis:multi()
			end
			redis:smembers(temp_key or base_set_key)
		end)
		--note that the temporary result set is not deleted explicitly, but it does expire.
		res = res[#res]
		for i,v in pairs(res) do
			res[i]=self.model:new(nil, v, false)
		end
		return res
	end
}
local query_meta = { __index = query_prototype }
function query(starting_set, model)
	local self = setmetatable({
		set = starting_set,
		queue = {},
		redis = model.redis,
		model = model,
		temp_set_model = lohm.new({type="set", key="~index_result:%s", expire=10}, model.redis),
	}, query_meta)
	return self
end

function new(model, name)
	assert(type(name)=='string', 'What do you want indexed? (no name parameter)')
	local Index = lohm.new({ type='set', key=model:key(("index.hash:%s:%%s"):format(name or "NONAME"))}, model.redis)
	
	return setmetatable({
		model = Index,
		update = function(self, id, newval, oldval)
			assert(id, "id must be given")
			assert(type(newval)~='table')
			assert(type(oldval)~='table')
			local redis = self.model.redis
			if(oldval~=nil) then
				redis:srem(self:getSetKey(oldval), id)
			end
			if(newval~=nil) then
				redis:sadd(self:getSetKey(newval), id)
			end
			return self
		end,
		getSet = function(self, val)
			return self.model:new(nil, hash(val), false)
		end,
		getSetKey = function(self, val)
			return (self.model:key(hash(val)))
		end,
	}, {__call = function(self, ...)
		return self:getSet(...) 
	end })
end	
