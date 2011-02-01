local print, type, assert, pcall, require, table, tostring, pairs, ipairs, setmetatable = print, type, assert, pcall, require, table, tostring, pairs, ipairs, setmetatable
local error = error
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


local query_meta = {
	union = function(self, set)
		table.insert(self.queue, {command='sunionstore', key=set:getKey()})
		return self
	end,

	intersect = function(self, set)
		table.insert(self.queue, {command='sinterstore', key=set:getKey()})
		return self
	end,
	
	exec = function(self)
		self.redis:transaction({cas=true, watch=self.set:getKey()}, function(redis)
			local queue = self.queue
			for i, cmd in ipairs(queue) do
				redis:watch(cmd.key)
			end
			redis:multi()
			if #queue >= 1 then
				local temp = TempSet:new(nil, TempSet:reserveNextId(), false)
				local temp_key = temp.getKey()
				redis:expire(temp_key, 10) -- expire that set in 10 seconds flat.
				local base_set_key = self.set:getKey()
				self.set = temp
				for i=1, #queue do
					local cmd = queue[i]
					redis[cmd.command](temp_key, cmd.key, base_set_key or temp_key)
					base_set_key = nil
				end
			end
		end)
		--note that the temporary result set is not deleted explicitly, but it does expire.
		local res = redis:smembers(temp_key)
		for i,v in pairs(assert(redis:smembers(res_key))) do
			res[i]=self.model:new(nil, v, false)
		end
		return res, temp
	end
}

function query(starting_set, model)
	local self = setmetatable({
		set = starting_set,
		queue = {},
		redis = model.redis,
		model = model
	}, query_meta)
	return self
end

local index 

function new(model, name)
	if not indices[indexType] then
		error(("Unknown index '%s'. Known indices: %s."):format(tostring(indexType), allIndices()))
	end
	assert(type(name)=='string', 'What do you want indexed? (no name parameter)')
	
	local Index = lohm.new({ type='set', key=model:key(("%s:index.%s:%%s"):format(name or "NONAME", indexType))}, model.redis)
	
	return setmetatable({
		model = Index,
		update = function(self, id, newval, oldval)
			local id = assert(self:getId(), "id must be given")
			if(oldval~=nil) then
				self(oldval):remove(id):save()
			end
			if(newval~=nil) then
				self(newval):add(id):save()
			end
			return self
		end,
		getSet = function(self, val)
			return self.model:new(nil, hash(val), false)
		end,
	}, {__call = function(self, ...)
		return self:getSet(...) 
	end })
end	
