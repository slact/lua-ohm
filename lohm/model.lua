local function I(...)
	return ...
end
local Datum = require "lohm.datum"
local Index = require "lohm.index"
local ref = require "lohm.reference"
local next, assert, coroutine, table, pairs, ipairs, type, setmetatable, require, pcall, io, tostring, math, unpack = next, assert, coroutine, table, pairs, ipairs, type, setmetatable, require, pcall, io, tostring, math, unpack
local print = print
module "lohm.model"

-- unique identifier generators
local newId = {
	autoincrement = function(model)
		local key = ("%s:autoincrement"):format(model:key("id"))
		return model.redis:incr(key)
	end,

	uuid = (function()
		local res, uuid, err = pcall(require "uuid")
		if not res then 
			return function()
				error("UUID lua module not found.")
			end
		else
			return uuid.new
		end
	end)()
}
do
	local s, mersenne_twister=pcall(require, "random")
	local hexstr
	if s and mersenne_twister then
		local os_entropy_fountain, err = io.open("/dev/urandom", "r") --quality randomness, please.
		local seed
		if os_entropy_fountain then
			local rstr = os_entropy_fountain:read(6) --48 bits, please.
			os_entropy_fountain:close()
			seed=0
			for i=0,5 do
				seed = seed + (rstr:byte(i+1) * 256^i) --note: not necessarily platform-safe...
			end
		else --we aren't in a POSIX world, are we. oh well.
			seed = os.time() + 1/(math.abs(os.clock()) +1)
		end
		assert(seed, "Invalid seed for id-making random number generator.")
		entropy_fountain = assert(mersenne_twister.new(seed), "Unable to start ID RNG (mersenne twister)")
		
		hexstr=function(bits)
			local t = {}
			while bits > 0 do
				bits = bits-32
				table.insert(t, ("%08x"):format(entropy_fountain(0, 0xFFFFFFFF))) --2^32-1
			end
			return table.concat(t):sub(1, math.ceil(bits/4)-1)
		end
	end
	
	for i, bits in pairs{32,64,128,256,1024} do
		local b = bits
		newId["random" .. tostring(bits)] = hexstr and function()
			return hexstr(b)
		end or function() error("Can't start random number generator") end
	end
	newId.random=newId.random256
end

local modelmeta
do
	local function fromSort_general(self, key, pattern, maxResults, offset, descending, lexicographic)
		local res, err = self.redis:sort(key, {
			by=pattern or "nosort", 
			get="#",  --oh the ugly!
			sort=descending and "desc" or nil, 
			alpha = lexicographic or nil,
			limit = maxResults and { offset or 0, maxResults }
		})
		if type(res)=='table' and res.queued==true then
			res, err = coroutine.yield()
		end
		if res then
			for i, id in pairs(res) do
				res[i]=self:findById(id)
			end
			return res
		else
			return nil, err or "unexpected thing cryptically happened..."
		end
	end
	
	modelmeta = { __index = {
		reserveNextId = function(self)
			return newId.autoincrement(self)
		end,

		find = function(self, arg)
			if type(arg)=="table" then
				return  self:findByAttr(arg)
			else
				return self:findById(arg)
			end
		end,
		
		findById = function(self, id)
			local key = self:key(id)
			if not key then return 
				nil, "Nothing to look for" 
			end
			local res, err = self.redis:hgetall(key)
			if res and next(res) then
				return self:new(res, id)
			else
				return nil, "Not found."
			end
		end,

		findByAttr = function(self, arg, limit, offset)
			local indices = self.indices
			local sintersect = {}
			for attr, val in pairs(arg) do
				local thisIndex = indices[attr]
				assert(thisIndex, "model attribute " .. attr .. " isn't indexed. index it first, please.")
				table.insert(sintersect, thisIndex:getKey(val))
			end
			
			local lazy = false
			local randomkey = "searchunion:" .. newId.random()
			self.redis:sinterstore(randomkey, unpack(sintersect))
			local res, err = self:fromSet(randomkey, limit, offset)
			--self.redis:del(randomkey)
			return res, err
		end,
		
		fromSortDelayed = function(self, key, pattern, maxResults, offset, descending, lexicographic)
			local wrapper = coroutine.wrap(fromSort_general)
			assert(wrapper(self, key, pattern, maxResults, offset, descending, lexicographic))
			return wrapper
		end, 

		fromSort = function(self, ...)
			return fromSort_general(self, ...)
		end,

		fromSetDelayed = function(self, setKey, maxResults, offset, descending, lexicographic)
			local wrapper = coroutine.wrap(fromSort_general)
			wrapper(self, setKey, nil, maxResults, offset, descending, lexicographic)
			return wrapper
		end, 

		fromSet = function(self, setKey, maxResults, offset, descending, lexicographic)
			return fromSort_general(self, setKey, nil, maxResults, offset, descending, lexicographic)
		end,

		modelOf = function(self, obj)
			if type(obj)=='table' and obj.getModel then
				local s, res, err = pcall(obj.getModel, obj)
				return s and res==self, err
			end
			return false
		end
	}}
end

function new(arg, redisconn)

	local model, object = arg.model or {}, arg.datum or arg.object or {}
	assert(type(arg.key)=='string', "Redis object Must. Have. Key.")
	assert(redisconn, "Valid redis connection needed")
	assert(redisconn:ping())
	model.redis = redisconn --presumably an open connection

	local key = arg.key
	model.key = function(self, id)
		return key:format(id)
	end

	model.indices = {}
	local indices = arg.index or arg.indices
	if indices and #indices>0 then
		local defaultIndex = Index:getDefault()
		for attr, indexType in pairs(indices) do
			if type(attr)~="string" then 
				attr, indexType = indexType, defaultIndex
			end
			model.indices[attr] = Index:new(indexType, model, attr)
		end
	end
	
	local newobject = Datum.new(object, model, arg.attributes)
	model.new = function(self, res, id)
		return newobject(res or {}, id)
	end

	return setmetatable(model, modelmeta)
end
