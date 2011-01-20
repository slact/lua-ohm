local function I(...)
	return ...
end
local Data = require "lohm.data"
local ref = require "lohm.reference"
local print = print
local debug = debug
local next, assert, coroutine, table, pairs, ipairs, type, setmetatable, require, pcall, io, tostring, math, unpack = next, assert, coroutine, table, pairs, ipairs, type, setmetatable, require, pcall, io, tostring, math, unpack
module "lohm.model"

local all_models = setmetatable({}, {__mode='k'})
function isModel(m)
	return all_models[m]
end

-- unique identifier generators
local newId = {
	autoincrement = function(model)
		local key = model:key("__id")
		return model.redis:incr(key)
	end,

	uuid = (function()
		local res, uuid, err = pcall(require, "uuid")
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
	
	for i, bits in pairs{64,160,256,1024} do
		local b = bits
		newId["random" .. tostring(bits)] = hexstr and function()
			return hexstr(b)
		end or function() error("Can't start random number generator") end
	end
	newId.random=newId.random256
end

local model_prototype
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
			res, newself = coroutine.yield()
			if type(newself)=='table' then
				self = newself
			end
		end
		if res then
			for i, id in pairs(res) do
				res[i]=self:findById(id)
			end
			return res
		else
			return nil, "Sort results missing... It's probably your fault."
		end
	end
	
	model_prototype = {
		reserveNextId = function(self, redis)
			if redis then
				return self:withRedis(redis, function(self)
					return newId.autoincrement(self)
				end)
			else
				return newId.autoincrement(self)
			end
		end,
		
		--@return (possibly empty) table of results
		find = function(self, arg)
			if type(arg)=="table" then
				return self:findByAttr(arg)
			else
				local res, err = self:findById(arg)
				return { res }
			end
		end,
		
		findOne = function(self, arg)
			local res, err = self:find(arg)
			if res and #res>0 then 
				return res[1] 
			else
				return nil, err
			end
		end,

		--@return found object or nil
		findById = function(self, id)
			local key = self:key(id)
			if not key then return 
				nil, "Nothing to look for" 
			end
			return self:load(id)
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
		end,

		withRedis = function(self, redis_client, callback)
			return callback(setmetatable({redis=redis_client}, {__index=self}))
		end
	}
end

function new(arg, redisconn)

	local model, object = arg.model or {}, arg.object or {}
	assert(type(arg.key)=='string', "Redis " .. (arg.type or "") .. " object Must Have a key parameter (something like 'foo:%s').")
	assert(redisconn, "Valid redis connection needed")
	assert(redisconn:ping())
	model.redis = redisconn --presumably an open connection

	local key = arg.key
	model.keyf = key --format-string for the key
	
	local idmatch = key:gsub("([%^%$%(%)%.%[%]%*%+%-%?])", "%%%1") --note that we aren't filtering the % char. because it's used in sprintf. 
	idmatch = ("^" .. idmatch .. "$"):format("(.*)")
	model.id = function(self, key)
		print(debug.traceback())
		return key:find(idmatch)
	end
	model.key = function(self, id)
		return key:format(id)
	end
	local newobject = Data[arg.type or "hash"](model, arg)
	model.new = function(self, res, id)
		return newobject(res or {}, id)
	end
	model.load = function(self, id)
		return newobject(nil, id, true)
	end
	
	all_models[model]=true
	
	return setmetatable(model, {
		__index = function(self, k)
			local k_type = type(k)
			if k_type == 'number' then
				-- MyModel[12] == MyModel:findById(12)
				return self:findById(k)
			elseif k_type == 'table' then
				-- MyModel[{foo='bar',bar="baz"}] == MyModel:findByAttr({foo='bar',bar="baz"})
				return self:findByAttr(k)
			else
				return model_prototype[k]
			end
		end
	})
end