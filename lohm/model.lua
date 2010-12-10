local Datum = require "lohm.datum"
local Index = require "lohm.index"
local assert, coroutine, table, pairs, ipairs, type = assert, coroutine, table, pairs, ipairs, type
module "lohm.model"

local function reserveId(self)
	if self.autoincrement ~= false then
		local res, err = self.redis:incr(autoincr_key)
		return res
	else
		return nil, "don't know how to autoincrement ids for key pattern " .. (keypattern or "???")
	end
end

local function modelcache

local function getKey(self, id)
	return modelcache[self]:getKey(id)
end

local modelmeta
do
	local function fromSort_general(delay, self, key, pattern, maxResults, offset, descending, lexicographic)
		local res, err = assert(self.redis:sort(key, {
			by=pattern or "nosort", 
			get="# GET " .. self:key("*"),  --oh the ugly!
			sort=descending and "desc" or nil, 
			alpha = lexicographic or nil,
			limit = maxResults and {offset or 0, maxResults}
		}))
		if delay then
			res, err = coroutine.yield()
		end
		if res then
			for i=0, #res, 2 do
				res[i+1]=self:new(res[i], res[i+1])
				table.remove(res, i)
			end
		else
			return nil, err or "unexpected thing cryptically happened..."
		end
	end
	
	modelmeta = { __index = {
		find = function(self, arg)
			if type(arg)=="table" then
				return self:findByAttr(arg)
			else
				return self:findById(arg)
			end
		end,
		
		findById = function(self, id)
			local key = getKey(self, id)
			if not key then return 
				nil, "Nothing to look for" 
			end
			local res, err = self.redis:hgetall(key)
			if res then
				return self:new(id, res)
			else
				return nil, "Not found."
			end
		end,

		findByAttr = function(self, arg, limit, offset)
			local indextable = {}
			for attr, val in pairs(arg) do
				local thisIndex = self.indices[attr]
				assert(thisIndex, "model attribute " .. attr .. " isn't indexed. index it first, please.")
				indextable[thisIndex]=val
			end
			return Index:lookup(self.redis, indextable, limit, offset)
			
		end,
		
		fromSortDelayed = function(self, key, pattern, maxResults, offset, descending, lexicographic)
			local wrapper = coroutine.wrap(fromSort_general)
			assert(wrapper(true, self, key, pattern, maxResults, offset, descending, lexicographic))
			return wrapper
		end, 

		fromSort = function(self, ...)
			return fromSort_general(self, ...)
		end,

		fromSetDelayed = function(self, setKey, maxResults, offset, descending, lexicographic)
			local wrapper = coroutine.wrap(fromSort_general)
			assert(wrapper(true, self, setKey, nil, maxResults, offset, descending, lexicographic))
			return wrapper
		end, 

		fromSet = function(self, ...)
			return self:fromSetDelayed(self, ...)()
		end, 

		key = function(self, id)
			return keymaker(id)
		end
	}}
end

function new(arg, redisconn)
	local model, object = arg.model or {}, arg.datum or arg.object or {}
	assert(redisconn:ping())
	model.redis = redisconn --presumably an open connection

	
	local key, keymaker = arg.key, nil
	assert(arg.key, "Redis object Must. Have. Key.")
	if type(key)=="string" then
		assert(key:format('foo')~=key:format('bar'), "Invalid key pattern string (\""..key.."\") produces same key for different ids.")
		keymaker = function(arg)
			return key:format(arg)
		end
	elseif type(key)=="function" then
		keymaker = key
	end

	model.indices = {}

	local indices = arg.index or arg.indices
	if indices and #indices>0 then
		
		local defaultIndex = Index:getDefault()
		for attr, indexType in pairs(indices) do
			if type(attr)~="string" then 
				attr, indexType = indexType, defaultIndex
			end
			mode.indices[attr] = Index:new(indexType, model, attr)
		end
	end
	
	object = Datum.new(datum, model)
	model.new = function(self, res, id)
		object:new(res or {}, id)
	end

	return setmetatable(model, modelmeta)
end



function setAutoIncrementKey(self, key)
	assert(type(key)=="string", "Autoincrement key must be a string")
	autoincr_key = key
	return self
end