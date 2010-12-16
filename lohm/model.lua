local function I(...)
	return ...
end
local Datum = require "lohm.datum"
local Index = require "lohm.index"
local next, assert, coroutine, table, pairs, ipairs, type, setmetatable = next, assert, coroutine, table, pairs, ipairs, type, setmetatable
local print = print
module "lohm.model"

-- unique identifier generators
local newId = {
	autoincrement = function(model)
		local key = ("%s:autoincrement"):format(model:key("id"))
		return model.redis:incr(key)
	end,

	uuid = function()
		local res, uuid, err = pcall(require "uuid")
		if not res then 
			return function()
				error("UUID lua module not found.")
			end
		else
			return uuid.new
		end
	end
}

local modelmeta
do
	local function fromSort_general(delay, self, key, pattern, maxResults, offset, descending, lexicographic)
		local res, err = assert(self.redis:sort(key, {
			by=pattern or "nosort", 
			get="# GET " .. self:key("*"),  --oh the ugly!
			sort=descending and "desc" or nil, 
			alpha = lexicographic or nil,
			limit = maxResults and { offset or 0, maxResults }
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
			local indices, indextable = self.indices, {}
			for attr, val in pairs(arg) do
				local thisIndex = indices[attr]
				assert(thisIndex, "model attribute " .. attr .. " isn't indexed. index it first, please.")
				indextable[thisIndex]=val
			end

			local lazy = false

			local randomkey = self.redis:randomkey()
			local finishFromSet
			local res, err = assert(self.redis:transaction(function(r)
				for index, value in pairs(indextable) do
					r:sunionstore(randomkey, index:getKey(value))
				end
				if not lazy then
					finishFromSet = self:fromSetDelayed(randomkey, limit, offset)
					r:del(randomkey)
				end
			end))
			if not lazy then
				res, err = finishFromSet(res[#res])
			else
				res, err = self:fromSetLazily(randomkey)
			end
			return res, err
			
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
			print(wrapper, type(wrapper))
			print(wrapper(true, self, setKey, nil, maxResults, offset, descending, lexicographic))
			return wrapper
		end, 

		fromSet = function(self, ...)
			return self:fromSetDelayed(self, ...)()
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
	
	local newobject = Datum.new(object, model)
	model.new = function(self, res, id)
		return newobject(res or {}, id)
	end

	
	return setmetatable(model, modelmeta)
end
