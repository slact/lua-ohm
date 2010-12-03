local rawset, rawget, assert, pairs, ipairs = rawset, rawget, assert, pairs, ipairs
local redis=require "ohm.redis"
module("ohm.datum")

local datum_methods = {
	load = function(self, what)
		if not what then --load the whole damn thing
			local res = assert(redis:hget(getKey(self), what))
			rawset(self, what, res)
		else
			local res = assert(redis:hgetall(getKey(self)))
			for k,v in pairs(res) do
				rawset(self, k, v)
			end
		end
		return self
	end,
	
	save = function(self, what)
		local key = getKey(self)
		local res, err
		
		local myModel = self:getModel()
		
		--this needs to be some sort of transaction or another.
		for key, index in pairs(myModel:getIndices()) do
			if(rawget(self[key])) then
				index:update(key, self[key], self:getSavedValue(key))
			end
		end

		if not what then
			res, err = redis:hmset(key, self)
		elseif type(what)=='string' then
			res, err = redis:hset(key, what, self[what])
		elseif type(what)=='table' then
			local delta = {}
			for i, k in pairs(what) do
				delta[k]=self[k]
			end
			res, err = redis:hmset(key, delta)
		end
		if res then 
			return self
		else 
			return nil, err
		end
	end,

	delete = function(self)
		
		
	end,

	getKey = getKey
} 


local metadatum =  { {
	__index = function(self, k, v)
		local f 


