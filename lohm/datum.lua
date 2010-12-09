
module "tredis.datum"

local datum_prototype = {
	save = function(self, what)
		local key = self:getKey() or self:getModel():reserveNextKey()
		assert(key, "Trying to save data without a key or key assignment scheme")

		local res, err		
		if not what then
			res, err = redis:hmset(key, self)
		elseif type(what)=='string' then
			res, err = redis:hset(key, what, self[what])
		elseif type(what)=='table' then
			for i, k in pairs(what) do
				delta[k]=self[k]
			end
			res, err = redis:hmset(key, delta)
		end
			
			
			return self
		else 
			return nil, err
		end
	end,

	delete = function(self)
		redis:del(self:getKey())
	end,

	get = function(self, attr)
		local res = rawget(self, attr) or redis:hget(self:getKey(), attr)
		if indexedBy(attr) then
			
		end
		return res
	end,
}


function new(prototype, model)
	
	
	return function()
end