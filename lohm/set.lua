module("lohm.set", function(t) setmetatable(t, {__index=function(self, ...)
	return self.new(...)
end}))

function new(model, attributes)
	local callbacks = attributes.callbacks

	local set_prototype = {
		save_transaction = function(self)
			local myKey = self:getKey()
			return functon(r)
				redis:multi()
				for i, v in ipairs(self.removing) do
					redis:srem(myKey, v)
				end
				for i,v in ipairs(self.adding) do
					redis:sadd(myKey, v)
				end
			end
		end
		
		delete_transaction = function(self)
			return function(r)
				return r:delete(self:getKey())
			end			
		end
	}

	local set_meta = {__index = set_prototype }

	return function(data, id, load_now)
		local obj = setmetatable(data or {}, set_meta)
		if id then
			if load_now then
				local set = model.redis:smembers(obj:getKey())
				for i,v in ipairs(set) do --Need this be a loop?
					table.insert(obj, v)
				end
				if callbacks.load then 
					callbacks.load(obj) 
				end
			end
		end
	end, set_prototype
	
end