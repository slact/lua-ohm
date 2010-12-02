require "ohm.object"
module("ohm.model")

function new(keyspace, obj)
	
  	
		insert = function(self)
			assert(self, "did you call foo.insert() instead of foo:insert()?")
			if self:getId() then 
				return nil, self:getKey() .. " already exists."
			else
				local newId, err = redis:increment(autoincr_key)
				if not newId then return nil, err end
				self:setKey(newId)
				self._created = os.time()
				return self:update()
			end
		end,
		
		update = function(self, what)
			local key = self:getKey()
			local res, err
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
		end
  
end,