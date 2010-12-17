local print = print
local assert, coroutine, table = assert, coroutine, table 
module ("lohm.reference", function(t)
	setmetatable(t, { __call = function(...) return t.new(...) end })
	return t
end)

function new(model, cascade)
	return {
		load = function(redis, self, key, attr)
			local id, err = redis:hget(key, attr)
			if not id then return nil, err end
			local res =  model:find(id)
			return res
		end, 

		save = function(redis, self, key, attr, val)
			if(cascade) then
				assert(model:modelOf(val), "Wrong object.")
				local c = coroutine.create(self:save_coroutine())
				coroutine.resume(c, redis)
				coroutine.yield()
				if(coroutine.status(c)=='suspended') then
					coroutine.resume(c)
				end
			else
				coroutine.yield()
			end
			print("HEYO")
			return redis:hset(key, attr, val:getId())
		end,
		
		delete = function(redis, self, key, attr)
			if cascade and model:modelOf(self[attr]) then
				return self:delete_coroutine()
			end
		end
	}
end
