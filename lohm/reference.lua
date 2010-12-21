local print, debug, type = print, debug, type
local assert, coroutine, table = assert, coroutine, table 
module ("lohm.reference", function(t)
	setmetatable(t, { __call = function(self, ...) return t.new(...) end })
	return t
end)

function new(model, cascade)
	return {
		load = function(redis, self, key, attr)
			local id, err = redis:hget(key, attr)
			if not id then return nil, err end
			local res, err =  model:find(id)
			if res then
				self[attr]=res
				return res
			else
				error(("Failed loading %s: %s."):format(model:key(id), err))
			end
		end, 

		save = function(redis, self, key, attr, val)
			assert(model:modelOf(val), ("Not a lohm object, or object of unexpected model (type %s)."):format(type(val)))
			if(cascade) then
				local c = coroutine.create(val:save_coroutine())
				assert(coroutine.resume(c, redis))
				coroutine.yield()
				if(coroutine.status(c)=='suspended') then
					assert(coroutine.resume(c))
				end
			else
				coroutine.yield()
			end
			assert(redis:hset(key, attr, val:getId()))
		end,
		
		delete = function(redis, self, key, attr)
			if cascade and model:modelOf(self[attr]) then
				local c = coroutine.create(self[attr]:delete_coroutine())
				assert(coroutine.resume(c, redis))
				coroutine.yield()
				if(coroutine.status(c)=='suspended') then
					assert(coroutine.resume(c))
				end
			end
		end
	}
end
