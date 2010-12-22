local print, debug, type, error = print, debug, type, error
local assert, coroutine, table, pairs, ipairs = assert, coroutine, table, pairs, ipairs
local tcopy = function(t)
	local res = {}
	for i, v in pairs(t) do
		res[i]=v
	end
	return res
end
module ("lohm.reference", function(t)
	setmetatable(t, { __call = function(self, ...) return t.one(...) end })
	return t
end)

function one(model, cascade)
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

local incr_key = "reference:set"
local keyf = "reference:set:%s"
function many(model, cascade)
	local setId
	return {
		load = function(redis, self, key, attr)
			if not setId then return nil, err end
			local res, err = {}, nil
			local finishFindById = {}
			local results, err = redis:check_and_set(key, function(r)
				if not setId then setId=r:hget(key, attr) end
				if not setId then return end --no set here.
				local setKey = keyf:format(setId)
				r:watch(setKey)
				finishFromSetIds = assert(r:smembers(setKey))
				coroutine.yield()
				model:withRedis(r, function(model)
					for i, id in ipairs(finishFromSetIds) do
						table.insert(finishFindById, model:findByIdDelayed(id))
					end
				end)
			end)
			table.remove(results, 1)
			table.remove(results, 1)
			for i, v in ipairs(results) do
				results[i]=finishFindById[i](v)
			end
			if results then
				self[attr]=results
				return results
			else
				error(("Failed to load set %s: %s."):format(keyf:format(setId or "<?>"), err or "(?)"))
			end
		end,
		
		save = function(redis, self, key, attr, val)
			--this is gonna get a little messy.
			assert(type(val)=='table', "Attribute expected to be a table")
			local newKey
			if not setId then  --need a new set maybe?
				setId = assert(redis:hget(key, attr) or redis:incr(incr_key))
				newKey = true
			end
			local setKey = keyf:format(setId)
			redis:watch(setKey) --watch this one, too
			local oldset, newset = {}, {}
			for i, v in pairs(assert(redis:smembers(setKey))) do
				oldset[v]=true
			end
			for i,v in pairs(val) do
				newset[v]=v
			end
			--cascading deletions
			local coros = {}
			if(cascade) then
				for i, v in pairs(oldset) do
					if not newset[i] then
						local obj = assert(model:findById(i))
						local c = coroutine.create(obj:save_coroutine())
						assert(coroutine.resume(c, redis))
						table.insert(coros, c)
					end
				end
			end
			
			coroutine.yield()
			
			--finish up cascading deletion coroutines.
			for i, coro in pairs(coros) do
				if coroutine.status(coro)=='suspended' then
					assert(coroutine.resume(coro))
				end
			end
			
			for id,v in pairs(oldset) do
				if not newset[id] then
					redis:srem(setKey, id)
				else --no change
					newset[id]=nil
				end
			end
			for i,obj in pairs(newset) do
				redis:sadd(setKey, obj:getId())
			end
			if newKey then redis:hset(key, attr, setId) end
		end, 
		
		delete = function(redis, self, key, attr)
			local val = self[attr]
			assert(type(val)=='table', "Attribute expected to be a table")
			if not setId then  --need a new set maybe?
				setId = redis:hget(key, attr)
			end
			if not setId then return nil end --there's really nothing to delete.

			local setKey = keyf:format(setId)
			redis:watch(setKey) --watch this one, too
			local oldmembers = redis:smembers(setKey)
			
			local coros = {}
			if cascade then
				for i,id in pairs(oldmembers) do
					local obj = assert(model:findById(id))
					local c = coroutine.create(obj:delete_coroutine())
					assert(coroutine.resume(c, redis))
					table.insert(coros, c)
				end
			end

			coroutine.yield()

			for i, coro in pairs(coros) do
				if coroutine.status(coro)=='suspended' then
					assert(coroutine.resume(coro))
				end
			end
			
			redis:del(setKey)
		end
	}
end
