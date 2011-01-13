local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next
local require, rawset = require, rawset

local tinsert = table.insert
local tslice = function(orig, first, last)
	local copy = {}
	while i=first, last do
		tinsert(copy, orig[i])
	end
	return copy
end

local debug = debug
local function I(...) return ... end
local datatypes = {hash=true, set=true, string=false, list=false, zset=false}
module ("lohm.data", function(t)
	setmetatable(t, { 
		__call = function(self, ...) return t.new(...) end, 
		__index = function(self, k)
			if datatypes[k] then
				return function(...) 
					return self.new(k, ...)
				end
			end
		end
	})
end)

local ccreate, cresume, cstatus = coroutine.create, coroutine.resume, coroutine.status

local function transactionize = function(self, callbacks, ...)
	local transaction_coroutines = {} --TODO: reuse this table, memoize more, etc.
	for i,naked_callback in pairs(callbacks) do
		table.insert(transaction_coroutines, ccreate(naked_callback))
	end
	local my_key = self:getKey()

	--transaction function
	local res, err = redis:transaction({cas=true, watch=self:getKey()}, function(redis)
		--WATCH ...
		while i<#transaction_coroutines do
			local transaction_callback = transaction_coroutines[i]
			assert(cresume(transaction_callback, redis, ...))
			if cstatus(transaction_callback)~='dead' then
				i = i + 1
			else
				table.remove(transaction_coroutines, i)
			end
		end
		
		redis:multi()
		
		local queued_commands_offset = {}
		while i<#transaction_coroutines do
			local transaction_callback = transaction_coroutines[i]
			local already_queued = redis:commands_queued()
			assert(cresume(transaction_callback))
			if cstatus(transaction_callback) ~= 'dead' then
				queued_commands_offset[transaction_callback]={ already_queued, redis:commands_queued() }
				i = i + 1
			else
				table.remove(transaction_coroutines, i)
			end
		end
	end)

	if not res then return nil, err end
	for i, transaction_callback in ipairs(transaction_coroutines) do
		cresume(transaction_callback, tslice(res, unpack(queued_commands_offset[transaction_callback])))
		--we no longer care about the coroutine's status. we're done.
	end
	return res
end


function new(datatype, model, ...)
	assert(datatypes[datatype], ("%s is an invalid redis data type, or it hasn't been implemented in lohm yet."):format(datatype))
	
	local ids = setmetatable({}, { __mode='k'})
	local keys = setmetatable({}, { __mode='k'})
	
	local callbacks = { load={},save={},delete={} }
	
	local data_prototype = {
		setId = function(self, id)
			if not ids[self] then
				ids[self]=id
				keys[self]=model:key(id)
			else
				error("Object id is already set (" .. ids[self] .. "). Can't change it -- yet.")
			end
			return self
		end,
		
		setKey = function(self, key)
			--sanity check first
			return self:setId(self, assert(model.id(key), "That looks like a mighty invalid key"))
		end,
		
		getKey = function(self)
			return keys[self]
		end,
		
		getCallbacks = function(self, event_name)
			return (callbacks[event_name] or {})[event_name]
		end, 
		
		addCallback = function(self, event_name, callback)
			if not callback then return nil, "nothing to add" end
			if not callbacks[event_name] then callbacks[event_name] = {} end
			local cb = callbacks[event_name]
			table.insert(cb, callback)
			return #
		end,
		
		getId = function(self)
			return ids[self]
		end,
		
		getModel = function(self)
			return model
		end, 
		
		save = function(self, what)
			local res, err = transactionize(self, 'save', what)
			if res then
				return self
			else 
				return nil, err
			end
		end,

		delete = function(self)
			local key = assert(self:getKey(), "Cannot delete without a key")
			local res, err = transactionize(self, 'delete')
			if not res or not res[#res] then error(err) end
			return self
		end
	}
	
	local obj = require("lohm." .. datatype)
	local newobj, obj_prototype, callbacks = obj.new(model, ...)
	--now the common object methods. we're copying values instead of using   metatables for runtime efficiency
	for k, v in pairs(data_prototype) do
		rawset(obj_prototype, k, v)
	end	
	
	for i,cbs in ipairs(callbacks) do
		obj:addCallback('save', cbs.save)
		obj:addCallback('load', cbs.load)
		obj:addCallback('delete', cbs.delete)
	end

	return newobj
end