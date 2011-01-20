local print, getmetatable, rawget = print, getmetatable, rawget
local pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next = pairs, ipairs, table, error, setmetatable, assert, type, coroutine, unpack, next
local require, rawset = require, rawset

local tinsert = table.insert
local tslice = function(orig, first, last)
	local copy = {}
	for i=first, last do
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
				return function(model, arg)
					local res = self.new(k, model, arg)
					return res
				end
			elseif datatypes[k]==false then
				error(("%s is an invalid redis data type, or it hasn't been implemented in lohm yet."):format(k))
			else
				error("Leaky " .. k)
			end
		end
	})
end)

local ccreate, cresume, cstatus = coroutine.create, coroutine.resume, coroutine.status

function transactionize(self, redis, callbacks)
	debug.print(callbacks)
	local transaction_coroutines = {} --TODO: reuse this table, memoize more, etc.
	for i,naked_callback in pairs(callbacks) do
		table.insert(transaction_coroutines, ccreate(naked_callback))
	end
	local my_key, my_id = (self:getKey()), (self:getId())
	
	local i
	--transaction function
	local queued_commands_offset = {}
	local success, last_ret, last_err 
	local res, err = redis:transaction({cas=true, watch=self:getKey()}, function(redis)
		--WATCH ...
		i=1
		while i<=#transaction_coroutines do
			local transaction_callback = transaction_coroutines[i]
			success, last_ret, last_err = assert(cresume(transaction_callback, self, redis, my_id))
			print("PREMULTI", success, last_ret, last_err)
			if cstatus(transaction_callback)~='dead' then
				i = i + 1
			else
				table.remove(transaction_coroutines, i)
			end
		end
		
		redis:multi()
		i=1
		while i<=#transaction_coroutines do
			local transaction_callback = transaction_coroutines[i]
			local already_queued = redis:commands_queued()
			success, last_ret, last_err = assert(cresume(transaction_callback))
			print("MULTI    ", success, last_ret, last_err)
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
		success, last_ret, last_err = assert(cresume(transaction_callback, tslice(res, unpack(queued_commands_offset[transaction_callback]))))
		print("POSTMULTI", success, last_ret, last_err)
		--we no longer care about the coroutine's status. we're done.
	end
	return last_ret, last_err
end



function new(datatype, model, arg)
	arg = arg or {}

	local ids = setmetatable({}, { __mode='k'})
	local keys = setmetatable({}, { __mode='k'})
	
	local callbacks = {load={},save={},delete={}}
	
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
		
		clearId = function(self)
			ids[self]=nil
			keys[self]=nil
			return self
		end,
		clearKey = function(self) return self:clearId() end,
		
		
		getCallbacks = function(self, event_name)
			return callbacks[event_name]
		end, 
		
		addCallback = function(self, event_name, callback, bind_to)
			if not callback then return nil, "nothing to add" end
			local cb = callbacks[event_name]
			if type(bind_to)=='table' then
				local true_callback = callback
				callback = function(self, ...)
					return true_callback(bind_to, ...)
				end
			end
			local oldcb = #cb
			table.insert(cb, callback)
			return self
		end,
		
		addCallbacks = function(self, event_name, callbacks, bind_to)
			for i, cb in pairs(callbacks) do
				assert(self:addCallback(event_name, cb))
			end
			return self
		end,
		
		getId = function(self)
			return ids[self]
		end,
		
		getModel = function(self)
			return model
		end
	}
	for i, operation in pairs{'load', 'delete', 'save'} do
		data_prototype[operation]=function(self, redis)
			print(operation, "IN PROGRESS")
			local key = self:getKey()
			if not key and operation=='save' then
				self:setId(model:reserveNextId(redis))
				key = self:getKey()
			end
			if not key then error(("Cannot %s data without a key"):format(operation)) end
			local res, err = transactionize(self, model.redis, self:getCallbacks(operation))
			return (res and self), err
		end
	end
	
	--merge custom object properties into the prototype
	for i,v in pairs(arg.prototype or arg.object or {}) do
		if not rawget(data_prototype, i) then
			data_prototype[i]=v
		else
			error(("'%s' is reserved and can't be customized."):format(i))
		end
	end
	
	local obj = require("lohm." .. datatype)
	return obj.initialize(data_prototype, arg)
end