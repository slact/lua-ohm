local print, type, assert, pcall, require, table, tostring, pairs, ipairs, setmetatable = print, type, assert, pcall, require, table, tostring, pairs, ipairs, setmetatable
local error = error
module("lohm.index", package.seeall)
local indexf = "lohm.index:%s:%s:%%s"

local indices = {}

--hash index: lookup by hashed value
do
	local s,  sha1 = pcall(require, "sha1")
	if s then
		hash = sha1.digest
	else
		local s, crypto = pcall(require, "crypto")
		if not s then 
			error("Can't find a sha1 library (tried sha1 (lmd5) and crypto (LuaCrypto))")
		else
			local digest = (require "crypto.evp").digest
			hash = function(input)
				return digest("sha1", input)
			end
		end
	end

	indices.hash = {
		update = function(self, redis, newval, oldval)
			local id = assert(self:getId(), "id must be given")
			if(oldval~=nil) then
				redis:srem(self:getKey(oldval), id)
			end
			if(newval~=nil) then
				redis:sadd(self:getKey(newval), id)
			end
			return self
		end,

		getKey = function(self, val)
			return self.keyf:format(hash(val))
		end
	}
end

function allIndices(sep)
	local t = {}
	for i, v in pairs(indices) do
		table.insert(t, i)
	end
	return table.concat(t, sep or ", ")
end

function getDefault()
	return "hash"
end

function new(self, indexType, model, attr)
	if not indices[indexType] then
		error(("Unknown index '%s'. Known indices: %s."):format(tostring(indexType), allIndices()))
	end
	assert(type(attr)=='string', 'What do you want indexed? (attr parameter is incorrect)')
	return setmetatable({
		keyf = indexf:format(model:key(indexType), attr)
	}, {__index=indices[indexType]})
end	
