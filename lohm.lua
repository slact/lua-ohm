local Model = require "lohm.model"

module ("lohm", function(t)
	setmetatable(t, { 
		__call = function(self, ...) return t.new(...) end,
		__index = function(self, k)
			return function(arg, redis) 
				arg.type=k
				return Model.new(arg, redis)
			end
		end
	})
end)

--arguments:
	-- key (string/function): sprintf-able string or a function to generate a key. something like "myfoo:%s"
	-- model (table): extra functions etc. belonging to the model
	-- object (table): extra functions  etc. for objects
	-- indices (table): attributes to index (and with what index, if given. defaults to Index:defaultIndex()

function new(arg, redis_connection)
	local res, err Model.new(arg, redis_connection)
	return res, err
end

function isModel(model)
	return Model:isModel(model)
end