require "redis"
require "lohm"

function dump(t)
	for i, v in pairs(t) do
		print(i, v)
	end
end

local function newr()
	local redis = Redis.connect()
	redis:select(14)
	redis:flushdb()
	return redis
end
--[[
function assert_type(var, typ)
	return assert(type(var)==typ, "wrong type")
end
function assert_true(...)
	for i, v in pairs{...} do
		assert(v and v)
	end
end
function context(lbl, tests)
	return tests()
end
function test(lbl, test)
	return test()
end
function assert_equal(a, ...)
	for i, b in pairs{...} do
		assert(a==b)
	end
	return true
end
]]
context("Initialization", function()
	test("Can use open redis connection", function()
        local redis = newr()
        assert(redis:ping())
		local res = assert(lohm.new({key="foo:%s"}, redis))
		assert_type(res,'table')
    end)

	test("Model / Data prototype init", function()
		local M = lohm.new({
			key="foobar:%s",
			object={
				getFoo = function(self)
					return self.foo
				end
			},
			model={
				getFirst = function(self)
					return self:find(1)
				end
			}
		}, newr())
		assert_equal(M:new({foo=11}):getFoo(), 11)
		M:getFirst()
	end)
end)

context("Basic manipulation", function()
	test("insertion / autoincrement id counter / lookup by id / deletion", function()
		local Model = lohm.new({key="foo:%s"}, newr())
		local m = Model:new{foo='barbar'}
		m:save()
		
		local k, id = m:getKey(), m:getId()
		m:set('barbar','baz'):save()
		assert(m:getId()==id)
		
		local checkM = assert(Model:find(id))
		assert_true( checkM.barbar=="baz" )
		assert_true( "barbar"==checkM.foo )
		assert(checkM:delete())
		local notfound = Model:find(id)
		assert_true(not notfound)
	end)
end)

context("Indexing", function()
	test("Storage and Retrieval with hash index", function()
		local M = lohm.new({
			key="testindexing:%s",
			index = {"i1","i2", i3="hash"}
		}, newr())


		local findme=math.random(1,300)
		local find_id, findey
		for i=1, 300 do
			local m=assert(M:new{i1=math.random(1000), i2=math.random(1000), i3=math.random(1,2)}:save())
			if i==findme then
				findey, find_id=m, m:getId()
			end
		end
		assert_equal(#assert(M:findByAttr{i3=1})+#assert(M:findByAttr{i3=2}), 300)
		local res = M:find(findey)
		assert_equal(#res, 1)
		for i, v in pairs(res[1]) do
			assert_equal(tostring(v), tostring(findey[i]))
		end
	end)
end)
