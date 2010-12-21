require "redis"
require "lohm"
function debug.dump(tbl)
	local function tcopy(t) local nt={}; for i,v in pairs(t) do nt[i]=v end; return nt end
	local function printy(thing, prefix, tablestack)
		local t = type(thing)
		if     t == "nil" then return "nil"
		elseif t == "string" then return string.format('%q', thing)
		elseif t == "number" then return tostring(thing)
		elseif t == "table" then
			if tablestack and tablestack[thing] then return string.format("%s (recursion)", tostring(thing)) end
			local kids, pre, substack = {}, "	" .. prefix, (tablestack and tcopy(tablestack) or {})
			substack[thing]=true	
			for k, v in pairs(thing) do
				table.insert(kids, string.format('%s%s=%s,',pre,printy(k, ''),printy(v, pre, substack)))
			end
			return string.format("%s{\n%s\n%s}", tostring(thing), table.concat(kids, "\n"), prefix)
		else
			return tostring(thing)
		end
	end
	local ret = printy(tbl, "", {})
	return ret
end

function debug.print(...)
	local buffer = {}
	for i, v in pairs{...} do
		table.insert(buffer, debug.dump(v))
	end
	local res = table.concat(buffer, "	")
	print(res)
	return res
end

local function newr()
	local redis = Redis.connect()
	redis:select(14)
	redis:flushdb()
	return redis
end

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

context("References", function()
	test("Making a reference", function()
		local r = newr()
		local Moo = lohm.new({key="moo:%s"}, r)
		local Thing = lohm.new({
			key="thing:%s",
			attributes = {
				moo = lohm.reference.one(Moo)
			}
		}, r)
		
		local t = Thing:new{ foo="bar" }
		local m = Moo:new({ bar="baz" }):save()
		t.moo = m
		t:save()

		local t1 = Thing:find(t:getId())
		assert_equal(t1.moo:getId(), '1')

	end)

	test("Deletion", function()
		local r = newr()
		local Foo = lohm.new({key="foo:%s"}, r)
		local Bar = lohm.new({key="bar:%s", 
			attributes={ 
				foo = lohm.reference(Foo) 
			}
		}, r)
		
		local HardBar = lohm.new({key="hardBar:%s",
			attributes = {
				foo = lohm.reference(Foo, true)
			}
		}, r)

		local foo1 = Foo:new({attr="foo1"}):save()
		local foo2 = Foo:new({attr="foo2"}):save()
		local bar = Bar:new({foo=foo1, bar=11}):save()

		foo2.test="test"
		local hardBar = HardBar:new({foo=foo2, bar=9}):save()

		local ids = {}
		for i, v in pairs{foo1=foo1, foo2=foo2, hardBar=hardBar, bar = bar} do
			ids[i]=v:getId()
		end
		assert_equal(Bar:find(ids.bar):getId(), bar:getId())
		bar:delete()
		assert_true(not Bar:find(ids.bar))
		assert(Foo:find(ids.foo1))
		assert_equal(HardBar:find(ids.hardBar).foo.attr, foo2.attr)
		hardBar:delete()
		
		assert_true(not HardBar:find(ids.hardBar))
		assert_true(not Foo:find(ids.foo2))
		assert(Foo:find(ids.foo1):getId())
	end)

	test("One-to-many references", function()
		local r = newr()
		local Bar = lohm({key="bar:%s"}, r)
		local Foo = lohm({
			key="foo:%s",
			attributes= {
				manyBar=lohm.reference.many(Bar),
				oneBar=lohm.reference.one(Bar)
			}
		}, r)

		local bars = {}
		for i=1, 20 do
			local bar = Bar:new{woo=i}:save()
			table.insert(bars, bar)
		end

		local foo = Foo:new{oneBar = Bar:new({yes="no"}):save(), manyBar = bars}
		foo:save()
	end)
end)
