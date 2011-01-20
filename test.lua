require "redis"
require "lohm"
require "debug"
function debug.dump(tbl)
	local function tcopy(t) local nt={}; for i,v in pairs(t) do nt[i]=v end; return nt end
	local function printy(thing, prefix, tablestack)
		local t = type(thing)
		if     t == "nil" then return "nil"
		elseif t == "string" then return string.format('%q', thing)
		elseif t == "number" then return tostring(thing)
		elseif t == "function" then
			local info = debug.getinfo(thing)
			local source = info.source
			if info.linedefined then
				source = source .. ":" .. info.linedefined .. " - " .. info.lastlinedefined
			end
			if info.name then
				return ("%s %s: %s"):format(info.namewhat, info.name, source)
			else
				return source
			end
		elseif t == "table" then
			if tablestack and tablestack[thing] then return string.format("%s (recursion)", tostring(thing)) end
			local kids, pre, substack = {}, "	" .. prefix, (tablestack and tcopy(tablestack) or {})
			substack[thing]=true	
			for k, v in pairs(thing) do
				table.insert(kids, string.format('%s%s= %s,',pre,printy(k, ''),printy(v, pre, substack)))
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

if not telescope then
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

	function assert_false(...)
		for i,v in pairs{...} do
			assert(not v)
		end
	end
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
					return self:findOne(1)
				end
			}
		}, newr())
		assert_equal(M:new({foo=11}):getFoo(), 11)
		assert_true(lohm.isModel(M))
		M:getFirst()
	end)
end)

context("Basic manipulation", function()
	test("insertion / autoincrement id counter / lookup by id / deletion", function()
		local redis = newr()
		local Model = lohm.new({key="foo:%s"}, redis)
		local m = Model:new{foo='barbar'}
		m:save()
		local k, id = m:getKey(), m:getId()
		m:set('barbar','baz'):save()
		assert(m:getId()==id)
		local checkM = assert(Model:findOne(id))
		assert_true( checkM.barbar=="baz" )
		assert_true( "barbar"==checkM.foo )
		assert(checkM:delete())
		local notfound = Model[tonumber(id)]
		debug.print("NOTFOUND", type(notfound), notfound)
		assert_true(not notfound)
	end)
end)

context("Sets", function()
	test("Rudimentary set manipulation", function()
		local r = newr()
		local Set = lohm.set({key="setty:%s"}, r)
		s=Set:new():add('foo', "bar","baz")
		s:save()
		local setId = s:getId()
		local sprime = Set[setId]
		assert_true(r:sismember(sprime:getKey(), 'foo'))
		assert_false(r:sismember(sprime:getKey(), 'bax'))
		s:remove("foo", "bax")
		s:save()
		assert_equal(#s, 2)
		assert_equal(#Set[setId], 2)
	end)

	test("Sets as references", function()
		local r = newr()
		local Foo = lohm.new({key="foohash:%s"}, r)
		local Set = lohm.new({key="refset:%s", type='set', reference = Foo}, r)
		local s = Set:new()
		table.insert(s, Foo:new({bar='baz'}))
		s:save()
		os.exit()
	end)
end)
--[[
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
]]
context("References", function()
	test("direct reference manipulation", function()
		local r = newr()
		local Moo = lohm.new({key="moo:%s"}, r)
		local Thing = lohm.new({
			key="thing:%s",
			attributes = {
				moo = Moo
			}
		}, r)
		
		local t = Thing:new{ foo="bar" }
		local m = Moo:new({ bar="baz" }):save()
		t.moo = m
		t:save()
		
		local t1 = Thing:findOne(t:getId())
		assert_equal(t1.moo:getId(), '1')

		t1.moo.bar="17"
		t1:save()
		
		local t1prime = Thing:findOne(t1:getId())
		debug.print(t1prime, t1prime.moo.bar, t1prime.moo:getId(), "AAH")
		assert_equal(t1prime.moo.bar, t1.moo.bar)

		t1:delete()
		assert_false(Thing[tonumber(t:getId())])

		local t2 = Thing:new({z="9", moo=Moo:new({aux='bax'})}):save()
	end)

	test("Deletion", function()
		os.exit()
		local r = newr()
		local Foo = lohm.new({key="foo:%s"}, r)
		local Bar = lohm.new({key="bar:%s", 
			attributes={ 
				foo = Foo
			}
		}, r)
		
		local HardBar = lohm.new({key="hardBar:%s",
			attributes = {
				foo = Foo
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
		assert_equal(Bar:findOne(ids.bar):getId(), bar:getId())
		bar:delete()
		assert_true(not Bar:findOne(ids.bar))
		assert(Foo:findOne(ids.foo1))
		assert_equal(HardBar:findOne(ids.hardBar).foo.attr, foo2.attr)
		hardBar:delete()
		
		assert_true(not HardBar:findOne(ids.hardBar))
		assert_true(not Foo:findOne(ids.foo2))
		assert(Foo:findOne(ids.foo1):getId())
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
		assert_equal(foo:save(), foo)

		local foo_take2 = Foo:findOne(foo:getId())
		assert_equal(foo_take2:getId(), foo:getId())
		for i,v in pairs(foo_take2.manyBar) do
			assert_equal(tostring(v.woo), tostring(bars[tonumber(v.woo)].woo))
		end
	end)
end)
