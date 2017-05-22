#!/usr/bin/env luajit

local luaunit = require 'luaunit'
local Sovoc = require 'sovoc'
local json = require 'cjson'

local db = nil

local function has_value (tbl, val)
    for index, value in ipairs(tbl) do
        if value == val then
            return true
        end
    end

    return false
end

local function tprint (tbl, indent)
  if not indent then indent = 0 end
  for k, v in pairs(tbl) do
    formatting = string.rep("  ", indent) .. k .. ": "
    if type(v) == "table" then
      print(formatting)
      tprint(v, indent+1)
    else
      print(formatting .. v)
    end
  end
end

TestCRUD = {}
  function TestCRUD:setUp()
    db = Sovoc:new{database=':memory:'}
    db:setup()
  end

  function TestCRUD:tearDown()

  end

  function TestCRUD:test_bulk()
    local result = db:bulk{
      {name='adam'},
      {name='bob'},
      {name='charlie'},
      {name='danni'},
      {name='eve'},
      {name='frank'}
    }
    luaunit.assertEquals(#result, 6)
  end

  function TestCRUD:testReadDocument()
    local result = db:insert{doc={hello='read'}}
    print(unpack(result))
    local doc = db:get{_id=result.id, _rev=result.rev}

    luaunit.assertEquals(result.id, doc._id)
    luaunit.assertEquals(result.rev, doc._rev)
    luaunit.assertEquals(doc.hello, 'read')
  end

  function TestCRUD:testDeleteDocument()
    local status = db:insert{doc={hello='delete'}}
    local result = db:destroy{_id=status.id, _rev=status.rev}
    luaunit.assertNotNil(result.id)
    luaunit.assertNotNil(result.rev)
    luaunit.assertEquals(status.id, result.id)
    luaunit.assertNotEquals(status.rev, result.rev)
  end

-- end of table TestCrud

TestMulti = {}
  function TestMulti:setUp()
    db = Sovoc:new{database=':memory:'}
    db:setup()
  end

  function TestMulti:tearDown()
    
  end

  function TestMulti:test_open_revs()
    local result1 = db:insert{doc={name='stefan'}}
    local result2 = db:insert{doc={name='stefan astrup'}, _id=result1.id, _rev=result1.rev}
    local result3 = db:insert{doc={name='stef'}, _id=result1.id, _rev=result1.rev}
    local result4 = db:insert{doc={name='steffe'}, _id=result1.id, _rev=result1.rev}
    local result5 = db:insert{doc={name='stefan astrup kruger'}, _id=result1.id, _rev=result2.rev}

    local result = db:open_revs{_id=result1.id}

    luaunit.assertEquals(#result, 3)
    luaunit.assertEquals(result[1].ok._revisions.start, 3)
    luaunit.assertEquals(result[2].ok._revisions.start, 2)
    luaunit.assertEquals(result[3].ok._revisions.start, 2)
  end
  
  function TestMulti:test_changes()
    local result1 = db:insert{doc={name='stefan'}}
    local result2 = db:insert{doc={name='stefan astrup'}, _id=result1.id, _rev=result1.rev}
    local result3 = db:insert{doc={name='stef'}, _id=result1.id, _rev=result1.rev}
    local result4 = db:insert{doc={name='steffe'}, _id=result1.id, _rev=result1.rev}
    local result5 = db:insert{doc={name='stefan astrup kruger'}, _id=result1.id, _rev=result2.rev}

    local i = 0 -- total
    local bookmark = nil

    local changes = db:changes()

    -- tprint(changes, 2)
    for _, entry in ipairs(changes) do
      if i == 2 then -- pick number 2
        bookmark = entry.seq
      end
      i = i + 1
    end

    luaunit.assertEquals(i, 5) -- documents added above

    -- fast-forward to bookmark @ 2
    changes = db:changes{seq=bookmark}
    local j = 0
    for _, entry in ipairs(changes) do
      j = j + 1
    end

    luaunit.assertEquals(j, i - 3) -- total - remainder - "fence post" @ 2
  end
  
  function TestMulti:test_co_changes()
    local result1 = db:insert{doc={name='stefan'}}
    local result2 = db:insert{doc={name='stefan astrup'}, _id=result1.id, _rev=result1.rev}
    local result3 = db:insert{doc={name='stef'}, _id=result1.id, _rev=result1.rev}
    local result4 = db:insert{doc={name='steffe'}, _id=result1.id, _rev=result1.rev}
    local result5 = db:insert{doc={name='stefan astrup kruger'}, _id=result1.id, _rev=result2.rev}
      
    local i = 0 -- total 
    local bookmark = nil
    
    for j, entry in db:iterate_changes() do
      if j == 2 then -- pick number 2
        bookmark = entry.seq
      end
      i = i + 1
    end
    
    luaunit.assertEquals(i, 5) -- documents added above

    local j = 0
    for _, entry in db:iterate_changes{seq=bookmark, include_docs=true} do
      j = j + 1
    end
    
    luaunit.assertEquals(j, i - 2) -- total - remainder - "fence post" @ 2
  end
  
-- end of table TestMulti

TestRevsdiff = {}
  function TestRevsdiff:setUp()
    db = Sovoc:new{database=':memory:'}
    db:setup()
  end

  function TestRevsdiff:tearDown()
    
  end

  function TestRevsdiff:test_static()
    local result1 = db:insert{doc={name='stefan'}}
    local result2 = db:insert{doc={name='stefan astrup'}, _id=result1.id, _rev=result1.rev}
    local result3 = db:insert{doc={name='stef'}, _id=result1.id, _rev=result1.rev}
    local result4 = db:insert{doc={name='steffe'}, _id=result1.id, _rev=result1.rev}
    local result5 = db:insert{doc={name='stefan astrup kruger'}, _id=result1.id, _rev=result2.rev}

    local state = {}
    local fakerev = '6-7069a4a13b158f71082ae633266be1b5'
    
    state[result1.id] = {
      result1.rev,
      result2.rev,
      result3.rev,
      result4.rev,
      result5.rev,
      fakerev
    }

    local revsdiff = db:revs_diff(state)

    luaunit.assertEquals(revsdiff[1]._id, result1.id)
    luaunit.assertEquals(revsdiff[1]._rev, fakerev)
    luaunit.assertEquals(#revsdiff, 1)
  end

  function TestRevsdiff:test_co_revs_diff()
    local result1 = db:insert{doc={name='stefan'}}
    local result2 = db:insert{doc={name='stefan astrup'}, _id=result1.id, _rev=result1.rev}
    local result3 = db:insert{doc={name='stef'}, _id=result1.id, _rev=result1.rev}
    local result4 = db:insert{doc={name='steffe'}, _id=result1.id, _rev=result1.rev}
    local result5 = db:insert{doc={name='stefan astrup kruger'}, _id=result1.id, _rev=result2.rev}

    local state = {}

    local fakes = {'6-7069a4a13b158f71082ae633266be1b5', '7-a91f697da19c4e454e809064fa6fb534', '8-b22f1606faf694810fd822de2b9929b3'}
    
    state[result1.id] = {
      result1.rev,
      result2.rev,
      result3.rev,
      result4.rev,
      result5.rev,
      fakes[1], 
      fakes[2],
      fakes[3]
    }

    local count = 0
    for _, entry in db:iterate_revs_diff(state) do
      luaunit.assertEquals(entry._id, result1.id)
      luaunit.assertTrue(has_value(fakes, entry._rev))
      count = count + 1
    end

    luaunit.assertEquals(count, #fakes)
  end
  -- end of table TestRevsdiff

os.exit(luaunit.LuaUnit.run())