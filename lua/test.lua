local Sovoc = require "sovoc"
local mp = require 'MessagePack'
local md5 = require 'md5'
local uuid = require "lua_uuid"

-- db = Sovoc:new{database='mydb.db'}
-- db:setup()

function gen_revid(generation, body)
  local id = body._id
  if id then
    body._id = nil
  end
  local rev = body._rev  
  if rev then
    body._rev = nil
  end
  
  local digest = md5.sumhexa(mp.pack(body))
  
  if id then
    body._id = id
  end
  if rev then
    body._rev = rev
  end
  return string.format("%d-%s", generation, digest)
end    

function gen_docid()
  return md5.sumhexa(uuid())
end

local doc = { _id = "1-3b05f7e89fa0c3a4587dd741c67fd782", a = "hello", b = "world" }
print(gen_revid(1, doc))

print(gen_docid())