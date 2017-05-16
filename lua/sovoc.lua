local sqlite3 = require "lsqlite3"
local uuid = require "lua_uuid"
local mp = require 'MessagePack'
local md5 = require 'md5'

local Sovoc = { database = nil }

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

function Sovoc:new(tbl) 
  tbl = tbl or {}
  setmetatable(tbl, self)
  self.__index = self
  assert(tbl.database)
  tbl.db = assert(sqlite3.open_memory())
  
  tbl.uuid = uuid()
  
  return tbl
end

function Sovoc:setup()
  assert(self.db:exec[[
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS documents (
      _id TEXT NOT NULL,
      _rev TEXT NOT NULL,
      _deleted INTEGER DEFAULT 0 CHECK (_deleted = 0 OR _deleted = 1),
      generation INTEGER DEFAULT 1 CHECK (generation > 0),
      leaf INTEGER DEFAULT 1 CHECK (leaf = 0 OR leaf = 1),
      body TEXT,
      UNIQUE (_id, _rev) ON CONFLICT IGNORE
    );
    CREATE TABLE IF NOT EXISTS ancestors (
        -- Table holding the document tree structure
        -- represented as a closure table. The 'depth'
        -- column holds the number of levels between
        -- ancestor and descendant
      ancestor INTEGER NOT NULL,
      descendant INTEGER NOT NULL,
      depth INTEGER NOT NULL CHECK (depth >= 0),
      FOREIGN KEY(ancestor) REFERENCES documents(rowid),
      FOREIGN KEY(descendant) REFERENCES documents(rowid)
    );
    CREATE TABLE IF NOT EXISTS changes (
      doc_row INTEGER NOT NULL,
      seq TEXT NOT NULL,
      FOREIGN KEY(doc_row) REFERENCES documents(rowid)
    );
    CREATE INDEX seq_idx ON changes (seq);
    CREATE VIEW changes_feed AS
      SELECT c.seq, d.rowid AS doc_row, d._deleted, d._id, d._rev
      FROM changes c, documents d
      WHERE c.doc_row = d.rowid
      ORDER BY d.rowid;
    COMMIT;
  ]])
end

-- function Cloudant:request(method, url, params, data)
--   local response_body = {}
--   local req = {
--     url = url,
--     method = method,
--     sink = ltn12.sink.table(response_body),
--     headers = self.cookie and {['Cookie'] = self.cookie} or {['Authorization'] = self.auth}
--   }
--
--   if data then
--     local jsonData = json.stringify(data)
--     req.source = ltn12.source.string(jsonData)
--     req.headers['Content-Type'] = 'application/json'
--     req.headers['Content-Length'] = jsonData:len()
--   end
--
--   -- print(dump(req))
--
--   local res, httpStatus, responseHeaders, status = http.request(req)
--   return json.parse(table.concat(response_body))
-- end
--
-- -- The CouchDB document API --
--
-- function Cloudant:bulkdocs(data, options)
--   return self:request('POST', self:url('_bulk_docs'), options, {docs=data})
-- end
--
-- function Cloudant:read(docid, options)
--   return self:request('GET', self:url(docid), options, nil)
-- end
--
-- function Cloudant:create(body, options)
--   local data = self:bulkdocs({body}, options)
--   return data[1]
-- end
--
-- function Cloudant:update(docid, revid, body, options)
--   body._id = docid
--   body._rev = revid
--   local data = self:bulkdocs({body}, options)
--   return data[1]
-- end


return Sovoc