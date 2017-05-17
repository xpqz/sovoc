local sqlite3 = require "lsqlite3"
local uuid = require "lua_uuid"
local mp = require 'MessagePack'
local md5 = require 'md5'
local json = require 'cjson'

local Sovoc = { database = nil }

local function gen_revid(generation, body)
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

local function gen_docid()
  return md5.sumhexa(uuid())
end

local function execute(arg)
  assert(arg.statement)
  assert(arg.data)
  
  arg.statement:reset()
  local status = arg.statement:bind_values(unpack(arg.data))
  if status ~= sqlite3.OK then
    error(string.format("An error occurred: %d", status))
  end
  
  -- option to execute insert, update and deletes directly
  if arg.step then
    status = arg.statement:step() 
    if status ~= sqlite3.DONE then
      error(string.format("An error occurred: %d", status))
    end
  end
  
  return true
end

local function fetchone(bound_statement)
  local result = nil
  for row in bound_statement:nrows() do
    result = row
    break
  end
  
  return result
end

function Sovoc:new(tbl) 
  tbl = tbl or {}
  setmetatable(tbl, self)
  self.__index = self
  assert(tbl.database)
  tbl.db = assert(sqlite3.open(tbl.database))
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

function Sovoc:bulk(docs)
  local insert_document = assert(self.db:prepare('INSERT INTO documents (_id, _rev, _deleted, generation, leaf, body) VALUES (?, ?, ?, ?, 1, json(?))'))
  local find_parent = assert(self.db:prepare('SELECT rowid, generation FROM documents WHERE _id=? AND _rev=? AND _deleted=0'))
  local get_last_insert = assert(self.db:prepare('SELECT last_insert_rowid()'))
  local ancestral_identity = assert(self.db:prepare('INSERT INTO ancestors (ancestor, descendant, depth) VALUES (?, ?, ?)'))
  local ancestral_closure = assert(self.db:prepare('INSERT INTO ancestors (ancestor, descendant, depth) SELECT ancestor, ?, depth+1 FROM ancestors WHERE descendant=?'))
  local make_parent_internal = assert(self.db:prepare('UPDATE documents SET leaf=0 WHERE rowid=?'))
  local changes_feed = assert(self.db:prepare('INSERT INTO changes (doc_row, seq) VALUES (?, ?)'))
  
  local seq = uuid()
  local result = {}
  
  assert(self.db:exec('BEGIN TRANSACTION'))
  
  for _, doc in ipairs(docs) do
    local generation = 1
    local parent_row = nil
    local docid = doc._id and doc._id or gen_docid()
    local parent_revid = doc._rev
    local deleted = doc._deleted
    
    if parent_revid then
      execute{statement=find_parent, data={docid, parent_revid}}
      local parent = fetchone(find_parent)
      
      if not parent then
        error{error='conflict', reason='Document update conflict.'}
      end
      
      parent_row = parent.rowid
      generation = parent.generation + 1
    end
    
    local revid = gen_revid(generation, doc)
    doc._id = docid
    doc._rev = revid
    
    -- Store the document itself
    execute{statement=insert_document, data={docid, revid, deleted and 1 or 0, generation, json.encode(doc)}, step=true}
    local doc_rowid = self.db:last_insert_rowid()
    
    -- Insert the indentity relation in the ancestors table
    execute{statement=ancestral_identity, data={doc_rowid, doc_rowid, 0}, step=true}
    if parent_revid then
      -- As we have at least one ancestral node, we need to complete the closures for this branch
      execute{statement=ancestral_closure, data={doc_rowid, parent_row}, step=true}
      -- ... and also ensure that we record that the direct parent is no longer a leaf
      execute{statement=make_parent_internal, data={parent_row}, step=true}
    end
            
    -- Record the change
    execute{statement=changes_feed, data={doc_rowid, seq}, step=true}
        
    result[#result+1] = {ok=true, id=docid, rev=revid}
  end
  
  assert(self.db:exec('COMMIT'))
  
  return result
end
 


return Sovoc