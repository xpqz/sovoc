--- An MVCC layer on top of SQLite3
-- @author Stefan Kruger

local sqlite3 = require "lsqlite3"
local uuid = require "lua_uuid"
local mp = require 'MessagePack'
local md5 = require 'md5'
local json = require 'cjson'

local SCHEMA = [[
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
    SELECT c.seq AS seq, d.rowid AS doc_row, d._deleted AS deleted, d._id AS _id, d._rev AS _rev, d.body AS body
    FROM changes c, documents d
    WHERE c.doc_row = d.rowid
    ORDER BY d.rowid;
  COMMIT;
]]

local Sovoc = { database = nil }

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

--- An MVCC layer on top of SQLite3
-- @param database The name of the database file. Use ':memory:' for a temporary db
-- @usage local db = Sovoc:new{database="data.db"}
-- @return table
function Sovoc:new(tbl) 
  tbl = tbl or {}
  setmetatable(tbl, self)
  self.__index = self
  assert(tbl.database)
  if tbl.database == ':memory:' then 
    tbl.db = assert(sqlite3.open_memory())
  else
    tbl.db = assert(sqlite3.open(tbl.database))
  end
  return tbl
end

--- Initialise a new database by writing its schema
-- @usage local db = Sovoc:new{database="data.db"}
-- db:setup()
function Sovoc:setup()
  assert(self.db:exec(SCHEMA))
end

--- Bulk-load a set of documents.
-- This is the basis for all create, update and delete operations. 
-- @param single table holding the documents to be uploaded
-- @usage local result = db:bulk{
--   {name='adam'},
--   {name='bob'},
--   {name='charlie'},
--   {name='danni'},
--   {name='eve'},
--   {name='frank'}
-- }
-- @return table holding the status stubs of successfully written documents:
--   {{ok=true, id=docid1, rev=revid1}, {ok=true, id=docid2, rev=revid2}, ... }
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
  
  assert(self.db:exec 'BEGIN TRANSACTION')
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
  assert(self.db:exec 'COMMIT')
  
  return result
end

--- Read a document from the database.
-- @param _id The document id [required]
-- @param _rev The revision id [optional]. If no revision id is given, this routine returns the winning revsion for the document id.
-- @return table representing the document body
-- @usage local doc = db:get{_id='06e2f7c4b188b9de7a78ef073b777202', _rev='2-282efa82df06afa57349b3bfc6156ad7'}
function Sovoc:get(options)
  -- Winner: sort on generation first and then lexicographically on _rev. Return first row.
  assert(options._id)
  
  local get_specific_rev = assert(self.db:prepare 'SELECT * FROM documents WHERE _id=? AND _rev=?')
  local get_winner = assert(self.db:prepare 'SELECT * FROM documents WHERE _id=? AND leaf=1 ORDER BY generation DESC, _rev DESC LIMIT 1')

  local document = nil
  if options._rev then -- specific rev is the simple case.
    execute{statement=get_specific_rev, data={options._id, options._rev}}
    document = fetchone(get_specific_rev)
  else
    execute{statement=get_winner, data={options._id}}
    document = fetchone(get_winner)
  end
          
  return json.decode(document.body)
end

--- Insert a document into the database. Implemented as a call to Sovoc:bulk().
-- @param doc Table representing the document body
-- @param _id The document id [optional].
-- @param _rev The revision id [optional]. 
-- @param _deleted boolean Set to true to generate tombstone [optional]. 
-- @return table representing the status stub of the write, eg {ok=true, id=docid1, rev=revid1}
-- @usage local status = db:insert{doc={name='bob'}}
-- @see Sovoc:bulk()
function Sovoc:insert(options)
  assert(options.doc)
  
  if options._id then
    options.doc._id = options._id
  end
  if options._rev then
    options.doc._rev = options._rev
  end
  if options._deleted then
    options.doc._deleted = options._deleted
  end
  
  local result = self:bulk({options.doc})
  
  return result[1]
end

--- Update an existing document. Implemented as a call to Sovoc:bulk().
-- @param doc Table representing the document body 
-- @param _id The document id [optional]. If not given, the document must contain an '_id' field.
-- @param _rev The revision id [optional]. 
-- @param _deleted boolean Set to true to generate tombstone [optional]. 
-- @return table representing the status stub of the update, eg {ok=true, id=docid1, rev=revid1}
-- @usage local status = db:update{_id='06e2f7c4b188b9de7a78ef073b777202', doc={name='bob'}}
-- @see Sovoc:bulk()
function Sovoc:update(options)
  assert(options._id or (options.doc and options.doc._id))
  return Sovoc:insert(options)
end

--- Delete an existing document. Implemented as a call to Sovoc:bulk().
-- @param _id The document id.
-- @param _rev The revision id. 
-- @return table representing the status stub of the resulting tombstone revision, eg {ok=true, id=docid, rev=revid5}
-- @usage local status = db:destroy{_id='06e2f7c4b188b9de7a78ef073b777202', _rev='2-282efa82df06afa57349b3bfc6156ad7'}
-- @see Sovoc:bulk()
function Sovoc:destroy(options)
  assert(options._id)
  assert(options._rev)
  
  local result = self:bulk({{_id=options._id, _rev=options._rev, _deleted=true}})
  
  return result[1]
end

--- Return the full document tree structure for an existing document. 
-- @param _id The document id.
-- @return table representing the status stub of the resulting tombstone revision, with the same format as CouchDB
-- @usage local tree = db:open_revs{_id='06e2f7c4b188b9de7a78ef073b777202'}
-- @see https://dx13.co.uk/articles/2017/1/1/the-tree-behind-cloudants-documents-and-how-to-use-it.html
function Sovoc:open_revs(options)
  assert(options._id)
  
  local find_open_branches = assert(self.db:prepare 'SELECT rowid, body, _rev, generation FROM documents WHERE _id=? AND leaf=1 ORDER BY generation DESC')
  local find_ancestral_revs = assert(self.db:prepare 'SELECT d._rev FROM documents d JOIN ancestors a ON (d.rowid = a.ancestor) WHERE a.descendant=? ORDER BY generation DESC')
  local result = {}
  
  find_open_branches:bind_values(options._id)
  
  assert(self.db:exec 'BEGIN TRANSACTION')
    for leaf in find_open_branches:nrows() do
      local document = json.decode(leaf.body)
      document._revisions = {ids={}, start=leaf.generation}
      -- for each branch, find all ancestral nodes
      find_ancestral_revs:reset()
      find_ancestral_revs:bind_values(leaf.rowid)
      for rev in find_ancestral_revs:nrows() do
        local gen, hash = rev._rev:match("([0-9]+)-([0-9a-f]+)")
        document._revisions.ids[#document._revisions.ids+1] = hash
      end
      result[#result+1] = {ok=document}
    end
  assert(self.db:exec 'COMMIT')
  
  return result
end

--- Return the changes feed as a table.
-- @param seq A sequence id to fast-forward to [optional].
-- @return table with each entry being a table holding the sequence id, document id and revision id of the change.
-- @usage local changes = db:changes{_seq='06e2f7c-4b188b-9de7a78ef-073b777202'}
function Sovoc:changes(options)
  local seq = options and options.seq or nil
  local include_docs = false
  local fields = 'seq, _id, _rev'
  if options then
    include_docs = options.include_docs and options.include_docs or false
    if include_docs then
      fields = fields .. ', body'
    end
  end
  
  local get_changes = assert(self.db:prepare(string.format('SELECT %s FROM changes_feed WHERE doc_row > (SELECT MIN(rowid) FROM changes WHERE seq=?)', fields)))
  local get_changes_all = assert(self.db:prepare(string.format('SELECT %s FROM changes_feed', fields)))

  local statement = get_changes_all

  if seq then
    get_changes:bind_values(options.seq)
    statement = get_changes
  end

  local result = {}
  self.db:exec 'BEGIN TRANSACTION'
  for row in statement:nrows() do
    local entry = {seq=row.seq, id=row._id, rev=row._rev}
    if row._deleted == 1 then
      entry.deleted = true
    end
    if include_docs then
      entry.body = json.decode(row.body)
    end
    result[#result+1]=entry
  end
  self.db:exec 'COMMIT'

  return result
end

--- Co-routine version of the changes feed, allowing data to be streamed.
-- @param seq A sequence id to fast-forward to [optional].
-- @return yields the index and a table holding sequence id, document id and revision id of the change.
-- @usage Not intended to be used directly; see Sovoc:iterate_changes()
-- @see Sovoc:iterate_changes()
function Sovoc:next_change(options)
  local seq = options and options.seq or nil
  local include_docs = false
  local fields = 'seq, _id, _rev'
  if options then
    include_docs = options.include_docs and options.include_docs or false
    if include_docs then
      fields = fields .. ', body'
    end
  end
  
  local get_changes = assert(self.db:prepare(string.format('SELECT %s FROM changes_feed WHERE doc_row > (SELECT MIN(rowid) FROM changes WHERE seq=?)', fields)))
  local get_changes_all = assert(self.db:prepare(string.format('SELECT %s FROM changes_feed', fields)))

  local statement = get_changes_all

  if seq then
    get_changes:bind_values(options.seq)
    statement = get_changes
  end
  
  local index = 1
  for row in statement:nrows() do
    local entry = {seq=row.seq, id=row._id, rev=row._rev}
    if row._deleted == 1 then
      entry.deleted = true
    end
    if include_docs then
      entry.body = json.decode(row.body)
    end
    coroutine.yield(index, entry)
    index = index + 1
  end
end

--- Iterator for the changes feed, allowing data to be streamed.
-- @param seq A sequence id to fast-forward to [optional].
-- @return yields the index and a table holding sequence id, document id and revision id of the change.
-- @usage  for index, entry in db:iterate_changes{seq=bookmark} do
--    ...
-- end
-- @see Sovoc:next_change()
function Sovoc:iterate_changes(options)
  return coroutine.wrap(function() self:next_change(options) end)
end

--- Static revs_diff: return data about misssing revisions
-- @param Table
-- @usage local missing = db:revs_diff{
--     "baz": {
--         "2-7051cbe5c8faecd085a3fa619e6e6337"
--     },
--     "foo": {
--         "3-6a540f3d701ac518d3b9733d673c5484"
--     },
--     "bar": {
--         "1-d4e501ab47de6b2000fc8a02f84a0c77",
--         "1-967a00dff5e02add41819138abb3284d"
--     }
-- }
function Sovoc:revs_diff(data)
  local tblname = gen_docid()
  self.db:exec(string.format('BEGIN TRANSACTION; CREATE TEMPORARY TABLE "%s" (_id TEXT, _rev TEXT);', tblname))

  local insert_diff = assert(self.db:prepare(string.format('INSERT INTO "%s" (_id, _rev) VALUES (?, ?)', tblname)))
  local missing_revs = string.format('SELECT _id, _rev FROM "%s" EXCEPT SELECT _id, _rev FROM documents', tblname)

  -- Populate temporary table
  for idset, revs in pairs(data) do
    for _, rev in ipairs(revs) do
      execute{statement=insert_diff, data={idset, rev}, step=true}
    end
  end
  
  local i=1
  local result = {}
  for missing in self.db:nrows(missing_revs) do
    result[#result+1] = missing
  end
  self.db:exec(string.format('DROP TABLE "%s"; COMMIT;', tblname))

  return result
end

--- Return data about misssing revisions -- co-routine helper
-- @param Table
function Sovoc:next_revs_diff(data)
  local tblname = gen_docid()
  self.db:exec(string.format('BEGIN TRANSACTION; CREATE TEMPORARY TABLE "%s" (_id TEXT, _rev TEXT);', tblname))

  local insert_diff = assert(self.db:prepare(string.format('INSERT INTO "%s" (_id, _rev) VALUES (?, ?)', tblname)))
  local missing_revs = string.format('SELECT _id, _rev FROM "%s" EXCEPT SELECT _id, _rev FROM documents', tblname)
    
  -- Populate temporary table
  for idset, revs in pairs(data) do
    for _, rev in ipairs(revs) do
      execute{statement=insert_diff, data={idset, rev}, step=true}
    end
  end
  
  local i=1
  for missing in self.db:nrows(missing_revs) do
    coroutine.yield(i, missing)
    i = i + 1
  end
  self.db:exec(string.format('DROP TABLE %s; COMMIT;', tblname))
end

--- Co-routine revs_diff: return data about misssing revisions
-- @usage local data = {
--     "baz": {
--         "2-7051cbe5c8faecd085a3fa619e6e6337"
--     },
--     "foo": {
--         "3-6a540f3d701ac518d3b9733d673c5484"
--     },
--     "bar": {
--         "1-d4e501ab47de6b2000fc8a02f84a0c77",
--         "1-967a00dff5e02add41819138abb3284d"
--     }
-- }
-- for _, missing in db:iterate_revs_diff(data) do
--   ...
-- end
function Sovoc:iterate_revs_diff(data)
  return coroutine.wrap(function() self:next_revs_diff(data) end)
end

return Sovoc