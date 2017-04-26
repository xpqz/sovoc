import json
import sqlite3
import marshal
import hashlib
import uuid

SCHEMA = [
    '''
    CREATE TABLE documents (
      _id TEXT NOT NULL,
      _rev TEXT NOT NULL,
      _deleted INTEGER DEFAULT 0 CHECK (_deleted = 0 OR _deleted = 1),
      generation INTEGER DEFAULT 1 CHECK (generation > 0),
      leaf INTEGER DEFAULT 1 CHECK (leaf = 0 OR leaf = 1),
      body TEXT,
      UNIQUE (_id, _rev) ON CONFLICT IGNORE
    )''',
    
    '''
    CREATE TABLE ancestors (
        -- Table holding the document tree structure
        -- represented as a closure table. The 'depth'
        -- column holds the number of levels between
        -- ancestor and descendant
      ancestor INTEGER NOT NULL,
      descendant INTEGER NOT NULL,
      depth INTEGER NOT NULL
    )'''
]

class Sovoc:
    def __init__(self, database):
        self.database = database
        self.conn = sqlite3.connect(self.database)
        
    def execute(self, sql):
        if not type(sql) is list:
            sql = [sql]
        try:
            with self.conn:
                for st in sql:
                    self.conn.execute(st)
        except sqlite3.OperationalError as err:
            print err
        
    def setup(self):
        self.execute(SCHEMA)
        
    def gen_revid(self, generation, body): # should be class method probably
        body.pop('_id', None)
        body.pop('_rev', None)
        m = hashlib.md5()
        m.update(marshal.dumps(body))
        return '{0}-{1}'.format(generation, m.hexdigest())
        
    def gen_docid(self):
        return uuid.uuid4().hex
        
    def insert(self, docid=None, parent=None, deleted=False, payload={}):        
        insert_document = 'INSERT INTO documents (_id, _rev, _deleted, generation, leaf, body) VALUES (?, ?, ?, ?, 1, json(?))'
        find_parent = 'SELECT rowid, generation FROM documents WHERE _id=? AND _rev=? AND _deleted=0'
        get_last_insert = 'SELECT last_insert_rowid()'
        ancestral_identity = 'INSERT INTO ancestors (ancestor, descendant, depth) VALUES (?, ?, ?)'
        ancestral_closure = 'INSERT INTO ancestors (ancestor, descendant, depth) SELECT ancestor, ?, depth+1 FROM ancestors WHERE descendant=?'
        make_parent_internal = 'UPDATE documents SET leaf=0 WHERE rowid=?'
        
        generation = 1
        
        if not docid:
            docid = self.gen_docid()
        
        try:
            with self.conn:
                c = self.conn.cursor()
                parent_row = None
                if parent:
                    c.execute(find_parent, [docid, parent])
                    row = c.fetchone()
                    print row
                    parent_row = row[0]
                    # if no parent_row here, we should handle the error.
                    
                    generation = row[1] + 1
                    
                revid = self.gen_revid(generation, payload)
                    
                # Store the document itself
                payload.update({'_id': docid, '_rev': revid})
                c.execute(insert_document, [docid, revid, 1 if deleted else 0, generation, json.dumps(payload)])
                c.execute(get_last_insert)
                row = c.fetchone()
                # Insert the indentity relation in the ancestors table
                c.execute(ancestral_identity, [row[0], row[0], 0])
                if parent:
                    # As we have at least one ancestral node, we need to complete the closures for this branch
                    c.execute(ancestral_closure, [row[0], parent_row]) 
                    # ... and also ensure that we record that the direct parent is no longer a leaf
                    c.execute(make_parent_internal, [parent_row])               
                
        except sqlite3.OperationalError as err:
            print err
            
        return (docid, revid)
            
    def fetch_open_revs(self, docid): # https://dx13.co.uk/articles/2017/1/1/the-tree-behind-cloudants-documents-and-how-to-use-it.html
        find_open_branches = 'SELECT rowid FROM documents WHERE _id=? AND leaf=1'
        find_ancestral_revs = 'SELECT d.* FROM documents d JOIN ancestors a ON (d.rowid = a.ancestor) WHERE a.descendant=?'
        
        try:
            with self.conn:
                branches = self.conn.cursor()
                
                for leaf in branches.execute(find_open_branches, [docid]):
                    with self.conn:
                        revs = self.conn.cursor()
                        print 'branch tip: {}'.format(leaf[0])
                        # For each branch, find all ancestral nodes
                        for rev in revs.execute(find_ancestral_revs, [leaf[0]]):
                            print rev[1]
                    print "--"

        except sqlite3.OperationalError as err:
            print err
            
        # return a sensible format
            
    def get(self, docid, revid=None):
        get_specific_rev = 'SELECT * FROM documents WHERE _id=? AND _rev=?'
        get_leaves = 'SELECT * FROM documents WHERE _id=? AND leaf=1 AND _deleted=0'
        
        if revid:
            try:
                with self.conn:
                    c = self.conn.cursor()
                    c.excute(get_specific_rev, [docid, revid])
                    return c.fetchone()
            except sqlite3.OperationalError as err:
                print err
        else:
            with self.conn:
                c = self.conn.cursor()
                c.excute(get_leaves, [docid])
                
        
if __name__ == '__main__':
         
    db = Sovoc(':memory:')
    db.setup()


    # Quick extension test. This would blow up if the extension wasn't loaded:
    # conn = sqlite3.connect(':memory:')
    #
    # print type(conn)
    # conn.execute('select json(?)', (1337,)).fetchone()

    (docid, revid1) = db.insert(None, None, False, {'name':'stefan'})
    (docid, revid2) = db.insert(docid, revid1, False, {'name':'stefan astrup'})
    (docid, revid3) = db.insert(docid, revid1, False, {'name':'stef'})
    (docid, revid4) = db.insert(docid, revid2, False, {'name':'stefan astrup kruger'})
    
    # print docid
    # print revid

    c = db.conn.cursor()
    c.execute('SELECT * FROM documents')
    for row in c:
        print row
        
    c.execute('SELECT * FROM ancestors')
    for row in c:
        print row
        
    db.fetch_open_revs(docid)