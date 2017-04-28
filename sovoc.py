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
        self.conn.row_factory = sqlite3.Row
        
    def setup(self):
        with self.conn:
            c = self.conn.cursor()
            for statement in SCHEMA:
                c.execute(statement)

    def gen_revid(self, generation, body): # should be class method probably
        body.pop('_id', None)
        body.pop('_rev', None)
        m = hashlib.md5()
        m.update(marshal.dumps(body))
        return '{0}-{1}'.format(generation, m.hexdigest())
        
    def gen_docid(self):
        return uuid.uuid4().hex
        
    def insert(self, docid=None, parent_revid=None, deleted=False, payload={}):        
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
                if parent_revid:
                    c.execute(find_parent, [docid, parent_revid])
                    parent = c.fetchone()
                    print parent
                    parent_row = parent['rowid']
                    # TODO: if no parent_row here, we should handle the error.
                    
                    generation = parent['generation'] + 1
                    
                revid = self.gen_revid(generation, payload)
                    
                # Store the document itself
                payload.update({'_id': docid, '_rev': revid})
                c.execute(insert_document, [docid, revid, 1 if deleted else 0, generation, json.dumps(payload)])
                c.execute(get_last_insert)
                document = c.fetchone()
                # Insert the indentity relation in the ancestors table
                c.execute(ancestral_identity, [document['last_insert_rowid()'], document['last_insert_rowid()'], 0])
                if parent_revid:
                    # As we have at least one ancestral node, we need to complete the closures for this branch
                    c.execute(ancestral_closure, [document['last_insert_rowid()'], parent_row]) 
                    # ... and also ensure that we record that the direct parent is no longer a leaf
                    c.execute(make_parent_internal, [parent_row])               
                
        except sqlite3.OperationalError as err:
            print err
            
        return (docid, revid)
            
    def open_revs(self, docid): # https://dx13.co.uk/articles/2017/1/1/the-tree-behind-cloudants-documents-and-how-to-use-it.html
        find_open_branches = 'SELECT rowid, body, _rev, generation FROM documents WHERE _id=? AND leaf=1 ORDER BY generation DESC'
        find_ancestral_revs = 'SELECT d._rev FROM documents d JOIN ancestors a ON (d.rowid = a.ancestor) WHERE a.descendant=? ORDER BY generation DESC'
        
        result = []
        try:
            with self.conn:
                branches = self.conn.cursor()
                
                for leaf in branches.execute(find_open_branches, [docid]):
                    with self.conn:
                        revs = self.conn.cursor()
                        print 'branch tip: {0} at rev: {1}'.format(leaf['rowid'], leaf['_rev'])
                        document = json.loads(leaf['body'])
                        document['_revisions'] = {'ids':[]}
                        document['_revisions']['start'] = leaf['generation'] 
                        # For each branch, find all ancestral nodes
                        for rev in revs.execute(find_ancestral_revs, [leaf['rowid']]):
                            document['_revisions']['ids'].append(rev['_rev'].split('-')[1])
                        result.append({'ok': document})

        except sqlite3.OperationalError as err:
            print err
            
        return result
            
    def get(self, docid, revid=None):
        # Winner: sort on generation first and then lexicographically on _rev. Return first row.
        
        get_specific_rev = 'SELECT * FROM documents WHERE _id=? AND _rev=?'
        get_winner = 'SELECT * FROM documents WHERE _id=? AND leaf=1 ORDER BY generation DESC, _rev DESC LIMIT 1'

        try:
            with self.conn:
                c = self.conn.cursor()
                if revid: # specific rev is the simple case.
                    c.excute(get_specific_rev, [docid, revid])
                else:
                    c.execute(get_winner, [docid])
                    
                document = c.fetchone()

                return json.loads(document['body'])
    
        except sqlite3.OperationalError as err:
            print err                
             
                
        
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
    (docid, revid4) = db.insert(docid, revid1, False, {'name':'steffe'})
    (docid, revid5) = db.insert(docid, revid2, False, {'name':'stefan astrup kruger'})
    
    # print docid
    # print revid

    c = db.conn.cursor()
    c.execute('SELECT * FROM documents')
    for row in c:
        print row
        
    c.execute('SELECT * FROM ancestors')
    for row in c:
        print row
        
    data = db.open_revs(docid)
    print json.dumps(data, indent=2)
    
    data = db.get(docid)
    print json.dumps(data, indent=2)