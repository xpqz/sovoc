import json
import sqlite3
import marshal
import hashlib
import uuid
import time
import copy

from sovoc.exceptions import SovocError, ConflictError

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
      depth INTEGER NOT NULL CHECK (depth >= 0),
      FOREIGN KEY(ancestor) REFERENCES documents(rowid),
      FOREIGN KEY(descendant) REFERENCES documents(rowid)
    )''',
    
    '''
    CREATE TABLE changes (
      doc_row INTEGER NOT NULL,
      seq TEXT NOT NULL,
      FOREIGN KEY(doc_row) REFERENCES documents(rowid)
    )''',
    
    '''
    CREATE INDEX seq_idx ON changes (seq)
    ''',
    
    '''
    CREATE VIEW changes_feed AS
      SELECT c.seq, d.rowid AS doc_row, d._deleted, d._id, d._rev
      FROM changes c, documents d
      WHERE c.doc_row = d.rowid
      ORDER BY d.rowid
    '''
]

def _flatten(d):

    result = {}
    
    def _key(parent, key):
        if parent:
            return '{0}.{1}'.format(parent, key)
        return key
        
    def _f(d, parent=None):
        for key, val in d.items():
            keystr = _key(parent, key)
            if type(val) == dict:
                _f(val, keystr)
            else:
                result[keystr] = val

    _f(d)
    
    return result

class Sovoc:
    def __init__(self, database):
        self.database = database
        self.conn = None
        attempts = 0
        
        while not self.conn and attempts < 5:
            try:
                self.conn = sqlite3.connect(self.database)
            except sqlite3.OperationalError:
                attempts += 1
                time.sleep(0.001)
                
        if not self.conn:
            raise sqlite3.OperationalError("Can't connect to sqlite database {}".format(database))
            
        self.conn.row_factory = sqlite3.Row
        
    def setup(self):
        with self.conn:
            c = self.conn.cursor()
            for statement in SCHEMA:
                c.execute(statement)

    @classmethod
    def gen_revid(cls, generation, body):
        body.pop('_id', None)
        body.pop('_rev', None)
        m = hashlib.md5()
        m.update(marshal.dumps(body))
        return '{0}-{1}'.format(generation, m.hexdigest())
        
    @classmethod
    def gen_docid(cls):
        return uuid.uuid4().hex
        
    def _insert_id(self, cursor):
        get_last_insert = 'SELECT last_insert_rowid()'
        cursor.execute(get_last_insert)
        row = cursor.fetchone()
        return row['last_insert_rowid()']
        
    def insert(self, doc, **kwargs):
        if '_rev' in kwargs:
            doc['_rev'] = kwargs['_rev']
        if '_id' in kwargs:
            doc['_id'] = kwargs['_id']
        if '_deleted' in kwargs:
            doc['_deleted'] = kwargs['_deleted']

        result = self.bulk([doc])
            
        return result[0]
        
    def update(self, doc, **kwargs):
        # We need at least an _id
        if '_rev' in kwargs:
            doc['_rev'] = kwargs['_rev']
        if '_id' in kwargs:
            doc['_id'] = kwargs['_id']
        if '_deleted' in kwargs:
            doc['_deleted'] = kwargs['_deleted']
            
        if not '_id' in doc:
            raise SovocError('No _id given')

        result = self.bulk([doc])
            
        return result[0]
        
    def bulk(self, docs, **kwargs):

        insert_document = 'INSERT INTO documents (_id, _rev, _deleted, generation, leaf, body) VALUES (?, ?, ?, ?, 1, json(?))'
        find_parent = 'SELECT rowid, generation FROM documents WHERE _id=? AND _rev=? AND _deleted=0'
        get_last_insert = 'SELECT last_insert_rowid()'
        ancestral_identity = 'INSERT INTO ancestors (ancestor, descendant, depth) VALUES (?, ?, ?)'
        ancestral_closure = 'INSERT INTO ancestors (ancestor, descendant, depth) SELECT ancestor, ?, depth+1 FROM ancestors WHERE descendant=?'
        make_parent_internal = 'UPDATE documents SET leaf=0 WHERE rowid=?'
        changes_feed = 'INSERT INTO changes (doc_row, seq) VALUES (?, ?)'
        
        seq = uuid.uuid4().hex # for now

        result = []
        
        with self.conn:
            c = self.conn.cursor()
            for doc in docs:
                generation = 1
                parent_row = None
                docid = doc.get('_id', Sovoc.gen_docid())
                parent_revid = doc.get('_rev', None)
                deleted = doc.get('_deleted', False)
                                
                if parent_revid:
                    c.execute(find_parent, [docid, parent_revid])
                    parent = c.fetchone()
                
                    if not parent:
                        raise ConflictError({'error': 'conflict', 'reason': 'Document update conflict.'})
                    
                    parent_row = parent['rowid']
                    generation = parent['generation'] + 1
                
                revid = Sovoc.gen_revid(generation, doc)
                
                # Store the document itself
                doc.update({'_id': docid, '_rev': revid})
                c.execute(insert_document, [docid, revid, 1 if deleted else 0, generation, json.dumps(doc)])
                doc_rowid = self._insert_id(c)
            
                # Insert the indentity relation in the ancestors table
                c.execute(ancestral_identity, [doc_rowid, doc_rowid, 0])
                if parent_revid:
                    # As we have at least one ancestral node, we need to complete the closures for this branch
                    c.execute(ancestral_closure, [doc_rowid, parent_row]) 
                    # ... and also ensure that we record that the direct parent is no longer a leaf
                    c.execute(make_parent_internal, [parent_row])
                
                # Record the change
                c.execute(changes_feed, [doc_rowid, seq])
            
                result.append({'ok': True, 'id': docid, 'rev': revid})
                
        return result
        
    def destroy(self, docid, revid):
        return self.insert({}, _id=docid, _rev=revid, _deleted=True)
            
    def open_revs(self, docid): # https://dx13.co.uk/articles/2017/1/1/the-tree-behind-cloudants-documents-and-how-to-use-it.html
        find_open_branches = 'SELECT rowid, body, _rev, generation FROM documents WHERE _id=? AND leaf=1 ORDER BY generation DESC'
        find_ancestral_revs = 'SELECT d._rev FROM documents d JOIN ancestors a ON (d.rowid = a.ancestor) WHERE a.descendant=? ORDER BY generation DESC'
        
        result = []

        with self.conn:
            branches = self.conn.cursor()
            
            for leaf in branches.execute(find_open_branches, [docid]):
                with self.conn:
                    revs = self.conn.cursor()
                    document = json.loads(leaf['body'])
                    document['_revisions'] = {'ids':[]}
                    document['_revisions']['start'] = leaf['generation'] 
                    # For each branch, find all ancestral nodes
                    for rev in revs.execute(find_ancestral_revs, [leaf['rowid']]):
                        document['_revisions']['ids'].append(rev['_rev'].split('-')[1])
                    result.append({'ok': document})
            
        return result
            
    def get(self, docid, revid=None):
        # Winner: sort on generation first and then lexicographically on _rev. Return first row.
        
        get_specific_rev = 'SELECT * FROM documents WHERE _id=? AND _rev=?'
        get_winner = 'SELECT * FROM documents WHERE _id=? AND leaf=1 ORDER BY generation DESC, _rev DESC LIMIT 1'

        with self.conn:
            c = self.conn.cursor()
            if revid: # specific rev is the simple case.
                c.excute(get_specific_rev, [docid, revid])
            else:
                c.execute(get_winner, [docid])
                
            document = c.fetchone()

            return json.loads(document['body'])
            
            
    def changes(self, **kwargs):
        seq = kwargs.get('seq', None)
        chunk = kwargs.get('chunk', 1000)
        
        get_changes = 'SELECT * FROM changes_feed WHERE doc_row > (SELECT MIN(rowid) FROM changes WHERE seq=?)'
        get_changes_all = 'SELECT * FROM changes_feed'

        with self.conn:
            c = self.conn.cursor()
            if seq:
                c.execute(get_changes, [seq])
            else:
                c.execute(get_changes_all)
                
            while True:
                results = c.fetchmany(chunk)
                if not results:
                    break
                    
                for row in results:
                    entry = {'seq': row['seq'], 'id': row['_id'], 'rev': row['_rev']}
                    if row['_deleted'] == 1:
                        entry['deleted'] = True
                    yield entry
                    
    def list(self, **kwargs):
        """
        See: http://docs.couchdb.org/en/2.0.0/api/database/bulk-api.html#db-all-docs
        """
        include_docs = kwargs.get('include_docs', False)
        conflicts = kwargs.get('conflicts', False)
        if not include_docs: # only allow conflicts if also including doc bodies; see CouchAPI link above
            conflicts = False
            
        chunk = kwargs.get('chunk', 1000)        
        keys = kwargs.get('keys', [])
        
        fields = '_id, _rev'
        if include_docs:
            fields = '_id, _rev, body'
            
        get_winners = 'SELECT {} FROM documents WHERE leaf=1 AND _deleted=0 ORDER BY generation DESC, _rev DESC'.format(fields)
        
        keyed_param_bindings = ','.join(['?']*len(keys))
        get_keyed_winners = 'SELECT {0} FROM documents WHERE leaf=1 AND _deleted=0 AND _id IN ({1}) ORDER BY generation DESC, _rev DESC'.format(fields, keyed_param_bindings)

        with self.conn:
            c = self.conn.cursor()
            if keys:
                c.execute(get_keyed_winners, keys)
            else:
                c.execute(get_winners)
                
            current_id = None
        
            while True:
                results = c.fetchmany(chunk)
                if not results:
                    break
                    
                for row in results:
                    entry = {'id': row['_id'], 'rev': row['_rev']}
                    if include_docs:
                        entry['doc'] = json.loads(row['body'])
                        
                    # If conflicts are requested, we return all leaf revisions. TODO: check format for conflicts - should they be grouped?
                    if conflicts:
                        yield entry
                    elif row['_id'] != current_id: # otherwise, only the winners (the first revision)
                        current_id = row['_id']

                        yield entry
                        
    def fetch(self, **kwargs):
        """_bulk_get"""
        pass
        
    def revs_diff(self, **kwargs):
        # http://docs.couchdb.org/en/2.0.0/api/database/misc.html#post--db-_revs_diff
        pass
        
    def find(self, query, chunk = 1000):
        # query is a CQ expression represented by a dict
        
        # 1. Find the requested fields: they will form the SELECT a, b, c... part, which we 
        #    need to extract from the json payload, apart from _id and _rev
        fields = []
        fieldstr = ''
        for field in query['fields']:
            if field in ['_id', '_rev']:
                fields.append(field)
            else:
                fields.append('json_extract(body, "$.{0}") AS {0}'.format(field)) # NOT injectable; see e.g....[so]
        
        if fields:
            fieldstr = ', '.join(fields)
             
        # 2. Optional sorting goes into ORDER BY x, y, x   
        order = []
        orderstr = ''
        if 'sort' in query:
            for sorter in query['sort']:
                for (field, direction) in sorter.items():
                    if direction.upper() in ['ASC', 'DESC']:
                        order.append('{0} {1}'.format(field, direction))
                    
        if order:
            orderstr = ' ORDER BY {0}'.format(','.join(order))
            
        # 3. The selector is the discriminant, i.e. the WHERE i, j, k bit of the statement
        discriminants = []
        wherestr = ''
        for (field, value) in query['selector'].items():
            if type(value) != dict: # scalar
                # If part of the requested fields, we don't extract from json, as should have been aliased already.
                if field in fields:
                    discriminants.append(['{0}=?'.format(field), value])
                else: # Discriminant not requested
                    discriminants.append(['json_extract(body, "$.{0}")=?'.format(field), value])
            else: # A dict -- either a sub-field query or an operator
                # 3.1 Sub-field as json object:
                #
                # "imdb": {
                #     "rating": 8
                # }
                # 
                # Flatten to "imdb.rating": 8
                # 
                # This will need to be extracted from the json
                for (fkey, fval) in _flatten({field: value}).items():
                    discriminants.append(['json_extract(body, "$.{0}")=?'.format(fkey), fval])
                
        if discriminants:
            wherestr = ' WHERE {0}'.format(' AND '.join([term[0] for term in discriminants]))
    
        statement = 'SELECT {0} FROM documents{1}{2}'.format(fieldstr, wherestr, orderstr)
        
        print(statement)
        
        values = [term[1] for term in discriminants]
        
        with self.conn:
            c = self.conn.cursor()
            c.execute(statement, values)
            
            while True:
                results = c.fetchmany(chunk)
                if not results:
                    break
                    
                for row in results:
                    yield {key: row[key] for key in row.keys()}