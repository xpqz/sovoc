#!/usr/bin/env python

import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import uuid
import sqlite3

from sovoc.sovoc import Sovoc
from sovoc.exceptions import SovocError, ConflictError

class TestBasics(unittest.TestCase):
    database = ':memory:'
    db = None

    @classmethod
    def setUpClass(cls):
        cls.db = Sovoc(cls.database)
        cls.db.setup()
        
    def setUp(self):
        self.db = Sovoc(self.database)
        self.db.setup()
        
    @classmethod
    def tearDownClass(cls):
        cls.db.conn.close()
        cls.db = None
        
    def tearDown(self):
        self.db.conn.close()
        self.db = None
        
    def test_json1_extension_present(self):
        # Quick extension test. This blows up if the extension wasn't loaded
        conn = sqlite3.connect(':memory:')
        conn.execute('select json(?)', (1337,)).fetchone()
        self.assertTrue(True)

    def test_createdoc(self):
        result = self.db.insert({'name':'stefan'})
        self.assertTrue(result['ok'])

    def test_readdoc(self):
        written_doc = self.db.insert({'name':'adam'})
        self.assertTrue(written_doc['ok'])
        
        read_doc = self.db.get(written_doc['id'])
        self.assertEqual(read_doc['_rev'], written_doc['rev'])

    def test_open_revs(self):
        result1 = self.db.insert({'name':'stefan'})
        result2 = self.db.insert({'name':'stefan astrup'}, _id=result1['id'], _rev=result1['rev'])
        result3 = self.db.insert({'name':'stef'}, _id=result1['id'], _rev=result1['rev'])
        result4 = self.db.insert({'name':'steffe'}, _id=result1['id'], _rev=result1['rev'])
        result5 = self.db.insert({'name':'stefan astrup kruger'}, _id=result1['id'], _rev=result2['rev'])
        
        data = self.db.open_revs(result1['id'])
        
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['ok']['_revisions']['start'], 3)
        self.assertEqual(data[1]['ok']['_revisions']['start'], 2)    
        self.assertEqual(data[2]['ok']['_revisions']['start'], 2)
        
    def test_missing_rev(self):
        result1 = self.db.insert({'name':'stefan'})     
        with self.assertRaises(ConflictError):
            result2 = self.db.insert({'name':'stefan astrup'}, _id=result1['id'], _rev='a bad rev') 
            
    def test_delete(self):
        result1 = self.db.insert({'name':'bob'})
        result2 = self.db.destroy(result1['id'], result1['rev'])
        
        with self.assertRaises(ConflictError):
            self.db.insert({'name':'stefan astrup'}, _id=result2['id'], _rev=result2['rev'])
            
    def test_bulk(self):
        result = self.db.bulk([
            {'name': 'adam'},
            {'name': 'bob'},
            {'name': 'charlie'},
            {'name': 'danni'},
            {'name': 'eve'},
            {'name': 'frank'}
        ])
        
        # print json.dumps(result, indent=2)
             
    def test_changes(self):
        result1 = self.db.insert({'name':'stefan'})
        result2 = self.db.insert({'name':'stefan astrup'}, _id=result1['id'], _rev=result1['rev'])
        result3 = self.db.insert({'name':'stef'}, _id=result1['id'], _rev=result1['rev'])
        result4 = self.db.insert({'name':'steffe'}, _id=result1['id'], _rev=result1['rev'])
        result5 = self.db.insert({'name':'stefan astrup kruger'}, _id=result1['id'], _rev=result2['rev'])
        
        i = 0 # total 
        bookmark = None
        for entry in self.db.changes():
            if i == 2: # pick number 2
                bookmark = entry['seq']
            i += 1
            
        self.assertTrue(i == 5) # documents added above
        
        # fast-forward to bookmark @ 2
        j = 0
        for entry in self.db.changes(seq=bookmark):
            j += 1
            
        self.assertTrue(j == i - 3) # total - remainder - "fence post" @ 2
        
    def test_alldocs1(self):
        result1 = self.db.insert({'name':'stefan'}) 
        result2 = self.db.insert({'name':'stefan astrup'}, _id=result1['id'], _rev=result1['rev'])
        result3 = self.db.insert({'name':'stef'}, _id=result1['id'], _rev=result1['rev'])
        result4 = self.db.insert({'name':'steffe'}, _id=result1['id'], _rev=result1['rev'])
        result5 = self.db.insert({'name':'stefan astrup kruger'}, _id=result1['id'], _rev=result2['rev'])
        
        bulk_results = self.db.bulk([
            {'name': 'adam'},
            {'name': 'bob'},
            {'name': 'charlie'},
            {'name': 'danni'},
            {'name': 'eve'},
            {'name': 'frank'}
        ])
        
        count = 0
        for winner in self.db.list(include_docs=True):
            count += 1

        self.assertEqual(count, 7)
            
        count = 0
        for leaf in self.db.list(include_docs=True, conflicts=True):
            count += 1
            
        self.assertEqual(count, 9)
        
        keys = [result1['id'], bulk_results[2]['id'], bulk_results[5]['id']]
        count = 0
        for leaf in self.db.list(include_docs=True, keys=keys):
            count += 1
            
        self.assertEqual(count, len(keys))
            
        
if __name__ == '__main__':
    unittest.main()
