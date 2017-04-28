#!/usr/bin/env python

import os
import sys

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

    @classmethod
    def tearDownClass(cls):
        cls.db.conn.close()
        cls.db = None
        
    def test_json1_extension_present(self):
        # Quick extension test. This blows up if the extension wasn't loaded
        conn = sqlite3.connect(':memory:')
        conn.execute('select json(?)', (1337,)).fetchone()
        self.assertTrue(True)

    def test_createdoc(self):
        result = self.db.insert(None, None, False, {'name':'stefan'})
        self.assertTrue(result['ok'])

    def test_readdoc(self):
        written_doc = self.db.insert(None, None, False, {'name':'adam'})
        self.assertTrue(written_doc['ok'])
        
        read_doc = self.db.get(written_doc['id'])
        self.assertEqual(read_doc['_rev'], written_doc['rev'])

    def test_open_revs(self):
        result1 = self.db.insert(None, None, False, {'name':'stefan'})
        result2 = self.db.insert(result1['id'], result1['rev'], False, {'name':'stefan astrup'})
        result3 = self.db.insert(result1['id'], result1['rev'], False, {'name':'stef'})
        result4 = self.db.insert(result1['id'], result1['rev'], False, {'name':'steffe'})
        result5 = self.db.insert(result1['id'], result2['rev'], False, {'name':'stefan astrup kruger'})
        
        data = self.db.open_revs(result1['id'])
        
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['ok']['_revisions']['start'], 3)
        self.assertEqual(data[1]['ok']['_revisions']['start'], 2)    
        self.assertEqual(data[2]['ok']['_revisions']['start'], 2)
        
    def test_missing_rev(self):
        result1 = self.db.insert(None, None, False, {'name':'stefan'})     
        with self.assertRaises(ConflictError):
            result2 = self.db.insert(result1['id'], 'a bad rev', False, {'name':'stefan astrup'}) 
            
    def test_delete(self):
        result1 = self.db.insert(None, None, False, {'name':'bob'})
        result2 = self.db.destroy(result1['id'], result1['rev'])
        
        with self.assertRaises(ConflictError):
            self.db.insert(result2['id'], result2['rev'], False, {'name':'stefan astrup'})
        
if __name__ == '__main__':
    unittest.main()