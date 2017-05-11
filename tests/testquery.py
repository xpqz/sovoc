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
        
    def test_find(self):

        query = {
            'selector': {
                'year': 2010,
                'title': 'ghi'
            },
            'fields': ['_id', '_rev', 'year', 'title'],
            'sort': [{'year': 'asc'}]
        }

        bulk_results = self.db.bulk([
            {'year': 1947, 'title': 'abc'},
            {'year': 1876, 'title': 'def'},
            {'year': 2010, 'title': 'ghi'},
            {'year': 2011, 'title': 'ghi'},
            {'year': 2010, 'title': 'qwe'},
            {'year': 1969, 'title': 'jkl'},
            {'year': 2007, 'title': 'mno'},
            {'year': 1982, 'title': 'pqr'}
        ])

        for row in self.db.find(query):
            self.assertEqual(row['_id'], bulk_results[2]['id'])

    def test_nested_selector(self):

        query = {
            'selector': {
                'rating': {
                    'imdb': 6
                }
            },
            'fields': ['_id', '_rev', 'year', 'title']
        }

        bulk_results = self.db.bulk([
            {'year': 1947, 'title': 'abc', 'rating': {'imdb': 10}},
            {'year': 1876, 'title': 'def', 'rating': {'imdb': 9}},
            {'year': 2010, 'title': 'ghi', 'rating': {'imdb': 8}},
            {'year': 2011, 'title': 'ghi', 'rating': {'imdb': 7}},
            {'year': 2010, 'title': 'qwe', 'rating': {'imdb': 6}},
            {'year': 1969, 'title': 'jkl', 'rating': {'imdb': 5}},
            {'year': 2007, 'title': 'mno', 'rating': {'imdb': 4}},
            {'year': 1982, 'title': 'pqr', 'rating': {'imdb': 3}}
        ])

        for row in self.db.find(query):
            self.assertEqual(row['_id'], bulk_results[4]['id'])

    def test_selector_field_not_returned(self):

        query = {
            'selector': {
                'year': 1969
            },
            'fields': ['_id', '_rev', 'title']
        }

        bulk_results = self.db.bulk([
            {'year': 1947, 'title': 'abc', 'rating': {'imdb': 10}},
            {'year': 1876, 'title': 'def', 'rating': {'imdb': 9}},
            {'year': 2010, 'title': 'ghi', 'rating': {'imdb': 8}},
            {'year': 2011, 'title': 'ghi', 'rating': {'imdb': 7}},
            {'year': 2010, 'title': 'qwe', 'rating': {'imdb': 6}},
            {'year': 1969, 'title': 'jkl', 'rating': {'imdb': 5}},
            {'year': 2007, 'title': 'mno', 'rating': {'imdb': 4}},
            {'year': 1982, 'title': 'pqr', 'rating': {'imdb': 3}}
        ])

        for row in self.db.find(query):
            self.assertEqual(row['_id'], bulk_results[5]['id'])
            
    def test_gt(self):
        
        query = {
            'selector': {
                'year': {
                    '$gt': 2000
                }
            },
            'fields': ['_id', '_rev', 'year']
        }
        
        bulk_results = self.db.bulk([
            {'year': 1947, 'title': 'abc', 'rating': {'imdb': 10}},
            {'year': 1876, 'title': 'def', 'rating': {'imdb': 9}},
            {'year': 2010, 'title': 'ghi', 'rating': {'imdb': 8}},
            {'year': 2011, 'title': 'ghi', 'rating': {'imdb': 7}},
            {'year': 2010, 'title': 'qwe', 'rating': {'imdb': 6}},            
            {'year': 1969, 'title': 'jkl', 'rating': {'imdb': 5}},
            {'year': 2007, 'title': 'mno', 'rating': {'imdb': 4}},
            {'year': 1982, 'title': 'pqr', 'rating': {'imdb': 3}}
        ])
        
        found = False
        for row in self.db.find(query):
            found = True
            self.assertTrue(row['year'] > 2000)
            
        self.assertTrue(found)
        
if __name__ == '__main__':
    unittest.main()