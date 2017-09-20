'''
    mongodb wraper
'''
from __future__ import absolute_import
from itertools import izip_longest
import json
import os
import pprint
import numpy as np
from pymongo import MongoClient

class MongoDatabase():
    '''
    wrapper for mongodb 
    '''
    # tables
    FIELD_FILE_SHA1 = 'file_sha1'
    FIELD_RECORD_ID = 'record_id'
    FIELD_CHANNEL_ID = 'channel_id'
    FIELD_OFFSET = 'offset'
    FIELD_HASH = 'hash'
    FIELD_TIMESTAMP = 'timestamp'
    FIELD_CHANNEL_NAME = 'channel_name'
    FINGERPRINTS_TABLENAME = "fingerprints"
    RECORD_TABLE = "records"
    TSI_BOUND = 15

    # fields
    FIELD_FINGERPRINTED = "fingerprinted"

    def insert_hashes(self, hashes, timestamp, channel_id, channel_name, file_hash):
        """
        Insert series of hash => record_id, offset
        values into the database.
        """
        values = []
        for hash, offset in hashes:
            values.append({"hash": hash, "offset": offset})

        return {self.FIELD_CHANNEL_ID: channel_id, self.FIELD_CHANNEL_NAME: channel_name,
                self.FIELD_FILE_SHA1: file_hash, self.FIELD_TIMESTAMP: int(timestamp),
                self.FINGERPRINTS_TABLENAME: values}

    def return_matches(self, hashes, timestamp):
        """
        Return the record witht he highest intersection size
        """
        lower_bound = timestamp - self.TSI_BOUND
        upper_bound = timestamp + self.TSI_BOUND

        # Create a dictionary of hash => offset pairs for later lookups
        hashlist = list()

        for hash, offset in hashes:
            hashlist.append(hash)
        

        client = MongoClient('127.0.0.1',
                      user='xmsmongodb',
                      password='xms@Prro#123mongo',
                      authSource='database',
                      authMechanism='SCRAM-SHA-1')
        db = client.database

        pipeline = [
            {
                '$match': {
                    'timestamp': {
                        '$gte': lower_bound, '$lt': upper_bound
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    self.FIELD_CHANNEL_ID: 1,
                    self.FIELD_CHANNEL_NAME: 1,
                    'confidence': {
                        '$size': {
                            '$setIntersection': [hashlist, '$fingerprints.hash']
                        }
                    },
                }
            },
            {
              '$redact': {
                '$cond': [{ '$lt': [ '$confidence', 1 ] },'$$PRUNE','$$KEEP']
              }
            },
            { 
                '$sort' : {'confidence' : -1} 
            },
            { "$limit": 1 }
        ]

        return db.fingerprints.aggregate(pipeline)
        
def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))


