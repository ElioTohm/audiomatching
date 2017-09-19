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
    """
    Queries:

    1) Find duplicates (shouldn't be any, though):

        select `hash`, `record_id`, `offset`, count(*) cnt
        from fingerprints
        group by `hash`, `record_id`, `offset`
        having cnt > 1
        order by cnt asc;

    2) Get number of hashes by record:

        select record_id, channel_id, count(record_id) as num
        from fingerprints
        natural join records
        group by record_id
        order by count(record_id) desc;

    3) get hashes with highest number of collisions

        select
            hash,
            count(distinct record_id) as n
        from fingerprints
        group by `hash`
        order by n DESC;

    => 26 different records with same fingerprint (392 times):

        select records.channel_id, fingerprints.offset
        from fingerprints natural join records
        where fingerprints.hash = "08d3c833b71c60a7b620322ac0c0aba7bf5a3e73";
    """

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
    LASTSAVEDRECORD = 1200

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
        Return the (record_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        lower_bound = timestamp - self.TSI_BOUND
        upper_bound = timestamp + self.TSI_BOUND

        # Create a dictionary of hash => offset pairs for later lookups
        hashlist = list()

        for hash, offset in hashes:
            hashlist.append(hash)
        

        client = MongoClient()
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
                '$cond': [{ '$lt': [ '$confidence', len(hashlist)/2 ] },'$$PRUNE','$$KEEP']
              }
            },
            { 
                '$sort' : {'confidence' : -1} 
            },
            { "$limit": 1 }
        ]

        return db.fingerprints.aggregate(pipeline)
        
        # with open('test1.json','wb+') as f:
        #     for match in matches:
        #         f.write(json.dumps(match))
        #         print np.shape(match['match'])


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))


