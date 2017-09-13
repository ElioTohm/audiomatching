'''
    mongodb wraper
'''
from __future__ import absolute_import
from itertools import izip_longest
import json
import os
import pprint
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
                                self.FINGERPRINTS_TABLENAME: values }

    def return_matches(self, hashes, timestamp):
        """
        Return the (record_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        # Take the timestamp difference fromt he sql_statment_config.json file
        module_dir = os.path.dirname(__file__)  # get current directory
        json_file_path = os.path.join(module_dir, 'sql_statments_config.json')
        print module_dir
        with open(json_file_path) as json_data_file:
            data = json.load(json_data_file)
        lower_bound = timestamp - data["timestamp_interval"]["lower_bound"]
        upper_bound = timestamp + data["timestamp_interval"]["upper_bound"]

        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}

        for hash, offset in hashes:
            pprint.pprint(hash)
            pprint.pprint(offset)
            # mapper[hash.upper()] = offset

        client = MongoClient()
        db = client.database

        matches = db.fingerprints.find({
                "fingerprints" : {
                    "$elemMatch": {
                        "hash": {
                            "$in":["d2ea889101155d3100c5"]
                        }
                    }
                }
            })

        pipeline = [
            {
                '$match': {
                        'timestamp' : { '$gte': 1, '$lt': 3 }
                    }
            },
            { '$project': {
                '_id': 0,
                'timestamp': 1,
                'channel_name': 1,
                'hit': { '$setIntersection': [ '$fingerprints.hash', ["7e7c85a1430d043e58ce", "2a9e6c4a0b80a06fb4b2"] ] }
            }}
        ]

        matches = db.fingerprints.aggregate(pipeline)
        for match in matches:
            pprint.pprint(match)
        
        # Get an iteratable of all the hashes we need
        # values = mapper.keys()

        # # mongo.fingerprints.Find()
        # for matche in matches:
        #     pprint.pprint(matche)
        

        # for hash, sid, offset in matches:
        #     yield (sid, offset - mapper[hash])



def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))


