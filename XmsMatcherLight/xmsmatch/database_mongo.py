from __future__ import absolute_import
from itertools import izip_longest
import Queue


import MySQLdb as mysql
from MySQLdb.cursors import DictCursor

from xmsmatch.database import Database

import json
import os

from pymongo import MongoClient
from django.conf import settings

class SQLDatabase(Database):
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

    type = "mysql"

    # tables
    FINGERPRINTS_TABLENAME = "fingerprints"
    RECORD_TABLE = "records"

    # fields
    FIELD_FINGERPRINTED = "fingerprinted"

    # inserts (ignores duplicates)
    INSERT_FINGERPRINT = """
        INSERT IGNORE INTO %s (%s, %s, %s, %s) values
            (UNHEX(%%s), %%s, %%s, %%s);
    """ % (FINGERPRINTS_TABLENAME, Database.FIELD_HASH, Database.FIELD_RECORD_ID, Database.FIELD_OFFSET, Database.FIELD_TIMESTAMP)

    # selects
    SELECT = """
        SELECT %s, %s FROM %s WHERE %s = UNHEX(%%s);
    """ % (Database.FIELD_RECORD_ID, Database.FIELD_OFFSET, FINGERPRINTS_TABLENAME, Database.FIELD_HASH)

    # use only chunk of data with the correspongig timestamp
    # have to send timestamp programatically
    SELECT_MULTIPLE = """
        SELECT HEX(%s), %s, %s FROM ( SELECT %s, %s, %s FROM %s WHERE %s BETWEEN %%s AND %%s) AS T WHERE %s IN (%%s);
    """ % (Database.FIELD_HASH, Database.FIELD_RECORD_ID, 
            Database.FIELD_OFFSET, Database.FIELD_HASH, Database.FIELD_RECORD_ID, Database.FIELD_OFFSET,
           FINGERPRINTS_TABLENAME, Database.FIELD_TIMESTAMP, Database.FIELD_HASH)

    SELECT_ALL = """
        SELECT %s, %s FROM %s;
    """ % (Database.FIELD_RECORD_ID, Database.FIELD_OFFSET, FINGERPRINTS_TABLENAME)

    SELECT_RECORD = """
        SELECT %s, %s, HEX(%s) as %s FROM %s WHERE %s = %%s;
    """ % (Database.FIELD_CHANNEL_ID, Database.FIELD_CHANNEL_NAME, Database.FIELD_FILE_SHA1, Database.FIELD_FILE_SHA1, RECORD_TABLE, Database.FIELD_RECORD_ID)

    SELECT_NUM_FINGERPRINTS = """
        SELECT COUNT(*) as n FROM %sx`
    """ % (FINGERPRINTS_TABLENAME)

    SELECT_UNIQUE_RECORD_IDS = """
        SELECT COUNT(DISTINCT %s) as n FROM %s WHERE %s = 1;
    """ % (Database.FIELD_RECORD_ID, RECORD_TABLE, FIELD_FINGERPRINTED)

    SELECT_RECORDS = """
        SELECT %s, %s, HEX(%s) as %s FROM %s WHERE %s = 1;
    """ % (Database.FIELD_RECORD_ID, Database.FIELD_CHANNEL_ID, Database.FIELD_FILE_SHA1, Database.FIELD_FILE_SHA1,
           RECORD_TABLE, FIELD_FINGERPRINTED)

    # drops
    DROP_FINGERPRINTS = "DROP TABLE IF EXISTS %s;" % FINGERPRINTS_TABLENAME
    DROP_RECORDS = "DROP TABLE IF EXISTS %s;" % RECORD_TABLE

    # update
    UPDATE_RECORD_FINGERPRINTED = """
        UPDATE %s SET %s = 1 WHERE %s = %%s
    """ % (RECORD_TABLE, FIELD_FINGERPRINTED, Database.FIELD_RECORD_ID)

    # delete
    DELETE_UNFINGERPRINTED = """
        DELETE FROM %s WHERE %s = 0;
    """ % (RECORD_TABLE, FIELD_FINGERPRINTED)


    # def query(self, hash):
    #     """
    #     Return all tuples associated with hash.

    #     If hash is None, returns all entries in the
    #     database (be careful with that one!).
    #     """
    #     # select all if no key
    #     query = self.SELECT_ALL if hash is None else self.SELECT

    #     with self.cursor() as cur:
    #         cur.execute(query)
    #         for sid, offset in cur:
    #             yield (sid, offset)

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, hashes, timestamp, channel_id, channel_name, file_hash):
        """
        Insert series of hash => record_id, offset
        values into the database.
        """
        values = []
        for hash, offset in hashes:
            values.append({"hash": hash, "offset": offset})
        
        return {"channel_id": channel_id, "channel_name": channel_name, 
                                "file_hash": file_hash, "timestamp": timestamp,
                                "fingerprints": values }

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
            mapper[hash.upper()] = offset

        # Get an iteratable of all the hashes we need
        values = mapper.keys()

        # mongo.fingerprints.Find()
        # for hash, sid, offset in cur:
            # (sid, db_offset - record_sampled_offset)
            # yield (sid, offset - mapper[hash])



def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))


