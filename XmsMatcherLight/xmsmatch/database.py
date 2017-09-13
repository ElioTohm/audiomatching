from __future__ import absolute_import
import abc

class Database(object):
    __metaclass__ = abc.ABCMeta

    FIELD_FILE_SHA1 = 'file_sha1'
    FIELD_RECORD_ID = 'record_id'
    FIELD_CHANNEL_ID = 'channel_id'
    FIELD_OFFSET = 'offset'
    FIELD_HASH = 'hash'
    FIELD_TIMESTAMP = 'timestamp'
    FIELD_CHANNEL_NAME = 'channel_name'

    # Name of your Database subclass, this is used in configuration
    # to refer to your class
    type = None

    def __init__(self):
        super(Database, self).__init__()

    # @abc.abstractmethod
    # def get_num_records(self):
    #     """
    #     Returns the amount of records in the database.
    #     """
    #     pass

    # @abc.abstractmethod
    # def get_num_fingerprints(self):
    #     """
    #     Returns the number of fingerprints in the database.
    #     """
    #     pass

    # @abc.abstractmethod
    # def set_record_fingerprinted(self, sid):
    #     """
    #     Sets a specific record as having all fingerprints in the database.

    #     sid: Song identifier
    #     """
    #     pass

    # @abc.abstractmethod
    # def get_records(self):
    #     """
    #     Returns all fully fingerprinted records in the database.
    #     """
    #     pass

    # @abc.abstractmethod
    # def get_record_by_id(self, sid):
    #     """
    #     Return a record by its identifier

    #     sid: Song identifier
    #     """
    #     pass

    # @abc.abstractmethod
    # def query(self, hash):
    #     """
    #     Returns all matching fingerprint entries associated with
    #     the given hash as parameter.

    #     hash: Part of a sha1 hash, in hexadecimal format
    #     """
    #     pass

    # @abc.abstractmethod
    # def get_iterable_kv_pairs(self):
    #     """
    #     Returns all fingerprints in the database.
    #     """
    #     pass

    @abc.abstractmethod
    def insert_hashes(self, hashes, timestamp, channel_id, channel_name, file_hash):
        """
        Insert a multitude of fingerprints.

           sid: Song identifier the fingerprints belong to
        hashes: A sequence of tuples in the format (hash, offset)
        -   hash: Part of a sha1 hash, in hexadecimal format
        - offset: Offset this hash was created from/at.
        """
        pass

    @abc.abstractmethod
    def return_matches(self, hashes, timestamp):
        """
        Searches the database for pairs of (hash, offset) values.

        hashes: A sequence of tuples in the format (hash, offset)
        -   hash: Part of a sha1 hash, in hexadecimal format
        - offset: Offset this hash was created from/at.

        Returns a sequence of (sid, offset_difference) tuples.

                      sid: Song identifier
        offset_difference: (offset - database_offset)
        """
        pass


# Import our default database handler
import xmsmatch.database_mongo
