'''
matching code 
'''
import os
import traceback
import sys
import xmsmatch.database_mongo as database_mongo
import xmsmatch.decoder as decoder
import xmsmatch.fingerprint as fingerprint
import numpy as np
from pprint import pprint

class Matcher(object):

    RECORD_ID = "record_id"
    CHANNEL_ID = 'channel_id'
    CONFIDENCE = 'confidence'
    MATCH_TIME = 'match_time'
    CHANNEL_NAME = 'channel_name'
    OFFSET = 'offset'
    OFFSET_SECS = 'offset_seconds'

    def __init__(self, config):
        super(Matcher, self).__init__()

        self.config = config

        # initialize db
        self.db = database_mongo.MongoDatabase()

        # if we should limit seconds fingerprinted,
        # None|-1 means use entire track
        self.limit = self.config.get("fingerprint_limit", None)
        if self.limit == -1:  # for JSON compatibility
            self.limit = None


    def fingerprint_file(self, filepath, channel_id=None):
        recordname = decoder.path_to_recordname(filepath)
        record_hash = decoder.unique_hash(filepath)

        channel_info_array = channel_id.split("_")
        channel_id = channel_info_array[1]
        timestamp = channel_info_array[2]
        timestamp = timestamp.split(".")
        timestamp = timestamp[0]
        channel_name = channel_info_array[0]

        channel_id = channel_id or recordname
        channel_id, hashes, file_hash = _fingerprint_worker(
            filepath,
            self.limit,
            channel_id=channel_id
        )
        return self.db.insert_hashes(hashes, timestamp, channel_id, channel_name, file_hash)

    def find_matches(self, samples, timestamp, Fs=fingerprint.DEFAULT_FS):
        hashes = fingerprint.fingerprint(samples, Fs=Fs)
        return self.db.return_matches(hashes, timestamp)

    def align_matches(self, matches, timestamp, client_id):
        """
            Finds hash matches that align in time with other matches and finds
            consensus about which hashes are "true" signal from the audio.

            Returns a dictionary with match information.
        """
        if timestamp % 60 < 30:
            timestamp = timestamp - (timestamp % 60)
        else:
            timestamp = timestamp - (timestamp % 60) + 60
        
        if not matches:
            return None
        else:
            matches[0]['timestamp'] = timestamp

            return matches[0]

    def recognize(self, recognizer, *options, **kwoptions):
        r = recognizer(self)
        return r.recognize(*options, **kwoptions)

def _fingerprint_worker(filename, limit=None, channel_id=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass

    recordname, extension = os.path.splitext(os.path.basename(filename))
    channel_id = channel_id or recordname
    channels, Fs, file_hash = decoder.read(filename, limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        print("Fingerprinting channel %d/%d for %s" % (channeln + 1,
                                                       channel_amount,
                                                       filename))
        hashes = fingerprint.fingerprint(channel, Fs=Fs)
        print("Finished channel %d/%d for %s" % (channeln + 1, channel_amount,
                                                 filename))
        result |= set(hashes)


    return channel_id, result, file_hash


def chunkify(lst, n):
    """
    Splits a list into roughly n equal parts.
    http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
    """
    return [lst[i::n] for i in xrange(n)]
