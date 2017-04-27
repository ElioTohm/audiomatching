from XmsMatcher.xmsmatch.database import get_database, Database
import decoder as decoder
import fingerprint
import multiprocessing.dummy
import os
import traceback
import sys


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
        db_cls = get_database(config.get("database_type", None))

        self.db = db_cls(**config.get("database", {}))
        self.db.setup()

        # if we should limit seconds fingerprinted,
        # None|-1 means use entire track
        self.limit = self.config.get("fingerprint_limit", None)
        if self.limit == -1:  # for JSON compatibility
            self.limit = None
        self.get_fingerprinted_records()

    def get_fingerprinted_records(self):
        # get records previously indexed
        self.records = self.db.get_records()
        self.recordhashes_set = set()  # to know which ones we've computed before
        for record in self.records:
            record_hash = record[Database.FIELD_FILE_SHA1]
            self.recordhashes_set.add(record_hash)


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
        # don't refingerprint already fingerprinted files
        if record_hash in self.recordhashes_set:
            print "%s already fingerprinted, continuing..." % channel_id
        else:
            channel_id, hashes, file_hash = _fingerprint_worker(
                filepath,
                self.limit,
                channel_id=channel_id
            )
            sid = self.db.insert_record(channel_id, channel_name, file_hash, timestamp)
            self.db.set_record_fingerprinted(sid)
            self.db.insert_hashes(sid, hashes, timestamp, channel_id, channel_name, file_hash)
            self.get_fingerprinted_records()

    def find_matches(self, samples, timestamp, Fs=fingerprint.DEFAULT_FS):
        hashes = fingerprint.fingerprint(samples, Fs=Fs)
        return self.db.return_matches(hashes, timestamp)

    def align_matches(self, matches, timestamp, client_id):
        """
            Finds hash matches that align in time with other matches and finds
            consensus about which hashes are "true" signal from the audio.

            Returns a dictionary with match information.
        """
        # align by diffs
        diff_counter = {}
        largest = 0
        largest_count = 0
        record_id = -1
        for tup in matches:
            sid, diff = tup
            if diff not in diff_counter:
                diff_counter[diff] = {}
            if sid not in diff_counter[diff]:
                diff_counter[diff][sid] = 0
            diff_counter[diff][sid] += 1

            if diff_counter[diff][sid] > largest_count:
                largest = diff
                largest_count = diff_counter[diff][sid]
                record_id = sid

        # extract idenfication
        record = self.db.get_record_by_id(record_id)
        if record:

            recordname = record.get(Matcher.CHANNEL_ID, None)
            channel_name = record.get(Matcher.CHANNEL_NAME, None)
        else:
            return None

        # return match info
        nseconds = round(float(largest) / fingerprint.DEFAULT_FS *
                         fingerprint.DEFAULT_WINDOW_SIZE *
                         fingerprint.DEFAULT_OVERLAP_RATIO, 5)

        if timestamp % 60 < 30:
            timestamp = timestamp - (timestamp % 60)
        else:
            timestamp = timestamp - (timestamp % 60) + 60

        record = {
            Matcher.RECORD_ID : record_id,
            Matcher.CHANNEL_ID : recordname,
            Matcher.CHANNEL_NAME : channel_name,
            Matcher.CONFIDENCE : largest_count,
            Matcher.OFFSET : int(largest),
            Matcher.OFFSET_SECS : nseconds,
            Database.FIELD_FILE_SHA1 : record.get(Database.FIELD_FILE_SHA1, None),
            Database.FIELD_TIMESTAMP: timestamp,
            'client_id' : client_id
            }
        return record

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

        # delete file after fingerprinting
        os.unlink(filename)

    return channel_id, result, file_hash


def chunkify(lst, n):
    """
    Splits a list into roughly n equal parts.
    http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
    """
    return [lst[i::n] for i in xrange(n)]
