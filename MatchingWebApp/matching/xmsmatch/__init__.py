from matching.xmsmatch.database import get_database, Database
import decoder as decoder
import fingerprint
import multiprocessing
import os
import traceback
import sys


class Matcher(object):

    RECORD_ID = "record_id"
    CHANNEL_ID = 'channel_id'
    CONFIDENCE = 'confidence'
    MATCH_TIME = 'match_time'
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

    def fingerprint_directory(self, path, extensions, nprocesses=None):
        # Try to use the maximum amount of processes if not given.
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if nprocesses <= 0 else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        # module_dir = os.path.dirname(__file__)
        
        filenames_to_fingerprint = []
        for filename, _ in decoder.find_files(path, extensions):

            # don't refingerprint already fingerprinted files
            if decoder.unique_hash(filename) in self.recordhashes_set:
                print "%s already fingerprinted, continuing..." % filename
                continue

            filenames_to_fingerprint.append(filename)

        # Prepare _fingerprint_worker input
        worker_input = zip(filenames_to_fingerprint,
                           [self.limit] * len(filenames_to_fingerprint))

        # Send off our tasks
        iterator = pool.imap_unordered(_fingerprint_worker,
                                       worker_input)

        # Loop till we have all of them
        while True:
            try:
                channel_id, hashes, file_hash = iterator.next()
                #split the channel_id to take the timestamp and the id seperately 
                channel_info_array  = channel_id.split("_")
                channel_id =  channel_info_array[1]
                timestamp = channel_info_array[2]

            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed fingerprinting")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                sid = self.db.insert_record(channel_id, file_hash, timestamp)
                self.db.set_record_fingerprinted(sid)
                self.db.insert_hashes(sid, hashes, timestamp)
                self.get_fingerprinted_records()

        pool.close()
        pool.join()

    def fingerprint_file(self, filepath, channel_id=None):
        recordname = decoder.path_to_recordname(filepath)
        record_hash = decoder.unique_hash(filepath)
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
            sid = self.db.insert_record(channel_id, file_hash)
            self.db.set_record_fingerprinted(sid)
            self.db.insert_hashes(sid, hashes)
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
            # TODO: Clarify what `get_record_by_id` should return.
            recordname = record.get(Matcher.CHANNEL_ID, None)
        else:
            return None

        # return match info
        nseconds = round(float(largest) / fingerprint.DEFAULT_FS *
                         fingerprint.DEFAULT_WINDOW_SIZE *
                         fingerprint.DEFAULT_OVERLAP_RATIO, 5)
        record = {
            Matcher.RECORD_ID : record_id,
            Matcher.CHANNEL_ID : recordname,
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

        os.unlink(filename)
        
        # delete file after fingerprinting

    return channel_id, result, file_hash


def chunkify(lst, n):
    """
    Splits a list into roughly n equal parts.
    http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
    """
    return [lst[i::n] for i in xrange(n)]
