import xmsmatch.fingerprint as fingerprint
import xmsmatch.decoder as decoder
import numpy as np
import pyaudio
import time


class BaseRecognizer(object):

    def __init__(self, xmsmatch):
        self.xmsmatch = xmsmatch
        self.Fs = fingerprint.DEFAULT_FS

    def _recognize(self, timestamp, client_id, *data):
        matches = []
        for d in data:
            matches.extend(self.xmsmatch.find_matches(d, timestamp, Fs=self.Fs))
        return self.xmsmatch.align_matches(matches, timestamp, client_id)

    def recognize(self):
        pass  # base class does nothing


class FileRecognizer(BaseRecognizer):
    def __init__(self, xmsmatch):
        super(FileRecognizer, self).__init__(xmsmatch)

    def recognize_file(self, filename):
        frames, self.Fs, file_hash = decoder.read(filename, self.xmsmatch.limit)
        filename_info_array = filename.split("_")
        timestamp_without_mp3 = filename_info_array[2].split(".") 
        timestamp = int(timestamp_without_mp3[0])

        t = time.time()
        match = self._recognize(timestamp, filename_info_array[1], *frames)
        t = time.time() - t

        if match:
            match['match_time'] = t

        return match

    def recognize(self, filename):
        return self.recognize_file(filename)

