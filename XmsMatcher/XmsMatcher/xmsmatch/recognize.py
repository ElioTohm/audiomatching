import XmsMatcher.xmsmatch.fingerprint as fingerprint
import XmsMatcher.xmsmatch.decoder as decoder
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
        timestamp = int(timestamp_without_mp3[0]) + 7200

        t = time.time()
        match = self._recognize(timestamp, filename_info_array[1], *frames)
        t = time.time() - t

        if match:
            match['match_time'] = t

        return match

    def recognize(self, filename):
        return self.recognize_file(filename)


class MicrophoneRecognizer(BaseRecognizer):
    default_chunksize   = 8192
    default_format      = pyaudio.paInt16
    default_channels    = 2
    default_samplerate  = 44100

    def __init__(self, xmsmatch):
        super(MicrophoneRecognizer, self).__init__(xmsmatch)
        self.audio = pyaudio.PyAudio()
        self.stream = None
        self.data = []
        self.channels = MicrophoneRecognizer.default_channels
        self.chunksize = MicrophoneRecognizer.default_chunksize
        self.samplerate = MicrophoneRecognizer.default_samplerate
        self.recorded = False

    def start_recording(self, channels=default_channels,
                        samplerate=default_samplerate,
                        chunksize=default_chunksize):
        self.chunksize = chunksize
        self.channels = channels
        self.recorded = False
        self.samplerate = samplerate

        if self.stream:
            self.stream.stop_stream()
            self.stream.close()

        self.stream = self.audio.open(
            format=self.default_format,
            channels=channels,
            rate=samplerate,
            input=True,
            frames_per_buffer=chunksize,
        )

        self.data = [[] for i in range(channels)]

    def process_recording(self):
        data = self.stream.read(self.chunksize)
        nums = np.fromstring(data, np.int16)
        for c in range(self.channels):
            self.data[c].extend(nums[c::self.channels])

    def stop_recording(self):
        self.stream.stop_stream()
        self.stream.close()
        self.stream = None
        self.recorded = True

    def recognize_recording(self):
        if not self.recorded:
            raise NoRecordingError("Recording was not complete/begun")
        return self._recognize(*self.data)

    def get_recorded_time(self):
        return len(self.data[0]) / self.rate

    def recognize(self, seconds=10):
        self.start_recording()
        for i in range(0, int(self.samplerate / self.chunksize
                              * seconds)):
            self.process_recording()
        self.stop_recording()
        return self.recognize_recording()


class NoRecordingError(Exception):
    pass
