from __future__ import division
from pydub import AudioSegment
from xmsmatch.decoder import path_to_recordname
from xmsmatch import Matcher
from xmsmatch.fingerprint import *
import traceback
import fnmatch
import os, re, ast
import subprocess
import random
import logging

def set_seed(seed=None):
    """
    `seed` as None means that the sampling will be random. 

    Setting your own seed means that you can produce the 
    same experiment over and over. 
    """
    if seed != None:
        random.seed(seed)

def get_files_recursive(src, fmt):
    """
    `src` is the source directory. 
    `fmt` is the extension, ie ".mp3" or "mp3", etc.
    """
    for root, dirnames, filenames in os.walk(src):
        for filename in fnmatch.filter(filenames, '*' + fmt):
            yield os.path.join(root, filename)

def get_length_audio(audiopath, extension):
    """
    Returns length of audio in seconds. 
    Returns None if format isn't supported or in case of error. 
    """
    try:
        audio = AudioSegment.from_file(audiopath, extension.replace(".", ""))
    except:
        print "Error in get_length_audio(): %s" % traceback.format_exc()
        return None
    return int(len(audio) / 1000.0)

def get_starttime(length, nseconds, padding):
    """
    `length` is total audio length in seconds
    `nseconds` is amount of time to sample in seconds
    `padding` is off-limits seconds at beginning and ending
    """
    maximum = length - padding - nseconds
    if padding > maximum:
        return 0
    return random.randint(padding, maximum)

def generate_test_files(src, dest, nseconds, fmts=[".mp3", ".wav"], padding=10):
    """
    Generates a test file for each file recursively in `src` directory
    of given format using `nseconds` sampled from the audio file. 

    Results are written to `dest` directory.

    `padding` is the number of off-limit seconds and the beginning and
    end of a track that won't be sampled in testing. Often you want to 
    avoid silence, etc. 
    """
    # create directories if necessary
    for directory in [src, dest]:
        try:
            os.stat(directory)
        except:
            os.mkdir(directory)

    # find files recursively of a given file format
    for fmt in fmts:
        testsources = get_files_recursive(src, fmt) 
        for audiosource in testsources:

            print "audiosource:", audiosource
            
            filename, extension = os.path.splitext(os.path.basename(audiosource))
            length = get_length_audio(audiosource, extension) 
            starttime = get_starttime(length, nseconds, padding)

            test_file_name = "%s_%s_%ssec.%s" % (
                os.path.join(dest, filename), starttime, 
                nseconds, extension.replace(".", ""))
            
            subprocess.check_output([
                "ffmpeg", "-y",
                "-ss", "%d" % starttime, 
                '-t' , "%d" % nseconds, 
                "-i", audiosource,
                test_file_name])

def log_msg(msg, log=True, silent=False):
    if log:
        logging.debug(msg)
    if not silent:
        print msg

def autolabel(rects, ax):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height, 
            '%d' % int(height), ha='center', va='bottom')

def autolabeldoubles(rects, ax):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height, 
            '%s' % round(float(height), 3), ha='center', va='bottom')

class MatcherTest(object):
    def __init__(self, folder, seconds):
        super(MatcherTest, self).__init__()

        self.test_folder = folder
        self.test_seconds = seconds
        self.test_records = []

        print "test_seconds", self.test_seconds

        self.test_files = [
            f for f in os.listdir(self.test_folder) 
            if os.path.isfile(os.path.join(self.test_folder, f)) 
            and re.findall("[0-9]*sec", f)[0] in self.test_seconds]

        print "test_files", self.test_files

        self.n_columns = len(self.test_seconds)
        self.n_lines = int(len(self.test_files) / self.n_columns)

        print "columns:", self.n_columns
        print "length of test files:", len(self.test_files)
        print "lines:", self.n_lines

        # variable match results (yes, no, invalid)
        self.result_match = [[0 for x in xrange(self.n_columns)] for x in xrange(self.n_lines)] 

        print "result_match matrix:", self.result_match 

        # variable match precision (if matched in the corrected time)
        self.result_matching_times = [[0 for x in xrange(self.n_columns)] for x in xrange(self.n_lines)] 

        # variable mahing time (query time) 
        self.result_query_duration = [[0 for x in xrange(self.n_columns)] for x in xrange(self.n_lines)] 

        # variable confidence
        self.result_match_confidence = [[0 for x in xrange(self.n_columns)] for x in xrange(self.n_lines)] 

        self.begin()

    def get_column_id (self, secs):
        for i, sec in enumerate(self.test_seconds):
            if secs == sec:
                return i

    def get_line_id (self, record):
        for i, s in enumerate(self.test_records):
            if record == s:
                return i
        self.test_records.append(record)
        return len(self.test_records) - 1

    def create_plots(self, name, results, results_folder):
        for sec in range(0, len(self.test_seconds)):
            ind = np.arange(self.n_lines) #
            width = 0.25       # the width of the bars

            fig = plt.figure()
            ax = fig.add_subplot(111)
            ax.set_xlim([-1 * width, 2 * width])

            means_dvj = [x[0] for x in results[sec]]
            rects1 = ax.bar(ind, means_dvj, width, color='r')

            # add some
            ax.set_ylabel(name)
            ax.set_title("%s %s Results" % (self.test_seconds[sec], name)) 
            ax.set_xticks(ind + width)

            labels = [0 for x in range(0, self.n_lines)]
            for x in range(0, self.n_lines):
                labels[x] = "record %s" % (x+1)
            ax.set_xticklabels(labels)

            box = ax.get_position()
            ax.set_position([box.x0, box.y0, box.width * 0.75, box.height])

            #ax.legend( (rects1[0]), ('Matcher'), loc='center left', bbox_to_anchor=(1, 0.5))

            if name == 'Confidence':
                autolabel(rects1, ax)
            else:
                autolabeldoubles(rects1, ax)

            plt.grid()

            fig_name = os.path.join(results_folder, "%s_%s.png" % (name, self.test_seconds[sec]))
            fig.savefig(fig_name)

    def begin(self):
        for f in self.test_files:
            log_msg('--------------------------------------------------')
            log_msg('file: %s' % f)

            # get column 
            col = self.get_column_id(re.findall("[0-9]*sec", f)[0])
            # format: XXXX_offset_length.mp3
            record = path_to_recordname(f).split("_")[0]  
            line = self.get_line_id(record)
            result = subprocess.check_output([
                "python", 
                "xmsmatch.py",
                '-r',
                'file', 
                self.test_folder + "/" + f])

            if result.strip() == "None":
                log_msg('No match')
                self.result_match[line][col] = 'no'
                self.result_matching_times[line][col] = 0
                self.result_query_duration[line][col] = 0
                self.result_match_confidence[line][col] = 0
            
            else:
                result = result.strip()
                result = result.replace(" \'", ' "')
                result = result.replace("{\'", '{"')
                result = result.replace("\':", '":')
                result = result.replace("\',", '",')

                # which record did we predict?
                result = ast.literal_eval(result)
                record_result = result["channel_id"]
                log_msg('record: %s' % record)
                log_msg('record_result: %s' % record_result)

                if record_result != record:
                    log_msg('invalid match')
                    self.result_match[line][col] = 'invalid'
                    self.result_matching_times[line][col] = 0
                    self.result_query_duration[line][col] = 0
                    self.result_match_confidence[line][col] = 0
                else:
                    log_msg('correct match')
                    print self.result_match
                    self.result_match[line][col] = 'yes'
                    self.result_query_duration[line][col] = round(result[Matcher.MATCH_TIME],3)
                    self.result_match_confidence[line][col] = result[Matcher.CONFIDENCE]

                    record_start_time = re.findall("\_[^\_]+",f)
                    record_start_time = record_start_time[0].lstrip("_ ")

                    result_start_time = round((result[Matcher.OFFSET] * DEFAULT_WINDOW_SIZE * 
                        DEFAULT_OVERLAP_RATIO) / (DEFAULT_FS), 0)

                    self.result_matching_times[line][col] = int(result_start_time) - int(record_start_time)
                    if (abs(self.result_matching_times[line][col]) == 1):
                        self.result_matching_times[line][col] = 0

                    log_msg('query duration: %s' % round(result[Matcher.MATCH_TIME],3))
                    log_msg('confidence: %s' % result[Matcher.CONFIDENCE])
                    log_msg('record start_time: %s' % record_start_time)
                    log_msg('result start time: %s' % result_start_time)
                    if (self.result_matching_times[line][col] == 0):
                        log_msg('accurate match')
                    else:
                        log_msg('inaccurate match')
            log_msg('--------------------------------------------------\n')




