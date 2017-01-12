import warnings
import json
warnings.filterwarnings("ignore")

from xmsmatch import Matcher
from xmsmatch.recognize import FileRecognizer, MicrophoneRecognizer

# load config from a JSON file (or anything outputting a python dictionary)
with open("xmsmatch.cnf.SAMPLE") as f:
    config = json.load(f)

if __name__ == '__main__':

	# create a Matcher instance
	djv = Matcher(config)

	# Fingerprint all the mp3's in the directory we give it
	djv.fingerprint_directory("mp3", [".mp3"])

	# Recognize audio from a file
	record = djv.recognize(FileRecognizer, "clientrecord/c_1_1.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 1 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_2_1.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 2 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_3_1.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 3 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_4_2.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 4 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_5_2.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 5 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_6_3.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 6 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_10_1.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 10 we recognized: %s\n" % record

	record = djv.recognize(FileRecognizer, "clientrecord/c_11_2.mp3")
	if record is None:
		print "Nothing recognized"
	else:
		print "From file 11 we recognized: %s\n" % record
