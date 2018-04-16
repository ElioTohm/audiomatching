import time
from datetime import datetime
import ConfigParser
import subprocess
import requests
import ftplib
import os
from requests.auth import HTTPBasicAuth

print "Starting Record ..."
Config = ConfigParser.ConfigParser()
Config.read("config.ini")
url = Config.get("URL", "MATCHER_SERVER")
record_dir = Config.get("DIR", "CHANNEL_RECORD_DIR")
print "Loading Config..."
now = int(time.time())
n = datetime.now()
timestamp = n.strftime("%Y-%m-%d_%H-%M-%S")
print str(now) + ' ============ ' + timestamp
#start ffmpeg
subprocess.call('ffmpeg -i udp://230.1.1.2:1234?fifo_size=2000000 -map 0:p:1:1 -t 5 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MBC1_1_'+
str(now)+'.mp3 -map 0:p:2:1 -t 5 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MBC2_2_'+
str(now)+'.mp3 -map 0:p:3:1 -t 5 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MBC3_3_'+
str(now)+'.mp3 -map 0:p:4:1 -t 5 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MBC4_4_'+
str(now)+'.mp3 -map 0:p:5:1 -t 5 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MBCAction_5_'+
str(now)+'.mp3', shell=True)
print "Finished Record"
print "Uploading ..."
files = []


#try:
filename = record_dir+'/MBC1_1_'+str(now)+'.mp3'
files.append(('userrecord', open(filename, 'rb')))
filename = record_dir+'/MBC2_2_'+str(now)+'.mp3'
files.append(('userrecord', open(filename, 'rb')))
filename = record_dir+'/MBC3_3_'+str(now)+'.mp3'
files.append(('userrecord', open(filename, 'rb')))
filename = record_dir+'/MBC4_4_'+str(now)+'.mp3'
files.append(('userrecord', open(filename, 'rb')))
filename = record_dir+'/MBCAction_5_'+str(now)+'.mp3'
files.append(('userrecord', open(filename, 'rb')))

print "Finished Upload."

try:
        requests.post(url, auth=('elio','201092elio'), files=files,timeout=15)
        subprocess.call('rm '+record_dir+'/* -f', shell=True)
        print "Post Sent"
except:
        print "Request Timeout."


#except:
#       print "Failed to Upload"

