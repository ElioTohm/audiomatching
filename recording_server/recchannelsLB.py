import time
import ConfigParser
from datetime import datetime
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
subprocess.call('ffmpeg -i udp://230.1.1.1:1234?fifo_size=2000000 -map 0:p:1:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/LBCI_1_'+
str(now)+'.mp3 -map 0:p:2:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MTV_2_'+
str(now)+'.mp3 -map 0:p:3:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/OTV_3_'+
str(now)+'.mp3 -map 0:p:4:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/ALJADEED_4_'+
str(now)+'.mp3 -map 0:p:5:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/TL_5_'+
str(now)+'.mp3 -map 0:p:6:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/MANAR_6_'+
str(now)+'.mp3 -map 0:p:7:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/FUTURE_7_'+
str(now)+'.mp3 -map 0:p:8:1 -t 30 -vn -acodec libmp3lame -ac 1 -ab 64000 -ar 48000 '+record_dir+'/NBN_8_'
+str(now)+'.mp3', shell=True)
print "Finished Record"
print "Uploading ..."
files = []


#try:
#session = ftplib.FTP('192.168.33.235','elio','201092')
filename = '/home/user/'+record_dir+'/LBCI_1_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/LBCI_1_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/MTV_2_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/MTV_2_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/OTV_3_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/OTV_3_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/ALJADEED_4_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/ALJADEED_4_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/TL_5_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/TL_5_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/MANAR_6_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/MANAR_6_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/FUTURE_7_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/FUTURE_7_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)
filename = '/home/user/'+record_dir+'/NBN_8_'+str(now)+'.mp3'
#file = open(filename, 'rb')
#session.storbinary('STOR Documents/DEV/XmsMatcher/XmsMatcher/mp3/NBN_8_'+str(now)+'.mp3', file)
files.append(('userrecord', open(filename, 'rb')))
#file.close()
#os.unlink(filename)

#session.quit()

print "Finished Upload."

try:
        requests.post(url, auth=('elio','201092elio'), files=files,timeout=15)
        subprocess.call('rm /home/user/'+record_dir+'/* -f', shell=True)
        print "Post Sent"
except:
        print "Request Timeout."


#except:
#       print "Failed to Upload"

