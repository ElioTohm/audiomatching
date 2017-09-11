from __future__ import absolute_import, unicode_literals
import os
import time
from pymongo import MongoClient
from celery import Celery
from celery.schedules import crontab
import json
from XmsMatcher.xmsmatch import Matcher
from XmsMatcher.xmsmatch.recognize import FileRecognizer
import datetime
from django.conf import settings

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'XmsMatcher.settings')

# app = Celery('XmsMatcher', broker='pyamqp://xms:987456321rabbitmq@127.0.0.1:5672/xms')
app = Celery('XmsMatcher', broker='pyamqp://xms:987456321rabbitmq@127.0.0.1:5672/')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
        Set up periodic task for celerybeat
    """
    # Calls test('hello') every 2mins.
    sender.add_periodic_task(120.0, periodicadddata.s('MTV', 2500, '20'), name='joe-congo 1')
    # sender.add_periodic_task(120.0, periodicadddata.s('GeoTV', 2500, '21'), name='dubai-1')
    # sender.add_periodic_task(120.0, periodicadddata.s('ExpressNews', 2500, '22'), name='dubai-2')
    # sender.add_periodic_task(120.0, periodicadddata.s('GeoTV', 2500, '23'), name='dubai-3')
    sender.add_periodic_task(120.0, periodicadddata.s('Other', 2500, '21'), name='dubai-1')
    sender.add_periodic_task(120.0, periodicadddata.s('Other', 2500, '22'), name='dubai-2')
    sender.add_periodic_task(120.0, periodicadddata.s('Other', 2500, '23'), name='dubai-3')

@app.task
def periodicadddata(channel_name, confidence, client_name):
    """
        task to be added periodically
    """
    timestamp = int(time.time())
    if timestamp % 60 < 30:
        timestamp = timestamp - (timestamp % 60)
    else:
        timestamp = timestamp - (timestamp % 60) + 60

    print '{} {}'.format(client_name, timestamp)
    client = MongoClient('localhost', 27017)
    mongo_db = client['database']

    mongo_db.records.insert_one({'channel_name': channel_name, 'client_id': client_name,
                                 'confidence': confidence, 'timestamp': timestamp})

@app.task(bind=True)
def fingerprint(recordarray):
    """
        fingerprint task
    """
	# get current directory
    module_dir = os.path.dirname(__file__)
    file_path = os.path.join(module_dir, 'xmsmatch.cnf.SAMPLE')
    with open(file_path) as f:
	    # load config from a JSON file (or anything outputting a python dictionary)
        config = json.load(f)

	    # create a Matcher instance
        djv = Matcher(config)
        # for mp3file in recordarray:
            # djv.fingerprint_file(module_dir + '/mp3/' + mp3file, mp3file)
        djv.fingerprint_directory(module_dir + '/mp3/', recordarray)


@app.task(bind=True)
def match(clientrecording):
    """
        match task
    """
    module_dir = os.path.dirname(__file__)  # get current directory
    file_path = os.path.join(module_dir, 'xmsmatch.cnf.SAMPLE')
    with open(file_path) as f:
        config = json.load(f)

        # create a Matcher instance
        djv = Matcher(config)
        result = list()

        client_file_path = os.path.join(module_dir, 'clientrecord/' + str(clientrecording))
        record = djv.recognize(FileRecognizer, client_file_path)
        client_id = str(clientrecording).split("_")

        if record is None:
            timestamp = str(client_id[2]).split(".")
            result.append({'none':client_file_path, 'client_id': client_id[1],
                           'timestamp': timestamp[0], 'channel_name':'Muted', 'confidence':'Muted'})
        else:
            result.append(record)

        # remove file matched
        print result
        os.unlink(client_file_path)

        client = MongoClient('127.0.0.1', 27017)
        db = client['database']
        db.authenticate(settings.MONGO_USER, settings.MONGO_PASS)
        collection = db.records

        collection.insert_many(result)
