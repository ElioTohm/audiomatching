from __future__ import absolute_import, unicode_literals
import os
import time
from pymongo import MongoClient
from celery import Celery
from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'XmsMatcher.settings')

app = Celery('XmsMatcher')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings')

# Load task modules from all registered Django app configs.
# app.autodiscover_tasks()

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 2mins.
    sender.add_periodic_task(120.0, periodicAddData.s('MANAR', 2500, '21'), name='joe-congo 1')
    sender.add_periodic_task(120.0, periodicAddData.s('ALJADEED', 2500, '20'), name='joe-congo 2')

@app.task
def periodicAddData(channel_name, confidence, client_name):
    timestamp = int(time.time())
    if timestamp % 60 < 30:
        timestamp = timestamp - (timestamp % 60)
    else:
        timestamp = timestamp - (timestamp % 60) + 60

    print '{} {}'.format(client_name, timestamp)
    client = MongoClient('localhost', 27017)
    db = client['database']

    db.records.insert_one({'channel_name': channel_name, 'client_id': client_name,
                           'confidence': confidence, 'timestamp': timestamp})
