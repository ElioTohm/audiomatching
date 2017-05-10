from __future__ import absolute_import, unicode_literals
import os
import time
from pymongo import MongoClient
from celery import Celery
from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'XmsMatcher.settings')

app = Celery('XmsMatcher', broker='pyamqp://xms:987456321rabbitmq@127.0.0.1:5672/xms')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings')

# Load task modules from all registered Django app configs.
# app.autodiscover_tasks()

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
