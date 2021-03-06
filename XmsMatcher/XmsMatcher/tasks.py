# Create your tasks here
from __future__ import absolute_import, unicode_literals
from celery import shared_task
from pymongo import MongoClient
import warnings
import json
import os
from django.conf import settings

warnings.filterwarnings("ignore")

from XmsMatcher.xmsmatch import Matcher
from XmsMatcher.xmsmatch.recognize import FileRecognizer
import datetime
import pprint
from pymongo import MongoClient

@shared_task
def fingerprint(mp3file):
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
        djv.fingerprint_file(module_dir + '/mp3/' + mp3file, mp3file)


@shared_task
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
