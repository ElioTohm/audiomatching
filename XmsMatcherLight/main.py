''' 
    Main flaskr
'''
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from celery import Celery
from celery.schedules import crontab
from flask_pymongo import PyMongo
from bson import Binary, Code
from bson.json_util import dumps, loads
import json
import os
import datetime
import time
from pprint import pprint
from werkzeug.utils import secure_filename
from xmsmatch import Matcher
from xmsmatch.recognize import FileRecognizer
import paho.mqtt.publish as publish
import random

def make_celery(app):

    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])

    celery.conf.update(app.config)

    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

app = Flask(__name__)
api = Api(app)

app.config.update(
    CELERY_BROKER_URL='pyamqp://xms:987456321rabbitmq@127.0.0.1:5672/xms',
    CELERY_RESULT_BACKEND='mongodb://127.0.0.1/celery',
    MONGO_DBNAME='database',
    # MONGO_USERNAME='xmsmongodb',
    # MONGO_PASSWORD='xmsPrro123mongo',
    MONGO_CONNECT=False
)

mongo = PyMongo(app)

celery = make_celery(app)

parser = reqparse.RequestParser()

@celery.task()
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

        mongo.db.fingerprints.insert_one(djv.fingerprint_file(module_dir + '/mp3/' + mp3file, mp3file))
        
        # delete file after fingerprinting
        os.unlink(module_dir + '/mp3/' + mp3file)

        return "done"


@celery.task()
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

        client_file_path = os.path.join(module_dir, 'clientrecord/' + clientrecording)
        record = djv.recognize(FileRecognizer, client_file_path)
       
        os.unlink(client_file_path)
        client_id = str(clientrecording).split("_")

        if record is None:
            timestamp = str(client_id[2]).split(".")
            result.append({'none':client_file_path, 'client_id': client_id[1],
                           'timestamp': int(timestamp[0]), 'channel_name':'Muted', 'confidence':'Muted'})
        else:
            result.append(record)
        
        mongo.db.records.insert_many(result)

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
        Set up periodic task for celerybeat
    """
    channels = ['']
    sender.add_periodic_task(120.0, periodicadddata.s(), name='periodic')

@celery.task()
def periodicadddata():
    """
        task to be added periodically
    """
    clients = ['1', '2', '3', '9', '7', '4', '5', '10', '11', '15', '16', '17',
                '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
                '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75',
                '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', 
                '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111']
    channels = ['MBCAction', 'MBC1', 'MBC2', 'MBC3', 'MBC4', 'Muted', 'Other']

    timestamp = int(time.time())
    if timestamp % 60 < 30:
        timestamp = timestamp - (timestamp % 60)
    else:
        timestamp = timestamp - (timestamp % 60) + 60

    records = []
    for client_id in clients:
        records.append({'channel_name': random.choice(channels), 'client_id': client_id,
                        'confidence': 2500, 'timestamp': timestamp})

    mongo.db.records.insert_many(records)


class FingerprintRequest (Resource) :
    def post(self):
        module_dir = os.path.dirname(__file__)
        
        # Write files to storage
        for clientrecording in request.files.getlist('serverrecord'):
            if clientrecording.filename.endswith('.mp3'):
                filename = secure_filename(clientrecording.filename)
                clientrecording.save(os.path.join(module_dir +'/mp3/', filename))
        
        # Loop through the file written to storage and fingerprint them
        for clientrecording in request.files.getlist('serverrecord'):
            fingerprint.delay(clientrecording.filename)
        
        return 200

class MatchRequest (Resource) :
    def post(self):
        module_dir = os.path.dirname(__file__)
        if not os.path.exists(module_dir + '/clientrecord/'):
            os.mkdir(module_dir + '/clientrecord/')

        for clientrecording in request.files.getlist('client_record'):
            if clientrecording.filename.endswith('.mp3'):
                filename = secure_filename(clientrecording.filename)
                clientrecording.save(os.path.join(module_dir +'/clientrecord/', filename))
        
        for clientrecording in request.files.getlist('client_record'):
            match.apply_async((clientrecording.filename,), countdown=5)
            # match.delay(clientrecording.filename)
        
        return 200

class RegisterRequest (Resource) :
    """
        register the client
    """
    def post(self):
        data = json.loads(request.data)

        client_name = 'Unknown'
        if data['name'] != "":
            client_name = data['name']

        client_inserted = getnextsequence(mongo.db.counters, "client_id")
        if not data['long'] or not data['lat']:
            mongo.db.clients.insert({'_id': client_inserted, 'name': client_name})

        else:
            mongo.db.clients.insert({'_id': client_inserted, 'name': client_name,
                               'lon': data['long'], 'lat': data['lat']})

        return Response({'registered': client_inserted, 'version': 0})

    def getnextsequence(collection, name):
        """
            read last number form counters document
            and increament
        """
        return collection.find_and_modify(query={'_id': name},
                                      update={'$inc': {'seq': 1}}, new=True).get('seq')

class ClientUpdateRequest(Resource):
    """
    Publish message to clients
    """
    def post(self):
        message = "{version:%s, update: %s}" % (1, True)

        publish.single("Client", payload=json.dumps(message), hostname="localhost", port=1883,
                       auth={'username':'pahopmclient', 'password':'xms@pmclient#12345'})

        return HttpResponse('200')

    def get(self):
        template = loader.get_template('clientmanager/clientadmin.html')
        return HttpResponse(template.render())


##
## Actually setup the Api resource routing here
##
api.add_resource(FingerprintRequest, '/fingerprint/')
api.add_resource(MatchRequest, '/matching/match/')
api.add_resource(RegisterRequest, '/register/')
api.add_resource(ClientUpdateRequest, '/update/')

if __name__ == '__main__':
    app.run(debug=True)
    