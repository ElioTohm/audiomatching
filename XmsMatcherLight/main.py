''' 
    Main flaskr
'''
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from celery import Celery
from flask_pymongo import PyMongo
from bson import Binary, Code
from bson.json_util import dumps, loads
import json
import os
import datetime
import pprint
from werkzeug.utils import secure_filename
from xmsmatch import Matcher
from xmsmatch.recognize import FileRecognizer

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
    CELERY_BROKER_URL='pyamqp://xms:987456321rabbitmq@127.0.0.1:5672/',
    CELERY_RESULT_BACKEND='mongodb://127.0.0.1/celery',
    MONGO_DBNAME='database'
)

mongo = PyMongo(app)

celery = make_celery(app)

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
        print client_file_path
        record = djv.recognize(FileRecognizer, client_file_path)

        pprint(record)
        
        os.unlink(client_file_path)
        # client_id = str(clientrecording).split("_")

        # if record is None:
        #     timestamp = str(client_id[2]).split(".")
        #     result.append({'none':client_file_path, 'client_id': client_id[1],
        #                    'timestamp': timestamp[0], 'channel_name':'Muted', 'confidence':'Muted'})
        # else:
        #     result.append(record)
        
        # mongo.records.insert_many(result)

        return "done"

TODOS = {
    'todo1': {'task': 'build an API'},
    'todo2': {'task': '?????'},
    'todo3': {'task': 'profit!'},
}


def abort_if_todo_doesnt_exist(todo_id):
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exist".format(todo_id))

parser = reqparse.RequestParser()
parser.add_argument('task')


# Todo
# shows a single todo item and lets you delete a todo item
class Todo(Resource):
    def get(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        
        add_together.delay(23, 42)
        
        return TODOS[todo_id]

    def delete(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        del TODOS[todo_id]
        return '', 204

    def put(self, todo_id):
        args = parser.parse_args()
        task = {'task': args['task']}
        TODOS[todo_id] = task
        return task, 201


class TodoList(Resource):
    def get(self):
        return TODOS

    def post(self):
        args = parser.parse_args()
        todo_id = int(max(TODOS.keys()).lstrip('todo')) + 1
        todo_id = 'todo%i' % todo_id
        TODOS[todo_id] = {'task': args['task']}
        return TODOS[todo_id], 201

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
            # match.apply_async((clientrecording.filename,), countdown=30)
            match.delay(clientrecording.filename)
        
        return 200

##
## Actually setup the Api resource routing here
##
api.add_resource(TodoList, '/todos')
api.add_resource(Todo, '/todos/<todo_id>')
api.add_resource(FingerprintRequest, '/fingerprint/')
api.add_resource(MatchRequest, '/matching/match/')
# api.add_resource(RegisterRequest, '/register/')
# api.add_resource(ClientUpdateRequest, '/update/')

if __name__ == '__main__':
    app.run(debug=True)
    