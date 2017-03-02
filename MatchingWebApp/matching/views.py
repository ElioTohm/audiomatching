from django.views.decorators.csrf import csrf_exempt

from django.contrib.auth.models import User, Group
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import permission_classes
from rest_framework.decorators import api_view
from rest_framework.response import Response

# matching imports
from django.http import JsonResponse
import warnings
import json
import os
warnings.filterwarnings("ignore")

from xmsmatch import Matcher
from xmsmatch.recognize import FileRecognizer
import datetime
import pprint
from pymongo import MongoClient




@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def match(request):
    if request.method == 'POST':
        if not os.path.exists('matching/clientrecord/'):
            os.mkdir('matching/clientrecord/')

        module_dir = os.path.dirname(__file__)  # get current directory
        file_path = os.path.join(module_dir, 'xmsmatch.cnf.SAMPLE')
        with open(file_path) as f:
            config = json.load(f)
            if __name__ == 'matching.views':

                # create a Matcher instance
                djv = Matcher(config)
                result = list()
                
                for clientrecording in request.FILES.getlist('client_record'):
                    if str(clientrecording).endswith('.mp3'):
                        with open('matching/clientrecord/' + str(clientrecording), 'wb+') as destination:
                            for chunk in clientrecording.chunks():
                                destination.write(chunk)  
                        
                        client_file_path = os.path.join(module_dir, 'clientrecord/' + str(clientrecording))
                        record = djv.recognize(FileRecognizer, client_file_path)

                        if record is None:
                            result.append({'none':client_file_path})
                        else:
                            result.append(record)

                        # remove file matched 
                        print( result )
                        os.unlink(client_file_path)             
                    
            client = MongoClient('localhost', 27017)
            db = client['database']
            collection = db.records
            
            collection.insert_many(result)

        return Response({"matched": "done"})
    else:
        return Response({"error": "get request was sent instead of post"})


@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def fingerprint(request):
    if request.method == 'POST':

        # get current directory
        module_dir = os.path.dirname(__file__)  
        file_path = os.path.join(module_dir, 'xmsmatch.cnf.SAMPLE')
        with open(file_path) as f:
            # load config from a JSON file (or anything outputting a python dictionary)
            config = json.load(f)
            
            # create a Matcher instance
            djv = Matcher(config)

            # Fingerprint all the mp3's in the directory we give it
            djv.fingerprint_directory(module_dir + "/mp3", [".mp3"])
            
        return Response({'fingerprint':'done'})
    else:
        return Response({'error':'get request was sent instead of post'})