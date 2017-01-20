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



@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def match(request):
    if request.method == 'POST':
        # load config from a JSON file (or anything outputting a python dictionary)
        module_dir = os.path.dirname(__file__)  # get current directory
        file_path = os.path.join(module_dir, 'xmsmatch.cnf.SAMPLE')
        with open(file_path) as f:
            config = json.load(f)
            if __name__ == 'matching.views':

                # create a Matcher instance
                djv = Matcher(config)
                result = list()
                info = json.loads(request.body)
                for client_record in info['records']:                    
                    client_file_path = os.path.join(module_dir, 'clientrecord/' + client_record)
                    
                    record = djv.recognize(FileRecognizer, client_file_path)
                    if record is None:
                        result.append('none')
                    else:
                        result.append(record)
            return Response(result)
    else:
        return Response({"error": "get request was sent instead of post"})


@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def fingerprint(request):
    if request.method == 'POST':
        
        module_dir = os.path.dirname(__file__)  # get current directory
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