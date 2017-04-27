from django.views.decorators.csrf import csrf_exempt
import XmsMatcher
from django.contrib.auth.models import User, Group
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import permission_classes
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Client

# matching imports
from django.http import JsonResponse
import warnings
import json
import os
from XmsMatcher import tasks 
from pymongo import MongoClient

warnings.filterwarnings("ignore")


# Create your views here.
@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def matchclientaudio(request):
    if request.method == 'POST':
        if not os.path.exists('XmsMatcher/clientrecord/'):
            os.mkdir('XmsMatcher/clientrecord/')

        module_dir = os.path.dirname(XmsMatcher.__file__)

        for clientrecording in request.FILES.getlist('client_record'):
            if str(clientrecording).endswith('.mp3'):
                with open(module_dir +'/clientrecord/' +
                          str(clientrecording), 'wb+') as destination:
                    for chunk in clientrecording.chunks():
                        destination.write(chunk)
                        tasks.match.delay(str(clientrecording))

        return Response({'matching':'done'})
    else:
        return Response({'error':'get request was sent instead of post'})

@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def registerclient(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        client = MongoClient('localhost', 27017)
        db = client['database']
        client_name = 'Unknown'
        if 'name' in data and not data['name']:
            client_name = data['name']

        client_inserted = getNextSequence(db.counters, "client_id")
        if not data['long'] or not data['lat']:
            db.clients.insert({'_id': client_inserted, 'name': client_name})

            return Response({'registered': client_inserted, 'location' : False})
        else:
            db.clients.insert({'_id': client_inserted, 'name': client_name,
                               'lon': data['long'], 'lat': data['lat']})
            return Response({'registered': client_inserted, 'location': True,
                             'long': data['long'], 'lat': data['lat']})
    else:
        return Response({'error':'cannot register'})

def getnextsequence(collection, name):
    return collection.find_and_modify(query={'_id': name},
                                      update={'$inc': {'seq': 1}}, new=True).get('seq')
