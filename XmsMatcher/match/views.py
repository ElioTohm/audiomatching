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
warnings.filterwarnings("ignore")


# Create your views here.
@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def MatchClientAudio(request):
    if request.method == 'POST':
        if not os.path.exists('XmsMatcher/clientrecord/'):
            os.mkdir('XmsMatcher/clientrecord/')

        module_dir = os.path.dirname(XmsMatcher.__file__)
        
        for clientrecording in request.FILES.getlist('client_record'):
            if str(clientrecording).endswith('.mp3'):
                with open(module_dir +'/clientrecord/' + str(clientrecording), 'wb+') as destination:
                    for chunk in clientrecording.chunks():
                        destination.write(chunk)
                        tasks.match.delay(str(clientrecording))
    	
    	return Response({'matching':'done'})
    else:
        return Response({'error':'get request was sent instead of post'})

@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def RegisterClient(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        if not data['long'] or not data['lat'] :
            client = Client()
            client.save()
            return Response({'registered': client.id, 'location' : False})
        else:
            client = Client(longitude = data['long'], lattitude = data['lat'])
            client.save()
            return Response({'registered': client.id, 'location': True, 'long': client.longitude, 'lat': client.lattitude})
    else:
        return Response({'error':'cannot register'})