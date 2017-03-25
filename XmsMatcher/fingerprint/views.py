from django.views.decorators.csrf import csrf_exempt
import XmsMatcher
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
from XmsMatcher import tasks 
warnings.filterwarnings("ignore")


# Create your views here.
@api_view(['GET', 'POST'])
@permission_classes((IsAuthenticated, ))
def FingerprintFolder(request):
    if request.method == 'POST':
		module_dir = os.path.dirname(XmsMatcher.__file__)  

		# for mp3file in os.listdir(module_dir + '/mp3/'):
		# 	if str(mp3file).endswith('.mp3'):
		recordarray = []
		for clientrecording in request.FILES.getlist('serverrecord'):
		    if str(clientrecording).endswith('.mp3'):
		    	recordarray.append(str(clientrecording))
		        with open(module_dir +'/mp3/' + str(clientrecording), 'wb+') as destination:
		            for chunk in clientrecording.chunks():
		                destination.write(chunk)
		tasks.fingerprint.delay(recordarray)        
	
		return Response({'fingerprint':'done'})

    else:
        return Response({'error':'get request was sent instead of post'})
