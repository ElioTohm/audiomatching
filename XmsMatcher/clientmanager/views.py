"""
    views for client
"""
import os
import json
from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import permission_classes
from rest_framework.decorators import api_view
from django.http import HttpResponse
from django.template import loader
import paho.mqtt.publish as publish

@permission_classes((IsAuthenticated, ))
@api_view(['POST', 'GET'])
def mqtpublishmessage(request):
    """
    Publish message to clients
    """
    if request.method == 'POST':
        message = "{version:%s, update: %s}" % (1, True)

        publish.single("Client", payload=json.dumps(message), hostname="localhost", port=1883,
                       auth={'username':'pahopmclient', 'password':'xms@pmclient#12345'})

        return HttpResponse('200')
    else:
        template = loader.get_template('clientmanager/clientadmin.html')
        return HttpResponse(template.render())
