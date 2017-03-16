from __future__ import unicode_literals

from django.db import models

# Create your models here.
class Client(models.Model):
    longitude = models.FloatField(null=True)
    lattitude = models.FloatField(null=True)