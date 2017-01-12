from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^/match/$', views.match, name='match'),
    url(r'^/fingerprint/$', views.fingerprint, name='match'),
]