# Install python dependencies
sudo apt install -y python-mysqldb python-tk python-scipy libasound-dev portaudio19-dev libportaudio2 libportaudiocpp0

sudo apt install -y  ffmpeg libav-tools

# Installation if django framework was in use
# sudo pip install pymongo celery django datetime djangorestframework markdown django-filter numpy scipy pyaudio paho-mqtt 

# Installation if flask framework was in use 
sudo pip install pymongo celery datetime markdown numpy scipy pyaudio paho-mqtt flask flask-restful Flask-PyMongo gevent

python manage.py migrate

