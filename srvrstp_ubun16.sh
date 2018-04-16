# Install python dependencies
apt install -y python-pip python-mysqldb python-tk python-scipy libasound-dev portaudio19-dev libportaudio2 libportaudiocpp0

apt install -y  ffmpeg libav-tools

# Installation if django framework was in use
#  pip install pymongo celery django datetime djangorestframework markdown django-filter numpy scipy pyaudio paho-mqtt 

# Installation if flask framework was in use 
pip install pymongo celery datetime markdown numpy scipy pyaudio paho-mqtt flask flask-restful Flask-PyMongo gevent pydub matplotlib requests

echo "deb https://dl.bintray.com/rabbitmq/debian xenial main" |  tee /etc/apt/sources.list.d/bintray.rabbitmq.list
wget -O- https://dl.bintray.com/rabbitmq/Keys/rabbitmq-release-signing-key.asc | apt-key add -
apt-get update && apt-get install rabbitmq-server