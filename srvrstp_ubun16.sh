# Install python dependencies
sudo apt install -y python-mysqldb python-tk python-scipy libasound-dev portaudio19-dev libportaudio2 libportaudiocpp0

sudo apt install -y  ffmpeg libav-tools

pip install pymongo celery django datetime djangorestframework markdown django-filter numpy scipy pyaudio paho-mqtt

python manage.py migrate

