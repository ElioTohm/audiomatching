from gevent.wsgi import WSGIServer
from main import app

http_server = WSGIServer(('', 45454), app)
http_server.serve_forever()