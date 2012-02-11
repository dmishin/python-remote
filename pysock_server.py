from pyremote.server import PythonServer
import logging 
port = 8888

logging.basicConfig(level = logging.INFO)
server = PythonServer( port )
print "Running server at the port %d"%port
server.start()
print "Server closed"
