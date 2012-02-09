import python_remote
import logging 
port = 8889

logging.basicConfig(level = logging.INFO)
server = python_remote.PythonServer( port )
print "Running server at the port %d"%port
server.start()
print "Server closed"
