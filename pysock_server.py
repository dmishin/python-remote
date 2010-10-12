import python_remote
port = 8888

server = python_remote.PythonServer( port )
print "Running server at the port %d"%port
server.start()
print "Server closed"
