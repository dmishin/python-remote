import python_remote
import socket
port = 8889
host = socket.gethostname()

print "Connecting to the remote python"
remote = python_remote.FarSide( host, port )
print "Connection done"


remote.stop_server()
remote.close()


