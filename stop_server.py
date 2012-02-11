from pyremote.client import FarSide
import socket
port = 8888
host = socket.gethostname()

print "Connecting to the remote python"
remote = FarSide( host, port )
print "Connected"


remote.stop_server()
remote.close()


