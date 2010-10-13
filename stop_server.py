import python_remote

port = 8888
host = "kenny"

print "Connecting to the remote python"
remote = python_remote.FarSide( host, port )
remote.connect()
print "Connection done"


remote.stop_server()
remote.close()

print "bump..."
remote.connect()
remote.close()
