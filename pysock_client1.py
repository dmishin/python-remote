import python_remote
import socket
import traceback

port = 8888
host = socket.gethostname()
print "Connecting to %s:%d"%(host, port)

print "Connecting to the remote python"
remote = python_remote.FarSide( host, port )
print "Connection done"

rmath = remote.import_module( "math" )
rbi = remote.import_module( "__builtin__" )

try:
    print rmath.sin( 1.0 )
    print float(rmath.pi)
    #Use "map" function from the remote side to map remose "sin" over list of values
    print rbi.map( rmath.sin, [1,2] )

    rmath = None
except KeyError, err:
    print "No key:", err
except AttributeError, err:
    print "No attribute:", err
except Exception, err:
    print "Exception occured", err
    traceback.print_exc()
finally:
    print "Closing server..."
    remote.close()

print "Code terminated"
