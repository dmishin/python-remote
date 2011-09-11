import python_remote

port = 8888
host = "dim-desktop"

print "Connecting to the remote python"
remote = python_remote.FarSide( host, port )
remote.connect()
print "Connection done"

rmath = remote.import_module( "math" )

try:
    print rmath.sin( 1.0 )
    print float(rmath.pi)

    rmath = None

except KeyError, err:
    print "No key:", err
    
finally:
    remote.close()
