import python_remote

port = 8888
host = "kenny"

print "Connecting to the remote python"
remote = python_remote.FarSide( host, port )
remote.connect()
print "Connection done"

rglobal = remote.globals()
rmath = remote.import_module( "math" )

try:
    print rmath.sin( 1.0 )
    print float(rmath.pi)
    rbuiltins = rglobal.__builtins__
    print rbuiltins.map( rmath.sin, [0,1,2,3] )

    rmath = None
    rglobal = None

except KeyError, err:
    print "No key:", err
    
finally:
    remote.close()
