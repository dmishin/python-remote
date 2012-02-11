import cPickle as pickle #Use faster realization of the pickle algorithm.
#Use the highest available pickle protocol.
protocol = pickle.HIGHEST_PROTOCOL 
dump = pickle.dump
load = pickle.load

SIMPLE_TYPES = (int, bool, str, long, float, unicode)

class UnknownObjectError( Exception ):
    """Error, raised when client requested an object, that is not known at the server side"""
    pass

class ProtocolException( Exception ):
    """Raised, when unexpected or incorrect message received by client or by server.
    Usually caused either by the errors in the code, or by network errors"""
    pass

class RemoteObjectWrapper:
    """Wrapper, used to transfer information about the remote objects via connection. Simply wraps the remote ID"""
    def __init__( self, remote_id ):
        self.remote_id = remote_id
    def __str__( self ):
        return "REMOTE(%d)"%self.remote_id
    def __repr__( self ):
        return "REMOTE(%s)"%self.remote_id

# Protocol constants: message and responce formats (both are tuples)
################################################################################

MSG_GET_ATTRIBUTE = 0
#>(msg, remote id, attr_name)
#<(resp-ok, wrapped-value)
#<(resp-not-reg, id)
#<(resp-not-found-attr)

MSG_GET_GLOBALS = 1
#>(msg )
#<(remote ID for globals array)

MSG_CALL = 2
#>(msg, obj_id, args)
#<(status-success, ans)
#<(statue-nocall, None) - no __call__ support
#<(status-except, err) - exception occurred

MSG_SET_ATTRIBUTE = 3
#>(msg, obj_id, attr_name, attr_value)
#<(resp-success) (resp-notreg, id) (resp-exc, exc)

MSG_IMPORT_MODULE = 4
#>(msg, mod_name )
#<(resp, module_object_id )
#<(resp-false, )
MSG_RELEASE_OBJECT = 5
#>(msg, obj_id )
#<(resp-true)
#<(resp-false)
MSG_GET_ATTR_LIST = 6
#>(msg, obj-id)
#<(resp-true, attr-list)
#<(resp-false)

MSG_STOP_SERVER = 7 #Client requested sever close
MSG_BYE = -1 #Said by the client, before quit

#Responce codes
RESP_SUCCESS = 0
RESP_EXCEPT = 1 #partial_success
RESP_NOT_REGISTERED = 2 #object not registered
RESP_NO_SUCH_ATTR = 3#Attribute requested not found
