import socket
import pickle
import threading
import weakref
import logging

class UnknownObjectError( Exception ):
    """Error, raised when requested object is not known at the far side"""
    pass

class ClientThread( threading.Thread ):
    def __init__( self, python_server, socket, address ):
        threading.Thread.__init__( self )
        self.socket = socket
        self.python_server = python_server
        self.address = address
        self.handlers = {
            MSG_GET_GLOBALS: python_server.on_get_globals,
            MSG_GET_ATTRIBUTE: python_server.on_get_obj_attr,
            MSG_CALL: python_server.on_call,
            MSG_SET_ATTRIBUTE: python_server.on_set_attr,
            MSG_IMPORT_MODULE: python_server.on_import_module,
            MSG_RELEASE_OBJECT: python_server.on_release_object,
            MSG_GET_ATTR_LIST: python_server.on_get_attr_list }

    def run( self ):
        fl = self.socket.makefile()
        try:
            while True:
                msg = pickle.load( fl )
                #print "##RCV:", msg
                if msg == None:
                    print "Close request"
                    break

                if msg[0] == MSG_STOP_SERVER:
                    pickle.dump( None, fl )
                    fl.close()
                    self.socket.close()
                    self.python_server.stop_requested = True
                    break

                assert (isinstance( msg, tuple ) )
                try:
                    resp = self.handlers[ msg[0] ]( msg )
                except KeyError, key:
                    resp = None
                    print "Unknownw message: %s"%key
                #print "RSP:", resp, "\n"
                pickle.dump( resp, fl )
                fl.flush()
        except Exception, err:
            print "Exception occured while communcating with server:%s"%err

        try:
            fl.close()
            self.socket.close()
        except Exception,err:
            print "Error closing file:%s"%err

        self.socket = None
        self.python_server = None

class RemoteObjectWrapper:
    """Wrapper, used to transfer information about the remote objects via connection"""
    def __init__( self, remote_id ):
        self.remote_id = remote_id
    def __str__( self ):
        return "REMOTE(%d)"%self.remote_id
    def __repr__( self ):
        return "REMOTE(%s)"%self.remote_id

class PythonServer:
    def __init__( self, port, are_lists_local=False ):
        """Create python server on teh specified port
        are_lists_local: When True, lists will be transferred to the client. Othervise, they will be externalized. True is safe only if lists are never modified on the server side.
        """
        self.port = port
        self.objects = dict() #Map id->remoted object
        self.will_wrap_lists = are_lists_local
        self.stop_requested = False

    def start( self ):
        #create an INET, STREAMing socket
        serversocket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self.serversocket = serversocket
        #bind the socket to a public host,
        # and a well-known port
        serversocket.bind( (socket.gethostname(), 
                            self.port) )
        #become a server socket
        serversocket.listen(5)

        while not self.stop_requested:
            #accept connections from outside
            (clientsocket, address) = serversocket.accept()
            print "Accepted connection from:", address
            #now do something with the clientsocket
            #in this case, we'll pretend this is a threaded server
            ct = ClientThread( self, clientsocket, address )
            ct.start()

    def register_object( self, obj ):
        obj_id = id( obj )
        self.objects[ obj_id ] = obj
        return obj_id

    def wrap_returned( self, value ):
        """Called by the server, to prepare returned value for transfer"""
        if value == None or \
                isinstance( value, (int, bool, str, long, float, unicode) ):
            return value
        elif isinstance( value, tuple ):
            return tuple(map( self.wrap_returned, value) )
        elif self.will_wrap_lists and isinstance( value, list ):
            #It is generally unsafe, because far-side modifications of the list 
            #will not be visible on the near-side. However, it can speed-up many applications significantly
            return map( self.wrap_returned, value )
        else:
            #impossible to transfer: transfer as external object
            return RemoteObjectWrapper( self.register_object( value ) )

    def unwrap_argument( self, value ):
        """Wrap values before calling remote method
        Called by the server, to unwrap arguments, passed from the client
        """
        #EMpty tuple is a very common case: check it first to improve performance
        if value == () or value == None \
                or isinstance( value, (int, bool, str, long, float, unicode) ):
            return value 
        if isinstance( value, tuple ):
            return tuple( map( self.unwrap_argument, value ) )

        if isinstance( value, RemoteObjectWrapper ):
            try:
                return self.objects[ value.remote_id ]
            except KeyError,err:
                print "Can't unwrap argument: object %s not registered"%err
                return None

        #Unsafe conversions
        #print "Warning: Argument can not be converted safely"
        if isinstance( value, list ):
            return map( self.unwrap_argument, value )
        if isinstance( value, set ):
            return set( map( self.unwrap_argument, value ) )
        if isinstance( value, dict ):
            return dict( map( self.unwrap_argument, value.items() ) )
        #TODO: process object attributes too?
        return value


    def on_get_globals( self, msg ):
        obj_id = self.register_object( globals() )
        return obj_id

    def on_get_obj_attr( self, msg ):
        msg_id, obj_id, attr_name = msg
        try:
            obj = self.objects[ obj_id ]
            try:
                attr = getattr( obj, attr_name )
                return (RESP_SUCCESS, self.wrap_returned( attr ) )
            except AttributeError:
                return (RESP_NO_SUCH_ATTR,)
        except KeyError, key: #Object not found
            return (RESP_NOT_REGISTERED, obj_id)

    def on_call( self, msg ):
        msg_id, obj_id, args = msg
        #print "###CALL:",obj_id, args
        args = self.unwrap_argument( args )
        try:
            obj = self.objects[ obj_id ]
            try:
                res = self.wrap_returned( obj( *args ) )
                return (RESP_SUCCESS, res)
            except AttributeError, err:
                return (RESP_NO_SUCH_ATTR, "__call__" )
            except Exception, err:
                #Remote exception - its OK
                return (RESP_EXCEPT, err)
        except KeyError, err:
            print "Error! No object %s"%obj_id
            return (RESP_NOT_REGISTERED, obj_id)

    def on_set_attr( self, msg ):
        msg_id, obj_id, attr_name, attr_val = msg
        attr_val = self.unwrap_argument( attr_val )
        try:
            obj = self.objects[ obj_id ]
            try:
                setattr( obj, attr_name, attr_val )
                return (RESP_SUCCESS, )
            except Exception, err:
                print "Faield to set attribute %s to %s"%(attr_name, attr_val )
                return (RESP_EXCEPT, err)
        except KeyError:
            return (RESP_NOT_REGISTERED, obj_id)

    def on_import_module( self, msg ):
        #MSG_IMPORT_MODULE
        msg_id, mod_name = msg
        try:
            module = __import__( mod_name )
            return (RESP_SUCCESS, self.register_object( module ) )
        except Exception, err:
            return (RESP_EXCEPT, err )

    def on_release_object( self, msg ):
        #MSG_RELEASE_OBJECT = 5
        #>(msg, obj_id )
        #<(resp-true)
        #<(resp-false)
        msg_id, obj_id = msg
        try:
            del self.objects[ obj_id ]
            #print "Released object %d"%(obj_id)
            return (RESP_SUCCESS, )
        except KeyError:
            return (RESP_NOT_REGISTERED, obj_id)

    def on_get_attr_list( self, msg ):
        #MSG_GET_ATTR_LIST
        msg_id, obj_id = msg
        try:
            obj = self.objects[ obj_id ]
            return (RESP_SUCCESS, dir( obj ) )
        except KeyError:
            return (RESP_NOT_REGISTERED, obj_id)


class RemoteObject:
    def __init__(self, far_side, remote_id ):
        """Wrapper, representing remote object"""
        self.__dict__[ "far_side" ] = far_side
        self.__dict__[ "remote_id"] = remote_id

    def __getattr__(self, name ):
        attr = self.far_side.get_attribute( self, name )
        #Caching of the special attributes to increase performance
        if name.startswith("__") and name.endswith("__"):
            self.__dict__[ name ] = attr
        return attr

    def __setattr__(self, name, value ):
        return self.far_side.set_attribute( self, name, value )

    def __del__(self):
        try:
            self.far_side.release_object( self )
        except Exception, err:
            print "Object %d can't be released: %s"%(self.remote_id, err)

    def __call__(self, *args):
        """For functions, performs call"""
        return self.far_side.call_object( self, args )

class FarSide:
    def __init__(self, host, port ):
        self.host = host
        self.port = port
        self.objects = weakref.WeakValueDictionary() #Maps remoteID->local wrapper.
        self.file = None
        self.socket = None
        
    def connect( self ):
        #create an INET, STREAMing socket
        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect( (self.host, self.port) )
        self.file = self.socket.makefile()
        
    def close( self ):
        if self.file:
            pickle.dump( None, self.file ) #Say bye to the server
            self.file.close()
            self.file = None
        if self.socket:
            self.socket.close()
            self.socket = None

    def stop_server( self ):
        resp = self._message( (MSG_STOP_SERVER, ) )
        self.close()
        
    def __del__(self):
        if self.file:
            try:
                self.close()
            except Exception, err:
                print "Warning: Error closing conenction ignored: %s"%err
    
    def _message( self, message ):
        """Send a message and read responce"""
        pickle.dump( message, self.file )
        self.file.flush()
        return pickle.load( self.file )

    def globals( self ):
        """Returns wrapped globals array"""
        globals_id = self._message( (MSG_GET_GLOBALS,) )
        return self.get_wrapper( globals_id )

    def unwrap_returned( self, value ):
        """Called by the client to unwrap value, returned from the server"""
        if value == None \
                or isinstance( value, (int, bool, str, long, float, unicode) ):
            return value 
        if isinstance( value, tuple ):
            return tuple( map( self.unwrap_returned, value ) )
        if isinstance( value, list ):
            #Unsafe, but optionally can be enabled at server
            return map( self.unwrap_returned, value )
        if isinstance( value, RemoteObjectWrapper ):
            return self.get_wrapper( value.remote_id )
        raise ValueError, "Returned value can not be unwrapped:", value

    def wrap_argument( self, value ):
        """Wrap values before calling remote method
        Called by the client, to prepare method arguments before call
        """
        #EMpty tuple is a very common case: check it first to improve performance
        if value == () or value == None \
                or isinstance( value, (int, bool, str, long, float, unicode) ):
            return value 
        if isinstance( value, tuple ):
            return tuple( map( self.wrap_argument, value ) )
        if isinstance( value, RemoteObject ):
            return RemoteObjectWrapper( value.remote_id )
        #Unsafe conversions
        #print "Warning: Argument can not be converted safely"
        if isinstance( value, list ):
            return map( self.wrap_argument, value )
        if isinstance( value, set ):
            return set( map( self.wrap_argument, value ) )
        if isinstance( value, dict ):
            return dict( map( self.wrap_argument, value.items() ) )
        #TODO: process object attributes too?
        return value


    def release_object( self, obj_wrapper ):
        """Releases a remote object"""
        #MSG_RELEASE_OBJECT = 5
        #>(msg, obj_id )
        assert( isinstance( obj_wrapper, RemoteObject ) )
        resp = self._message( (MSG_RELEASE_OBJECT, obj_wrapper.remote_id ) )
        if resp[0] == RESP_NOT_REGISTERED:
            print "Warning! Object with remote id %d was not registered at the server"%(resp[1])
            

    def get_attribute( self, object_wrapper, attr_name ):
        """Returns wrapped attrobute of the object
        """
        assert( isinstance( object_wrapper, RemoteObject ) )
        resp = self._message( (MSG_GET_ATTRIBUTE, 
                               object_wrapper.remote_id, 
                               attr_name ) )
        if resp[0] == RESP_SUCCESS:
            return self.unwrap_returned( resp[1] ) #resp is a remote ID

        if resp[0] == RESP_NO_SUCH_ATTR: #Remote object do not have such ID
            raise AttributeError, attr_name
        if resp[0] == RESP_NOT_REGISTERED: #Remote object do not have such ID
            raise UnknownObjectError, resp[1]

    def set_attribute( self, remote_obj, attr_name, attr_value ):
        assert( isinstance( remote_obj, RemoteObject ) )
        resp = self._message( (MSG_SET_ATTRIBUTE,
                               remote_obj.remote_id,
                               attr_name,
                               self.wrap_argument( attr_value ) ) ) #TODO sanitize value
        if resp[0] == RESP_EXCEPT:
            raise resp[1]
        if resp[0] == RESP_NOT_REGISTERED:
            raise UnknownObjectError, resp[1]
        
    def get_wrapper( self, remote_id ):
        """Returns wrapper for the given remote ID
        """
        try:
            return self.objects[ remote_id ]
        except KeyError:
            wrapper = RemoteObject( self, remote_id )
            self.objects[ remote_id ] = wrapper
            return wrapper

    def import_module( self, mod_name ):
        assert( isinstance( mod_name, str ) )
        resp = self._message( (MSG_IMPORT_MODULE, mod_name) )
        if resp[0] == RESP_SUCCESS:
            return self.get_wrapper( resp[1] )
        elif resp[0] == RESP_EXCEPT:
            raise resp[1]

    def dir( self, object_wrapper ):
        assert( isinstance( object_wrapper, RemoteObject ) )
        resp = self._message( (MSG_GET_ATTR_LIST, object_wrapper.remote_id ) )
        if resp[0] == RESP_SUCCESS:
            return resp[1]
        elif resp[1] == RESP_NOT_REGISTERED:
            raise UnknownObjectError, resp[1]

    def call_object( self, remote_obj, args ):
        """Calls remote obvject"""
        assert( isinstance( remote_obj, RemoteObject) )
        assert( remote_obj.remote_id in self.objects )
        args = self.wrap_argument( args )
        resp = self._message( (MSG_CALL,
                               remote_obj.remote_id,
                               args ) )
        if resp[0] == RESP_SUCCESS:
            return self.unwrap_returned( resp[1] ) #todo: sanitize
        elif resp[0] == RESP_NOT_REGISTERED:
            raise UnknownObjectError, resp[1]
        elif resp[0] == RESP_EXCEPT:
            raise resp[1]
        elif resp[0] == RESP_NO_SUCH_ATTR:
            raise AttributeError, "__call__"

def msg_name( msg_id ):
    try:
        return msg_name.id2name[ msg_id ]
    except AttributeError:
        mapping = dict()
        globs = globals()
        for key in globs.keys():
            if key.startswith( "MSG_" ):
                mapping[ globs[ key ] ] = key[4:]
        msg_name.id2name = mapping
        return msg_name( msg_id )
    except KeyError:
        return "UNKNOWN%d"%msg_id

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
#<(True, ans)
#<(False, None) - no __call__ support
#<(False, err) - exception occured

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

MSG_STOP_SERVER = 7

RESP_SUCCESS = 0
RESP_EXCEPT = 1 #partial_success
RESP_NOT_REGISTERED = 2 #object not registered
RESP_NO_SUCH_ATTR = 3#Attribute requested not found
