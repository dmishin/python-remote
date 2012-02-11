from core import *
################################################################################
#  Client objects
################################################################################
class FarSide:
    """Client"""
    def __init__(self, host, port, cache_all_attributes=False, connect=True ):
        """Create client for access to the server-side objects"""
        self.host = host
        self.port = port
        self.objects = weakref.WeakValueDictionary() #Maps remoteID->local wrapper.
        self.file = None
        self.socket = None
        self.msg_counter = 0
        self.cache_all_attributes = cache_all_attributes
        if connect: self.connect()

    def connect( self ):
        #create an INET, STREAMing socket
        if self.file != None: raise ValueError, "Already connected" 
        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect( (self.host, self.port) )
        self.file = self.socket.makefile("rwb")

    def get_msg_counter( self ):
        """Returns total number of the messages, passed between server and client"""
        return self.msg_counter

    def disconnect_objects( self ):
        """Disconnects all objects, associated with this FarSide"""
        for obj in self.objects.values():
            obj._release_remote_()
        
    def close( self ):
        if self.file:
            self.disconnect_objects()
            dump( (MSG_BYE, ), self.file, _protocol ) #Say bye to the server
            self._disconnect()
        else:
            raise ValueError, "Client already closed connection!"

    def _disconnect(self):
        self.file.close()
        self.file = None
        self.socket.close()
        self.socket = None
        
    def stop_server( self ):
        """Closes connection and requests server to stop"""
        self.disconnect_objects()
        resp = self._message( (MSG_STOP_SERVER, ) )
        self._disconnect()
        
    def __del__(self):
        if self.file: #If not yet disconnected
            try:
                self.disconnect_objects() #Mark all objects, associated with this connection as invalid.
                self.close()
            except Exception, err:
                print "Warning: Exception occured when closing connection was ignored: %s"%err
    
    def _message( self, message ):
        """Send a message and read response"""
        self.msg_counter += 1
        dump( message, self.file, _protocol )
        self.file.flush()
        resp = load( self.file )
#        print "#>>", message
#        print "#<<", resp
        return resp

    def globals( self ):
        """Returns wrapped globals array"""
        globals_id = self._message( (MSG_GET_GLOBALS,) )
        return self.get_wrapper( globals_id )

    def unwrap_returned( self, value ):
        """Called by the client to unwrap value, returned from the server"""
        if value is None or isinstance( value, SIMPLE_TYPES ):
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
        def do_wrap( value ):
            #Empty tuple is a very common case: check it first to improve performance
            if isinstance( value, ProxyObject ): #ProxyObject check must go first - or else comparisions will cause clinch.
                return RemoteObjectWrapper( value._remote_id_ )
            # empty tuple is very common case, check for it separately
            if ()==value or value is None \
                    or isinstance( value, SIMPLE_TYPES ):
                return value 
            if isinstance( value, tuple ):
                return tuple( map( do_wrap, value ) )
            #Unsafe conversions
            #print "Warning: Argument can not be converted safely"
            if isinstance( value, list ):
                return map( do_wrap, value )
            if isinstance( value, set ):
                return set( map( do_wrap, value ) )
            if isinstance( value, dict ):
                return dict( map( do_wrap, value.items() ) )
            #TODO: process object attributes too?
            return value
        return do_wrap(value)

    def release_object( self, obj_wrapper ):
        """Releases a remote object"""
        #MSG_RELEASE_OBJECT = 5
        #>(msg, obj_id )
        assert( isinstance( obj_wrapper, ProxyObject ) )
        resp = self._message( (MSG_RELEASE_OBJECT, obj_wrapper._remote_id_ ) )
        if resp[0] == RESP_NOT_REGISTERED:
            raise UnknownObjectError, resp[1]

    def get_attribute( self, object_wrapper, attr_name ):
        """Returns wrapped attribute of the object
        """
        assert( isinstance( object_wrapper, ProxyObject ) )
        remote_id = object_wrapper._remote_id_
        if remote_id == None:
            raise AttributeError, attr_name
        resp = self._message( (MSG_GET_ATTRIBUTE, 
                               remote_id, 
                               attr_name ) )
        try:
            resp_code = resp[0]
            if resp_code == RESP_SUCCESS:
                return self.unwrap_returned( resp[1] ) #resp is a remote ID
            if resp_code == RESP_NO_SUCH_ATTR: #Remote object do not have such ID
                raise AttributeError, attr_name
            if resp_code == RESP_NOT_REGISTERED: #Remote object do not have such ID
                raise UnknownObjectError, resp[1]
        except IndexError:
            raise ProtocolException, "Wrong response: %s"%str(resp)

    def set_attribute( self, remote_obj, attr_name, attr_value ):
        assert( isinstance( remote_obj, ProxyObject ) )
        resp = self._message( (MSG_SET_ATTRIBUTE,
                               remote_obj._remote_id_,
                               attr_name,
                               self.wrap_argument( attr_value ) ) ) #TODO sanitize value
        resp_code = resp[0]
        if resp_code == RESP_EXCEPT:
            raise resp[1]
        if resp_code == RESP_NOT_REGISTERED:
            raise UnknownObjectError, resp[1]
        
    def get_wrapper( self, remote_id, remote_name=None ):
        """Returns wrapper for the given remote ID
        """
        try:
            return self.objects[ remote_id ]
        except KeyError:
            wrapper = ProxyObject( self, remote_id, remote_name )
            self.objects[ remote_id ] = wrapper
            return wrapper

    def import_module( self, mod_name ):
        """Imports module at the remote side, and returns a proxy object for that module.
        """
        assert( isinstance( mod_name, str ) )
        resp = self._message( (MSG_IMPORT_MODULE, mod_name) )
        resp_code = resp[0]
        if resp_code == RESP_SUCCESS:
            module = self.get_wrapper( resp[1], mod_name )
            return module
        elif resp_code == RESP_EXCEPT:
            raise resp[1]
        else:
            raise ProtocolException, "Unexpected response:%s"%(str(resp))

    def dir( self, object_wrapper ):
        """Calls  dir() at the remote side and returns resulted list"""
        assert( isinstance( object_wrapper, ProxyObject ) )
        resp = self._message( (MSG_GET_ATTR_LIST, object_wrapper._remote_id_ ) )
        resp_code = resp[0]
        if resp_code == RESP_SUCCESS:
            return resp[1]
        elif resp_code == RESP_NOT_REGISTERED:
            raise UnknownObjectError, resp[1]
        else:
            raise ProtocolException, "Unexpected response:%s"%(str(resp))

    def call_object( self, remote_obj, args ):
        """Calls remote object as function"""
        assert( isinstance( remote_obj, ProxyObject) )
        assert( remote_obj._remote_id_ in self.objects )
        args = self.wrap_argument( args )
        resp = self._message( (MSG_CALL,
                               remote_obj._remote_id_,
                               args ) )
        try:
            resp_code = resp[0]
            if resp_code == RESP_SUCCESS:
                return self.unwrap_returned( resp[1] ) #todo: sanitize
            elif resp_code == RESP_NOT_REGISTERED:
                raise UnknownObjectError, resp[1]
            elif resp_code == RESP_EXCEPT:
                raise resp[1]
            elif resp_code == RESP_NO_SUCH_ATTR:
                raise AttributeError, "__call__"
            else:
                raise ValueError, "Unexpected response"
        except IndexError:
            raise ProtocolException, "Wrong answer: %s"%(str(resp))

class ProxyObject:
    def __init__(self, far_side, remote_id, name=None ):
        """Wrapper, representing remote object"""
        attrs = self.__dict__
        attrs[ "far_side" ] = far_side
        attrs[ "_remote_id_"] = remote_id
        attrs[ "_remote_name_" ] = name or "<%s>"%(remote_id)

    def __getattr__(self, name ):
        #print "#Get:", self._remote_name_, name
        attr = self.far_side.get_attribute( self, name )
        #Caching of the special attributes to increase performance
        #if name.startswith("__") and name.endswith("__"):
        if self.far_side.cache_all_attributes:
            self.__dict__[ name ] = attr
        if isinstance( attr, ProxyObject ):
            attr.__dict__[ "_remote_name_" ] = self._remote_name_ + "." + name
        return attr

    def __setattr__(self, name, value ):
        return self.far_side.set_attribute( self, name, value )

    def __del__(self):
        try:
            if not self._disconnected_(): #If it is not disconnected object
                self.far_side.release_object( self )
        except Exception, err:
            print "Failed to release object %s (%s): %s"%(self._remote_name_, self._remote_id_, err)

    def __call__(self, *args):
        """For functions, performs call"""
        return self.far_side.call_object( self, args )

    def _disconnected_(self):
        return self._remote_id_ is None

    def _release_remote_( self ):
        """Disconnect object from it's remote counterpart. Object becomes unusable after this."""
        #print "invalidate:", self._remote_name_
        self.far_side.release_object( self )
        self.__dict__[ "_remote_id_" ] = None #Mark object as disconnected.

