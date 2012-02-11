import socket
import threading
import weakref
import logging
import sys
from core import *

import cPickle as pickle #Use faster realization of the pickle algorithm.
#import simple_pickle as pickle
_protocol = pickle.HIGHEST_PROTOCOL #Use the highest available pickle protocol.
#import pickle #These are failsafe options
#_protocol = 0

dump = pickle.dump
load = pickle.load

SIMPLE_TYPES = (int, bool, str, long, float, unicode)


class PythonServer:
    """Server"""
    def __init__( self, port, are_lists_local=False, multithread = False ):
        """Create python server on the specified port
        are_lists_local: When True, lists will be transferred to the client. Otherwise, they will be 'externalized'. 
        True is safe only if lists are not owned by the server-side.
        """
        self.port = port
        self.objects = dict() #Map id->remoted object
        self.will_wrap_lists = are_lists_local
        self.stop_requested = False
        self.multithread = multithread
        self.logger = logging.getLogger( "py-remote.server" )

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
            self.logger.info( "Accepted connection from: %s"%str( address ) )
            #now do something with the clientsocket
            #in this case, we'll pretend this is a threaded server
            ct = ServerThread( self, clientsocket, address, self.logger ) #TODO: use child logger. (not available in python 25)
            if self.multithread:
                st.start()
            else:
                ct.run()
                ct = None

    def register_object( self, obj ):
        """Stores reference to the object in the internal map and returns object ID, that would be used as remote ID by the client"""
        obj_id = id( obj )
        self.objects[ obj_id ] = obj
        return obj_id

    def wrap_returned( self, value ):
        """Called by the server, to prepare returned value for transfer"""
        def do_wrap( value ):
            if value == None or \
                    isinstance( value, SIMPLE_TYPES ):
                return value
            elif isinstance( value, tuple ):
                return tuple(map( do_wrap, value) )
            elif self.will_wrap_lists and isinstance( value, list ):
                #It is generally unsafe, because far-side modifications of the list 
                #will not be visible on the near-side. However, it can speed-up many applications significantly
                return map( do_wrap, value )
            else:
                #impossible to transfer: transfer as external object
                return RemoteObjectWrapper( self.register_object( value ) )
        return do_wrap(value)

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
                self.logger.error( "Can't unwrap argument: object %s not registered"%err )
                raise UnknownObjectError, err

        #Unsafe conversions
        #self.logger.warning( "Warning: Argument can not be converted safely" )
        if isinstance( value, list ):
            return map( self.unwrap_argument, value )
        if isinstance( value, set ):
            return set( map( self.unwrap_argument, value ) )
        if isinstance( value, dict ):
            return dict( map( self.unwrap_argument, value.items() ) )
        #TODO: process object attributes too?
        return value

    def on_get_globals( self, msg ):
        """Handler for the get_globals message. Returns wrapper for the globals map. Not really usable."""
        obj_id = self.register_object( globals() )
        return obj_id

    def on_get_obj_attr( self, msg ):
        msg_id, obj_id, attr_name = msg
        try:
            obj = self.objects[ obj_id ]
            try:
                attr = getattr( obj, attr_name )
                #print "#succ get attr:", attr_name, attr
                return (RESP_SUCCESS, self.wrap_returned( attr ) )
            except AttributeError:
                return (RESP_NO_SUCH_ATTR,)
        except KeyError, key: #Object not found
            return (RESP_NOT_REGISTERED, obj_id)

    def on_call( self, msg ):
        """Called object as function"""
        msg_id, obj_id, args = msg
        args = self.unwrap_argument( args )
        try:
            obj = self.objects[ obj_id ]
            try:
                res = self.wrap_returned( obj( *args ) )
                return (RESP_SUCCESS, res)
            except AttributeError, err:
                return (RESP_NO_SUCH_ATTR, "__call__" )
            except Exception, err:
                #Remote exception - it's OK, just tell about th exception to the client
                return (RESP_EXCEPT, err)
        except KeyError, err:
            self.logger.error( "Error! No object %s"%obj_id )
            return (RESP_NOT_REGISTERED, obj_id)

    def on_set_attr( self, msg ):
        """Attempt to set attribute"""
        msg_id, obj_id, attr_name, attr_val = msg
        attr_val = self.unwrap_argument( attr_val )
        try:
            obj = self.objects[ obj_id ]
            try:
                setattr( obj, attr_name, attr_val )
                return (RESP_SUCCESS, )
            except Exception, err:
                self.logger.error( "Faield to set attribute %s to %s"%(attr_name, attr_val ) )
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
        """Client says that the object is no more needed."""
        #MSG_RELEASE_OBJECT = 5
        #>(msg, obj_id )
        #<(resp-true)
        #<(resp-false)
        msg_id, obj_id = msg
        try:
            del self.objects[ obj_id ]
            #self.logger.debug( "Released object %d"%(obj_id)
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

class ServerThread( threading.Thread ):
    def __init__( self, python_server, socket, address, logger ):
        threading.Thread.__init__( self )
        self.socket = socket
        self.python_server = python_server
        self.address = address
        self.logger = logger
        self.handlers = {
            MSG_GET_GLOBALS: python_server.on_get_globals,
            MSG_GET_ATTRIBUTE: python_server.on_get_obj_attr,
            MSG_CALL: python_server.on_call,
            MSG_SET_ATTRIBUTE: python_server.on_set_attr,
            MSG_IMPORT_MODULE: python_server.on_import_module,
            MSG_RELEASE_OBJECT: python_server.on_release_object,
            MSG_GET_ATTR_LIST: python_server.on_get_attr_list }

    def run( self ):
        """Main loop: receive messages and respond to them"""
        fl = self.socket.makefile("rwb")
        logger = self.logger
        def respond( message ):
            dump( message, fl, _protocol )
            fl.flush()

        try:
            while True:
                msg = load( fl )
                assert (isinstance( msg, tuple ) )
                msg_code = msg[0]

                if msg_code == MSG_BYE:
                    logger.info( "Close request received" )
                    break

                if msg_code == MSG_STOP_SERVER:
                    respond( (RESP_SUCCESS, ) )
                    fl.close()
                    self.socket.close()
                    self.python_server.stop_requested = True
                    break

                try:
                    respond( self.handlers[ msg[0] ]( msg ) )
                except KeyError, key:
                    respond( (RESP_EXCEPT, ValueError( "Unknown message:%s"%key ) ) )
                    logger.error( "Unknown message: %s"%key )
        except Exception, err:
            logger.error( "Exception (%s) occurred while communicating with client: %s"%(type(err), err) )

        try:
            fl.close()
            self.socket.close()
        except Exception,err:
            logger.error( "Error closing socket file:%s"%err )

        self.socket = None
        self.python_server = None

