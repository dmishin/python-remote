from core import *
################################################################################
# Other funcions and constants
################################################################################
def msg_name( msg_id ):
    """For debug only: returns name of the message"""
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

def print_args( prompt, skip=1 ):
    def decorator( func ):
        def decorated( *args, **kwargs ):
            print prompt, map(saferepr, args[skip:])
            return func( *args, **kwargs )
        return decorated
    return decorator

def print_ret( prompt ):
    """Decorator that prints returned value"""
    def decorator( func ):
        def decorated( *args, **kwargs ):
            rval = func( *args, **kwargs )
            print prompt, rval
            return rval
        return decorated
    return decorator

def saferepr( x ):
    if isinstance(x, ProxyObject):
        return "PROXY(%s)"%x._remote_id_
    if x == None: return x
    if isinstance(x, (bool, int, long, str, unicode)): return repr(x)
    if isinstance(x, tuple):
        return tuple(map(saferepr,x))
    if isinstance(x, list):
        return map(saferepr, x)
    return repr(x)
        
################################################################################
