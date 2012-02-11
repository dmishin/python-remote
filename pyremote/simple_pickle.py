#Serialization of some standard python objects
from struct import pack, unpack
from StringIO import StringIO
from core import RemoteObjectWrapper

TAG_NONE = "n"
TAG_BOOL = "b"
TAG_INT = "i"
TAG_LONG = "l"
TAG_STR = "s"
TAG_UNICODE = "u"
TAG_LIST = "["
TAG_TUPLE = "("
TAG_FLOAT = "f"
TAG_WRAPPER = 'w'

HIGHEST_PROTOCOL = None

def dump( obj, file, protocol=None ):
    _dump(obj, file.write)

def dumps( obj, protocol=None ):
    sio = StringIO()
    dump( obj, sio )
    return sio.getvalue()

def _dump( obj, write):
    if obj is None:
        write(TAG_NONE)
        return
    try:
        dumpers[type(obj)](obj, write)
    except KeyError:
        if isinstance(obj, RemoteObjectWrapper):
            return dump_wrapper(obj, write)
        raise ValueError, "This type can't be searialized: %s"%(obj)
        
def dump_bool( x, write ):
    write(TAG_BOOL)
    write("1" if x else "0")

def dump_int( x, write ):
    write(TAG_INT)
    write(pack("i",x))

def dump_long( x, write ):
    s = str(x)
    write(TAG_LONG)
    write(pack('i', len(s)))
    write(s)

def dump_str( x, write ):
    write(TAG_STR)
    write(pack('i',len(x)))
    write(x)

def dump_unicode( x, write ):
    write(TAG_UNICODE)
    xs = x.encode("utf-8")
    write(pack('i',len(xs)))
    write(xs)
    
def dump_list( x, write ):
    write(TAG_LIST)
    write(pack('i',len(x)))
    for i in x:
        _dump(i, write)

def dump_tuple( x, write ):
    write(TAG_TUPLE)
    write(pack('i',len(x)))
    for i in x:
        _dump(i, write)

def dump_float( x, write ):
    write(TAG_FLOAT)
    write(pack('d',x))

def dump_wrapper( x, write ):
    write(TAG_WRAPPER)
    _dump(x.remote_id, write)

dumpers = {
    bool : dump_bool,
    int: dump_int,
    long : dump_long,
    str : dump_str,
    unicode : dump_unicode,
    list : dump_list,
    tuple : dump_tuple,
    float : dump_float
    }


def loads( s, protocol=None ):
    return load( StringIO(s))

def load( file, protocol=None ):
    return _load(file.read)

def _load( read ):
    tag = read(1)
    if not tag: raise ValueError, "Unexpected EOF"
    return loaders[tag](read)


def read_int(read):
    d = read(4)
    if len(d)!=4: raise ValueError, "Unexpected EOF"
    return unpack('i', d )[0]

load_int = read_int

def load_long( read ):
    return long(load_str(read))

def load_float( read ):
    d = read(8)
    if len(d)!=8: raise ValueError, "Unexpected EOF"
    return unpack( 'd', d )[0]
    
def load_str( read ):
    l = read_int(read)
    s = read(l)
    if len(s) != l: raise ValueError, "Unexpected EOF"
    return s

def load_unicode( read ):
    return load_str(read).decode("utf-8")

def load_list( read ):
    l = read_int(read)
    return [_load(read) for idx in xrange(l)]

def load_tuple( read ):
    return tuple(load_list(read))

def load_bool(read):
    return read(1)=='1'

def load_wrapper(read):
    return RemoteObjectWrapper(_load(read))

loaders={
    TAG_NONE: lambda x: None,
    TAG_INT: load_int,
    TAG_LONG: load_long,
    TAG_FLOAT: load_float,
    TAG_BOOL: load_bool,
    TAG_STR: load_str,
    TAG_UNICODE: load_unicode,
    TAG_LIST: load_list,
    TAG_TUPLE: load_tuple,
    TAG_WRAPPER: load_wrapper
    }
