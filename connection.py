from pycassa import ConnectionPool
from pycassa.system_manager import SystemManager,_DEFAULT_TIMEOUT

import unittest

class ComedyClientConnection(object):
    """
    This is a client connection for Cassandra. This is basically a wrapper for pycassa's
    ConnectionPool
    But:...
    TODO:
       1. different strategy for connecting to server_list
       2. ZeroMQ integration
    """
    def __init__(self, keyspace, server_list=['localhost:9160'], credentials=None, timeout=0.5, \
                    use_threadlocal=True, pool_size=5, prefill=True, **kwargs):
        """
        >>> pool = pycassa.ConnectionPool(keyspace='Keyspace1', \
        ... server_list=['10.0.0.4:9160', '10.0.0.5:9160'], prefill=False)
        >>> cf = pycassa.ColumnFamily(pool, 'Standard1')
        >>> cf.insert('key', {'col': 'val'})
        1287785685530679
        """
        ConnectionPool(keyspace, server_list, credentials, timeout, use_threadlocal, 
                                pool_size, prefill, **kwargs)
    
    def close(self):
        pass

class ComedySysManagerConnection(object):
    def __init__(self, server='localhost:9160', credentials=None, framed_transport=True,
                                                                    timeout=_DEFAULT_TIMEOUT):
        SystemManager(server,credentials,framed_transport,timeout)
    
    def close(self):
        pass
    
class ComedyConnectionUnittest(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_case_step1_clientConnection(self):
        print "Initiating ComedyClientConnection..."
        ComedyClientConnection('Keyspace1')
        print "Yeah, connection established..."
    
    def test_case_step2_clientConnection(self):
        print "Initiating ComedySystemManager..."
        ComedySysManagerConnection()
        print "Yeah, connection established..."


if __name__ == "__main__":
    unittest.main()