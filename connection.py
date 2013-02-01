from pycassa import ConnectionPool,ColumnFamily
from pycassa.system_manager import SystemManager,SIMPLE_STRATEGY,_DEFAULT_TIMEOUT

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
        """'table_name'
        self.conn = ConnectionPool(keyspace, server_list, credentials, timeout, use_threadlocal, 
                                pool_size, prefill, **kwargs)
     
    def insert_into_column_family(self, cf_name, key, columns, timestamp=None, ttl=None,
                                                                                        write_consistency_level=None):
        col_fam = ColumnFamily(self.conn, cf_name)
        col_fam.insert(key, columns, timestamp, ttl, write_consistency_level)
    
    insert_into_table = insert_into_column_family
    
    
    def close(self):
        pass
    
    
    

class ComedySysManagerConnection(object):
    """
    This is a also a client connection of Cassandra, difference is it is only for System-Manager level
    """
    def __init__(self, server='localhost:9160', credentials=None, framed_transport=True,
                                                                    timeout=_DEFAULT_TIMEOUT):
        self.conn = SystemManager(server,credentials,framed_transport,timeout)
        
    def create_keyspace(self, name, replication_strategy='SimpleStrategy', strategy_options=None, durable_writes=True, **ks_kwargs):
        """Create a database"""
        self.conn.create_keyspace(name, replication_strategy, strategy_options, durable_writes)
    
    create_database = create_keyspace
    
    def drop_keyspace(self, keyspace):
        """Drop a database"""
        self.conn.drop_keyspace(keyspace)
    
    drop_database = drop_keyspace
    
    
    def create_column_family(self, keyspace, name, column_validation_classes=None, **cf_kwargs):
        """Create a table"""
        self.conn.create_column_family(keyspace, name, column_validation_classes=None, **cf_kwargs) 
    
    create_table = create_column_family
    
    def drop_column_family(self, keyspace, column_family):
        """Drop a table"""
        self.conn.drop_column_family(keyspace, column_family)
        
    drop_table = drop_column_family
    

    def close(self):
        pass
    
    
    
    
class ComedyConnectionUnittest(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_case_step1_clientConnection(self):
        print "Initiating ComedyClientConnection..."
        ComedyClientConnection('Keyspace1')
        print "Yeah, connection established..."
    
    def test_case_step2_sysManagerConnection(self):
        print "Initiating ComedySystemManager..."
        ComedySysManagerConnection()
        print "Yeah, connection established..."
        
    def test_case_step_3_create_drop_database(self):
        conn = ComedySysManagerConnection()
        print "Testing create_keyspace..."
        if "test_db" not in conn.conn.list_keyspaces():
            conn.create_keyspace("test_db", SIMPLE_STRATEGY, {'replication_factor': '1'})
        print "Testing drop_keyspace"
        conn.drop_keyspace("test_db")
        print "Testing create_database..."
        conn.create_database("test_db", SIMPLE_STRATEGY, {'replication_factor': '1'})
        print "Testing drop_database"
        conn.drop_database("test_db")
        
    def test_case_step_4_create_drop_table(self):
        conn = ComedySysManagerConnection()
        print "Creating database..."
        if "test_db" not in conn.conn.list_keyspaces():
            conn.create_database("test_db", SIMPLE_STRATEGY, {'replication_factor': '1'})
        print "Testing create_table"
        conn.create_table("test_db", "test_table")
        print "Testing drop_table"
        conn.drop_table("test_db", "test_table")
        print "Dropping database"
        conn.drop_database("test_db")
        
    def test_case_step_5_get_and_insert_into_table(self):
        sys_manager_conn = ComedySysManagerConnection()
        print "Creating database..."
        if "test_db" not in sys_manager_conn.conn.list_keyspaces():
            sys_manager_conn.create_database("test_db", SIMPLE_STRATEGY, {'replication_factor': '1'})
        client_conn = ComedyClientConnection('test_db')
        print "Creating table"
        sys_manager_conn.create_table("test_db", "test_table")
        print "Testing insert_into_table"
        client_conn.insert_into_table("test_table", 'row_key', {'col_name': 'col_val'})
        print "Dropping database"
        sys_manager_conn.drop_database("test_db")
        
        


if __name__ == "__main__":
    unittest.main()