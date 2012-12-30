from pycassa.cassandra.ttypes import (KsDef,)
from .datastructures import (OrderedDict,)
from .util import (CASPATHSEP,
                   CrossModelCache,
                   popmulti,
                  )
from . import connection

from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY

cmcache = CrossModelCache()

possible_validate_args = (
                          ('auto_create_models', False),
                          ('auto_drop_keyspace', False),
                          ('auto_drop_columnfamilies', False),
                         )

class InventoryType(type):
    """This keeps inventory of the models created, and prepares the
       limited amount of metaclass magic that we do. keep this small!"""
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, InventoryType)]
        new_cls = super(InventoryType, cls).__new__(cls, name, bases, attrs)
                
        if '__abstract__' in new_cls.__dict__: 
            return new_cls
        else: # we're not abstract -> we're user defined and stored in the db
            new_cls._init_class(name=name)

        return new_cls
    
class Cluster(object):
    def __init__(self, name):
        self.keyspaces = OrderedDict()
        self.name = name
        self._sm_client = None
        
        cmcache.append('clusters', self)
    
    def setclient(self, servers_list=[connection.DEFAULT_SERVER]):
        for server in servers_list:
            try:
                self._sm_client = SystemManager(server)
                return
            except:
                pass
        if self._sm_client is None:
            raise NoServerAvailable('Cannot create system_manager connections on server list %s' % str(servers_list))
    
    def registerKeyspace(self, name, keyspc):
        self.keyspaces[name] = keyspc
        if self._sm_client is None:
            self.setclient()
        # try to actually register this keyspace to the system via 
        if not name in self._sm_client.list_keyspaces():
            self._sm_client.create_keyspace(name, keyspec.strategy_class)
        
    def __str__(self):
        return self.name
    
class Keyspace(object):  
    def __init__(self, name, cluster):
        self.models = OrderedDict()
        self.name = name
        self.cluster = cluster
        self._client = None
        self._first_iteration_in_this_cycle = False
        
        self.strategy_class = SIMPLE_STRATEGY
        self.replication_factor = 1
        cluster.registerKeyspace(self.name, self)       
        
        cmcache.append('keyspaces', self)

    def connect(self, *args, **kwargs):
        newkwargs = popmulti(kwargs, *possible_validate_args )
        self._client = connection.connect(*args, **kwargs)
        # save server list here for future purposes.
        self._server_list = kwargs.get('servers', [])
        
        for model in self.models.values():
            model._init_stage_two()
        
        if newkwargs['auto_create_models']:
            self.verify_datamodel(**newkwargs)
            
        if not self._client._keyspace_set:
            self._client.set_keyspace(self.name)
    
    def get_system_manager(self, **kwargs):
        retry = len(self._server_list)
        for server in self._server_list:
            try:
                sys_mgr = SystemManager(server, **kwargs)
                break
            except:
                retry -= 1
        if retry > 0:
            return sys_mgr
             

    def getclient(self):
        assert self._client, "Keyspace doesn't have a connection."
        return self._client

    def path(self):
        return u'%s%s%s' % (self.cluster.name, CASPATHSEP, self.name)

    def register_model(self, name, model):
        #print 'REGISTER MODEL', name
        self.models[name] = model

    def __str__(self):
        return self.name

    def register_keyspace_with_cassandra(self):
        ksdef = KsDef(name=self.name, strategy_class=self.strategy_class,
                      replication_factor=self.replication_factor,
                      cf_defs=[x.asCfDef() for x in self.models.values()], # XXX: create columns at the same time
                     )
        self.getclient().system_add_keyspace(ksdef)

    def verify_datamodel(self, **kwargs):
        self._first_iteration_in_this_cycle = True
        # print 'VERIFY', kwargs
        for model in self.models.values():
            self.verify_datamodel_for_model(model=model, **kwargs)
    
    @classmethod
    def verify_datamodel_for_model(cls, model, **kwargs):
        cls.verify_keyspace_for_model(model=model, **kwargs)
        cls.verify_columnfamilies_for_model(model=model, **kwargs)
        model._keyspace._first_iteration_in_this_cycle = False
        
    @classmethod
    def verify_keyspace_for_model(cls, model, **kwargs):
        first_iteration = model._keyspace._first_iteration_in_this_cycle
        
        allkeyspaces = cls._sys_mgr.list_keyspaces()
        
        if first_iteration and model._keyspace.name in allkeyspaces and kwargs['auto_drop_keyspace']:
            print 'Autodropping keyspace %s...' % (model._keyspace,)
            #client.set_keyspace(model._keyspace.name) # this op requires auth
            #client.system_drop_keyspace(model._keyspace.name)            
            allkeyspaces = cls._sys_mgr.drop_keyspace(model._keyspace.name)
            allkeyspaces = cls._sys_mgr.list_keyspaces()
            
        if not model._keyspace.name in allkeyspaces:
            print "Cassandra doesn't know about keyspace %s (only %s)" % (model._keyspace, repr(tuple(allkeyspaces))[1:-1])
            if kwargs['auto_create_models']:
                print 'Creating keyspace %s...' % (model._keyspace,)
                model._keyspace.register_keyspace_with_cassandra()
    
    @classmethod
    def verify_columnfamilies_for_model(cls, model, **kwargs):
        first_iteration = model._keyspace._first_iteration_in_this_cycle
        
        client = model.getclient()
        if not client._keyspace_set:
            client.set_keyspace(model._keyspace.name)
            
        mykeyspace = client.describe_keyspace(model._keyspace.name)
        if first_iteration and kwargs['auto_drop_columnfamilies']:
            for cf in mykeyspace.keys():
                print 'Dropping %s...' % (cf,)     
                client.system_drop_column_family(model._keyspace.name, cf)
            mykeyspace = client.describe_keyspace(model._keyspace.name)
        if not model._column_family in mykeyspace.keys():
            print "Cassandra doesn't know about ColumnFamily '%s'." % (model._column_family,)
            if kwargs['auto_create_models']:
                print 'Creating ColumnFamily %s...' % (model._column_family,)
                model.register_columnfamiliy_with_cassandra()
                mykeyspace = client.describe_keyspace(model._keyspace.name)            
        mycf = mykeyspace[model._column_family]
        assert model._column_type == mycf['Type'], "Cassandra expects Column Type '%s' for ColumnFamily %s. Tragedy thinks it is '%s'." % (mycf['Type'], model._column_family, model._column_type)
        remotecw = mycf['CompareWith'].rsplit('.',1)[1]
        assert model._order_by == remotecw, "Cassandra thinks ColumnFamily '%s' is sorted by '%s'. Tragedy thinks it is '%s'." % (model._column_family, remotecw, model._order_by)
        
if __name__ == "__main__":
    pass
        
    