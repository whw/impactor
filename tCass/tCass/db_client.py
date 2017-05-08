from functools import partial
import pytz
# import pandas as PD
import importlib

from cassandra.cqlengine.connection import get_session

# import sys
# import os
# sys.path.insert(1, os.path.join(sys.path[0], '../..'))

# import tCass
# import tCass.terms as terms
import tCass.models as models
import tCass.keyspaces as keyspaces
import tCass.constants as constants
from tCass.tools import connect, allow_schema_management
# import tCass.tMet.tMet as tMet
import tCass.autoload as autoload
import tCass.resources


class Client(object):

    @property
    def keyspace(self):
        # you can't change the value of the keyspace once its made
        # this pattern will remove the temptation to do so
        return self._keyspace

    def load_category(self, category):
        for model in autoload.class_list(category=category):
            self.load_model(model)


class resourceModel(Client):

    def __init__(self, res_name, keyspace=None):
        self.res_name = res_name
        self._keyspace = keyspace

        res_models = importlib.import_module(
            constants.resource_import_path.format(res_name))

        self.models = {}
        categories = [res_name]
        for c in categories:
            self.load_category(c)

    def __getattr__(self, name):
        try:
            return self.models[name]
        except:
            raise AttributeError(
                'resourceModel has no attribute {0}'.format(name))

    def __getitem__(self, key):
        return self.models[key]

    def load_model(self, model):
        if self.keyspace:
            ks_cls = models.make_keyspaced_model(model, self.keyspace)
        else:
            ks_cls = model
        inst = ks_cls(res_name=self.res_name)
        self.models[ks_cls.access_name] = inst

    def process_packet(self, res_pkt):
        assert len(res_pkt.keys()) == 1
        model_name = res_pkt.keys()[0]
        row_data = res_pkt[model_name]
        try:
            self.models[model_name].process_packet(row_data)
        except KeyError:
            pass


class dataClient(Client):

    def __init__(self, res_names=None, keyspace=None):
        '''
        Create a dataClient to access data from the cassandra database.

        res_names: list of resource names that you want available from this client.
            Default is all known resources

        keyspace: specify the keyspace you want to read from.
            Default is whatever the current keyspace is.
        '''

        res_names = res_names or tCass.resources.all_resources
        self._keyspace = keyspace or keyspaces.default

        self.res_names = res_names

        # self.tmet = tMet.make_tmet()

        self.models = {}
        self.load_category(autoload.DEFAULT_CATEGORY)

        self.res_models = {}
        for rn in self.res_names:
            self.res_models[rn] = resourceModel(res_name=rn,
                                                keyspace=self._keyspace)

        self.connect(keyspace=self._keyspace)

    def __getattr__(self, name):
        try:
            return self.res_models[name]
        except KeyError:
            raise AttributeError(
                'dataClient has no attribute {0}'.format(name))

    def __getitem__(self, key):
        return self.res_models[key]

    def load_model(self, model):
        if self.keyspace:
            ks_cls = models.make_keyspaced_model(model, self.keyspace)
        else:
            ks_cls = model
        inst = ks_cls()
        self.models[ks_cls.access_name] = inst

    @property
    def all_models(self):
        mod = self.models.values()

        for res_name, resmod in self.res_models.items():
            mod += resmod.models.values()
        return mod

    def connect(self, *args, **kwargs):
        connect(*args, **kwargs)

    def process_packet(self, pkt):
        res_name = pkt.get('resource', None)
        data_l = pkt.get('data', None)
        # some logic for type and version?

        try:
            res_model = self.res_models[res_name]
        except AttributeError:
            print 'No resource model for', res_name
            # there is no resmodel for this part of the packet
            return None

        for item in data_l:
            res_model.process_packet(item)

    def rewind(self, **kwargs):
        for mod in self.all_models:
            mod.rewind(**kwargs)

    def build_keyspace(self):
        keyspaces.build(self.keyspace)

    def insert_from(self, source_keyspace, start, end=None, extra_days=0):
        '''
        Populate this keyspace with data from another keyspace.
        source_keyspace: name of the keyspace you want the data to come from
        start, end: The start and end times.  Can be anything you can pass
                    to the pandas to_datetime function.  If you pass in
                    a number it will be interpreted as seconds.
                    By default, end is None which is no ending cutoff time
        extra_days: Some tables (e.g. Training, Holidays) support putting extra
                    days of data at the beginning of them.  For these tables
                    you will get all data from start-extra_days until end
        '''

        def check_resp(resp):
            if False in [r.success for r in resp]:
                Warning('There were errors in cassandra execution')

        allow_schema_management()
        self.build_keyspace()
        source_client = dataClient(keyspace=source_keyspace)

        for name, model in self.models.iteritems():
            print 'Copying', name, model
            src_model = source_client.models[name]
            resp = model.insert_from(src=src_model,
                                     start=start, end=end)
            check_resp(resp)

        for res, res_model in self.res_models.iteritems():
            print 'Copying', res, 'tables'
            for name, model in res_model.models.iteritems():
                src_model = source_client[res][name]
                print '---> ', name, model
                try:
                    resp = model.insert_from(
                        src=src_model, start=start, end=end, extra_days=extra_days)
                except TypeError:
                    resp = model.insert_from(
                        src=src_model, start=start, end=end)
                check_resp(resp)

    def tearDown(self):
        True
        # return self.tmet.tearDown()

if __name__ == '__main__':
    import sys
    print sys.argv[1]
    client = dataClient(keyspace=sys.argv[1])
    print 'Client made with name "client"'
    import pdb
    pdb.set_trace()
