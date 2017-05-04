'''
Tools for managing keyspaces in cassandra.
Makes it easier to switch keyspaces and manage the
tables in them.

There is a master "ops" keyspace which is
for production only.  There is a dev_static
keyspace which holds a static set of data to develop
and test against.

Any developer can also create their own dev keyspace
and do whatever they want within it.  These keyspaces
can have their tables truncated and even be dropped entirely.
They are easy to load (from tDat cassandra environments)
and switch (just truncate all the tables and load a new environment).
'''

import os
import importlib

from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.connection import get_session
import cassandra.cqlengine.models as cqlmodels
from cassandra.cqlengine.management import create_keyspace_simple

import tCass.constants as constants
import tCass.autoload as autoload
import tCass.resources

ops = 'ops'
system = 'system'
static = 'static'

default = 'unittest'

REPLICATION_FACTOR = 1


def switch(keyspace):
    cqlmodels.DEFAULT_KEYSPACE = keyspace


def get():
    return cqlmodels.DEFAULT_KEYSPACE
#=========== Truncate tables
# !! never delete from tables in these keyspaces!!
NO_DELETE = (ops,
             system,
             static
             )


class DeleteNotAllowed(Exception):
    pass


def allowed_to_delete_from_keyspace(keyspace):
    if keyspace in NO_DELETE:
        raise DeleteNotAllowed(
            '{0} is protected by keyspaces.NO_DELETE'.format(keyspace))
    return True


def completely_remove_keyspace(keyspace):
    if allowed_to_delete_from_keyspace(keyspace):
        session = get_session()
        session.execute('DROP KEYSPACE "{0}"'.format(keyspace))


def build(keyspace, res_names=None):
    '''
    Creates a new keyspace and populates it with blank tables
    '''
    # create the keyspace if it doesn't already exist
    create_keyspace_simple(keyspace,
                           replication_factor=REPLICATION_FACTOR)
    switch(keyspace)
    create_all_tables(res_names)


def create_all_tables(res_names=None):
    '''
    Creates all the tables defined for the resources in res_names
    in the current keyspace.

    res_names: list of resource names
        Default is all the resources listed in
    '''
    # import tCass.autoload_general
    res_names = res_names or tCass.resources.all_resources
    create_tables(autoload.DEFAULT_CATEGORY)
    for name in res_names:
        importlib.import_module(
            constants.resource_import_path.format(name))
        create_tables(name)


def create_tables(category):
    '''
    Creates all the tables in the given category in the current keyspace

    category: string
    '''
    tables = autoload.class_list(category)
    print 'Loading category:', category
    for tab in tables:
        print '--------> Creating table', tab.__table_name__
        print tab
        sync_table(tab)
    for tab in tables:
        tab().create_views()


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
