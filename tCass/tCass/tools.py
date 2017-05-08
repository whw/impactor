import datetime
import os
import pandas as PD

from cassandra.cqlengine.connection import setup
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery

from tCass import progress
import tCass.keyspaces as keyspaces


cassandra_ip = os.getenv('T_CASSANDRA_IP', 'localhost')
DEFAULT_INTERFACE = [cassandra_ip]


def this_year():
    return datetime.datetime.now().year


def pdt(item):
    return PD.to_datetime(item, unit='s')


def yryday(pdt):
    return pdt.year * 1000 + pdt.dayofyear


def yryday_range(start, end):
    start = pdt(start)
    end = pdt(end)
    daterange = PD.date_range(start, end, freq='D')
    return list(yryday(daterange))


def query_to_df(query, index=None, drop_cols=None, utc=False):
    '''
    query: a cqlengine query object
    index: column name of the column you want to become the index
            Or None for a simple sequential index
    utc: If True, the index will be localized to UTC.  Will cause
            an exception if set to True on non-timestamp indexes
    Given a result object, return
    a pandas dataframe with the same data
    '''
    drop_cols = None  # JTG oct18
    sq = query._select_query()
    # retrieve all the actual values
    # we could make this a deferred if we wanted to
    results_generator = query._execute(sq)
    rg = results_generator
    df = PD.DataFrame(results_generator[:])

    if df.shape == (0, 0):
        return df

    if index is not None:
        df.set_index(index, inplace=True)
    if drop_cols is not None:
        if isinstance(drop_cols, str):
            df.drop(drop_cols, axis=1, inplace=True)
        else:
            for col in drop_cols:
                dr.drop(col, axis=1, inplace=True)
    if utc:
        df.index = df.index.tz_localize('UTC')
    return df


def allow_schema_management(allow=True):
    # this environment variable just has to be set to avoid a future warning
    key = 'CQLENG_ALLOW_SCHEMA_MANAGEMENT'
    if allow:
        os.environ[key] = ' '
    else:
        try:
            del os.environ[key]
        except KeyError:
            pass


def create(model):
    try:
        sync_table(model)
    except TypeError:
        # instance of a model as passed in, not the model itself
        sync_table(model.__class__)


def chunks(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    len_iter = len(iterable)
    for i in xrange(0, len_iter, n):
        progress.show(i, len_iter)
        yield iterable[i:i + n]


def connect(keyspace=None, address=None, manage=False):
    keyspace = keyspace or keyspaces.default
    address = address or DEFAULT_INTERFACE
    setup(address, keyspace)
    allow_schema_management(manage)


def sort_columns(df):
    cols = df.columns.tolist()
    cols.sort()
    return df[cols]


def move_col(df, col, pos=0):
    cols = df.columns.to_list()

    cols.remove(col)
    cols.insert(pos, col)
    return df[cols]


def split_df_by_nulls(df, col):
    '''
    Splits a dataframe into two dataframes.
    has: The data frame where every value of col is not null
    hasnot: The data frame where every value of col was null.
            Col has been removed from the hasnot dataframe
    '''
    null_locs = df[col].isnull()
    hasnot = df[null_locs]
    del hasnot[col]
    has = df[~null_locs]
    return has, hasnot


def split_df(df, cols=None):
    '''
    I'm sure there is a way more efficient way of doing this
    (using recursion).
    But, this works.
    Returns a list of dataframes which are guaranteed to not have Nans
    in them.  The dataframes are split into two dataframes, one with Nans
    and one without.  The one with Nans is further split until all the Nans
    are in a single column.  That column is then eliminated.
    '''
    prev_has = [df]
    prev_hasnots = []
    if cols is None:
        cols = df.columns
    for col in cols:
        this_round_has = []
        this_round_hasnots = []
        for mydf in prev_has + prev_hasnots:
            has, hasnot = split_df_by_nulls(mydf, col)
            this_round_has.append(has)
            this_round_hasnots.append(hasnot)
        prev_has = this_round_has
        prev_hasnots = this_round_hasnots
    return prev_has + prev_hasnots
