import traceback
import datetime

import numpy as NP
import pandas as PD
from cassandra.cqlengine import columns as C
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.connection import get_session
from cassandra.concurrent import execute_concurrent_with_args

import tCass.tools as CT
import tCass.keyspaces as keyspaces
import tCass.views as views

# =================== Tools =====================
# tools for manipulating models or tables


def save(model):
    return copy(model).save()


def truncate(model, keyspace=None):
    keyspace = keyspace or keyspaces.get()
    if keyspaces.allowed_to_delete_from_keyspace(keyspace):
        session = get_session()
        # now that the keyspace is an acceptable one, we can use it
        session.set_keyspace(keyspace)
        # and truncate the table from the development keyspace
        session.execute('TRUNCATE TABLE "{0}"'.format(model.__table_name__))


def resource_table_name(model, res_name):
    '''
    Enforce a naming convention of table names.
    They should all be <res_name>_<table_name>
    For example: VaTech_usage
    model can be either a string or a model class or instance.
    '''
    try:
        model_name = model.access_name
    except AttributeError:
        # model was passed in as a string, rather than a model object
        model_name = model
    return '{0}_{1}'.format(res_name, model_name)


def make_keyspaced_model(model, keyspace):
    class KSmodel(model):
        __keyspace__ = keyspace
        __original_model__ = model

        def __repr__(self):
            return '<tModel {0} in {1}>'.format(
                self.__original_model__.__table_name__,
                self.__keyspace__)

        def __str__(self):
            return self.__repr__()
    return KSmodel


class EmptyTableError(Exception):
    pass

#============ Types of Models =======================
# Define the types of models here
# these should be inherited from in each of the resources
# example: dont make a usage table, make a resource_usage table


class AmbigiousClusteringKey(Exception):
    pass


class tModel(Model):
    __abstract__ = True

    # tables that have a datetime primary key
    # will be tz localized to UTC if you ask for their data
    # in a dataframe
    _has_datetime_primary_key = False
    _concurrent_chunk_size = 2000

    def __init__(self, *args, **kwargs):
        super(tModel, self).__init__(*args, **kwargs)
        # keep track of when this was created so we can rewind to it later
        self.created_time = self.now()

    @staticmethod
    def now():
        return datetime.datetime.utcnow()

    def df_index_name(self):
        '''
        Returns the name of the clustering column.  Only usable
        if there is a single clustering column, otherwise this is
        ambigious
        '''
        cluster = self._clustering_keys.keys()
        if len(cluster) != 1:
            raise AmbigiousClusteringKey(
                'Need only one of {0}'.format(cluster))
        return cluster[0]

    def filter_by_columns(self, column_list, query=None, values=None):
        '''
        Going through all the columns in column_list
        and filter with key=value

        column_list: a list of column names

        query: a query object.
                Default is None, which gives you objects.all()

        values: a dictionary of column name: value that you want
                to search on.  Default is None, which uses the
                values currently stored in the Model object
        '''
        # set the default values
        if query is None:
            # evaluating the truth value of a query is actually very slow
            # so we dont want to use the "query = query or <...>" pattern
            query = self.objects.all()
        values = values or {}

        # go through the partition keys
        for col_name in column_list:
            # find the default value
            # if one is given in the values dict, use that one
            # otherwise, use the currently stored value in this instance
            value = values.get(col_name, getattr(self, col_name))
            filter_cond = {col_name: value}
            query = query.filter(**filter_cond)
        return query

    def filter_by_partition(self, *args, **kwargs):
        '''
        All arguments passed to filter_by_columns
        '''
        return self.filter_by_columns(
            column_list=self._partition_keys.keys(),
            *args, **kwargs)

    def filter_by_clustering(self, *args, **kwargs):
        '''
        All arguments passed to filter_by_columns
        '''
        return self.filter_by_columns(
            column_list=self._clustering_keys.keys(),
            *args, **kwargs)

    @classmethod
    def create_keyspaced_version(cls, keyspace):
        return make_keyspaced_model(cls, keyspace)

    def copy(self):
        return self.construct(self)

    @classmethod
    def construct(cls, record=None):
        record = record or {}
        return cls(**record)

    @classmethod
    def active_keyspace(cls):
        return cls.__keyspace__ or keyspaces.get()

    @classmethod
    def position_columns(cls, df):
        '''
        Rearrange the columns in dataframe to match the order that
        Cassandra wants them (e.g. when doing a COPY FROM command)
        '''
        df = CT.sort_columns(df)
        cols = df.columns.tolist()
        keys = cls._partition_keys.keys() + cls._clustering_keys.keys()
        for key in keys:
            try:
                cols.remove(key)
            except ValueError:
                # make a more helpful error message
                raise ValueError('{0} is not a column in {1}'.format(
                    key, cls))

        return df[keys + cols]

    def as_types(self, df):
        '''
        Coerse the dataframe to be the expected datatypes
        '''
        dt_cols = [name for name, col in self._defined_columns.iteritems()
                   if (isinstance(col, C.DateTime) and name in df.columns)]

        for c in dt_cols:
            df[c] = PD.to_datetime(df[c])
        return df

    @classmethod
    def delete_allowed(cls):
        return keyspaces.allowed_to_delete_from_keyspace(cls.active_keyspace)

    def delete(self):
        if self.delete_allowed:
            super(tModel, self).delete()

    def convert_to_df(self, query, index=None, utc=None):
        '''
        Returns the results of a query as a dataframe.
        The dataframe will have an index of the clustering key
        '''
        index = index or self.df_index_name()
        utc = utc or self._has_datetime_primary_key
        return CT.query_to_df(query, index=index, utc=utc)

    def resamp(self, df, freq):
        df = df.sort_index()
        df = df.interpolate(method='time')
        try:
            df = df.resample(freq, how='mean')
        except IndexError:
            # got an empty dataframe
            pass
        except pytz.exceptions.AmbiguousTimeError:
            # this is a bug in pandas
            # https://github.com/pydata/pandas/issues/5172
            # heres the workaround
            df2 = df.tz_convert('UTC')
            df2 = df2.resample(freq, how='mean')
            df2 = df2.tz_convert(df.index.tz)
            tLog.info('DF2')
            return df2
        return df

    @classmethod
    def insert_statement(cls, cols=None):
        '''
        Create an insert statement using the columns of the data frame
        '''
        if cols is None:
            cols = cls._columns.keys()

        vals = tuple(['?'] * len(cols))
        vals = str(vals).replace("'", '')

        cols = tuple(['"{0}"'.format(name) for name in cols])
        cols = str(cols).replace("'", '')

        s = 'INSERT INTO {ks}."{tab}" {cols} VALUES {vals}'.format(
            ks=cls.active_keyspace(),
            tab=cls.__table_name__,
            cols=cols,
            vals=vals)
        return s

    @classmethod
    def insert_concurrent(cls, df):
        '''
        Insert a dataframe using the execute concurrent.
        This should be faster for large dataframes than putting in the rows
        one at a time.
        '''
        ndx = 0
        failures = []
        if 'modified' not in df.columns:
            df['modified'] = datetime.datetime.utcnow()

        # getting unicode instead of str will add a 'u' to the beginning of the col name
        # so coerse to string first
        col_names = [str(name) for name in df.columns.tolist()]

        statement_str = cls.insert_statement(cols=col_names)
        session = get_session()
        statement = session.prepare(statement_str)

        while ndx < df.shape[0]:
            start = ndx
            end = ndx + cls._concurrent_chunk_size
            print 'inserting', start, 'to', end, '/', df.shape[0]
            this_df = df.iloc[start:end]

            parameters = this_df.values.tolist()
            resp = execute_concurrent_with_args(session, statement,
                                                parameters=parameters, results_generator=True)
            failures += cls.parse_failures(resp)

            ndx = end
        return failures

    def process_packet(self, row):
        self.insert_row(row)

    @classmethod
    def insert_row(cls, row_d):
        '''Inserts the data contained in the dictionary row_d
         into the model.
         row_d has keywords as column names
        '''
        # try a few times to insert the row
        for _ in range(3):
            try:
                cls.construct(row_d).save()
                return None  # if the row is inserted, leave the method
            except Exception as ex:
                print('Unable to insert into table',
                      {'table_name': cls.__table_name__,
                       'data': row_d,
                       'traceback': traceback.format_exc(),
                       })

    @staticmethod
    def connect(keyspace=None):
        keyspace = keyspace or keyspaces.default()
        CT.connect(keyspace=keyspace, manage=True)

    @classmethod
    def create(cls, keyspace=None):
        cls.connect(keyspace=keyspace)
        # now that we are connected create the new table in cassandra
        CT.create(cls)

    @staticmethod
    def format_df(df):
        return df

    @classmethod
    def type_cols(cls, cols, col_type):
        return [name for name, col in cls._defined_columns.iteritems()
                if (isinstance(col, col_type) and
                    name not in cls._partition_keys.keys() and
                    name not in cls._clustering_keys.keys() and
                    name in cols)]

    @staticmethod
    def parse_failures(resp):
        return [r for r in resp if r.success == False]

    @classmethod
    def insert_df(cls, df):
        df = cls.format_df(df)
        dfs = cls.split_df(df)

        failures = []
        print 'inserting dfs', len(dfs)
        for insert_this in dfs:
            print insert_this.shape
            if not insert_this.empty:
                resp = cls.insert_concurrent(insert_this)
                failures += cls.parse_failures(resp)
        return failures

    @classmethod
    def split_df(cls, df):
        # seperate out the integer columns
        int_cols = cls.type_cols(df.columns, C.Integer)
        str_cols = cls.type_cols(df.columns, C.Text)
        dto_cols = cls.type_cols(df.columns, C.DateTime)
        split_cols = int_cols + str_cols + dto_cols
        print 'splitting cols', split_cols
        dfs = CT.split_df(df, cols=split_cols)
        return dfs

    def insert_from(self, src, *args, **kwargs):
        '''
        Copies data in the source table
        into this table.

        src: Data Model object of the source table

        returns a dataframe of all data copied
        '''
        qry = src.get(*args, **kwargs)
        qry = qry.limit(None)
        df = CT.query_to_df(qry)
        return self.insert_df(df)


class ModelWithView(tModel):
    '''
    An extension of cqlengine model that is made to work with
    materialized views.
    '''
    # this is an abstract table which means there is no
    # corresponding table in the database
    # instead, other table definitions should inherit from this one
    __abstract__ = True

    def attach_view(self, view_cls):
        return view_cls.attach_to_model(self)

    def create_view(self, view_cls):
        att_view = self.attach_view(view_cls)()
        att_view.create_attached_view()

    def create_views(self):
        pass


class ModifiedModel(ModelWithView):
    __abstract__ = True
    modified = C.DateTime()

    modified_view = views.Modified

    def create_views(self):
        self.create_view(self.modified_view)
        super(ModifiedModel, self).create_views()

    def save(self, modified_time='now'):
        '''
        Override the save method to always write the
        modified time.  You can spoof the modified time
        or just use the current time.
        '''
        if modified_time == 'now':
            self.modified = self.now()
        else:
            self.modified = modified_time
        return super(ModifiedModel, self).save()

    def make_modified_view(self):
        view = self.attach_view(self.modified_view)
        view_inst = view(**self)
        return view_inst

    def modified_since(self, last_time):
        '''
        Return a query with all the records modified since
        the "last_time"
        last_time: anything that can be parsed into a pandas dto
            Numbers will be evaluated in seconds
        '''
        view_inst = self.make_modified_view()
        return view_inst.since(last_time=last_time)

    def rewinder(self, last_time, source_keyspace=None):
        '''
        Finds all the records that were modified after last_time
        For every record that is in the same table given in keyspace,
        write that record into this model.
        For every record that is NOT in the same table given in keyspace,
        delete that record.

        This is made to "undo" the writes that were done during development
        or a unit test.
        '''
        if last_time == 'created':
            last_time = self.created_time

        # we will be copying records
        # there is a source and a destination
        # the destination is here.  This is where the writing will happen
        # this is supposed to be in a dev keyspace

        # the source is the same table in a different keyspace(e.g. dev_static)
        # records will be copied from the source to the destination
        # keep this in mind because the naming gets confusing

        # make sure we are allowed to remove records from the dest keyspace
        assert keyspaces.allowed_to_delete_from_keyspace(
            self.active_keyspace())

        # copy records from the static keyspace by default
        keyspace = source_keyspace or keyspaces.dev_static

        # make a keyspaced version of this model
        # define a "source" table
        # the same table as this one, just in another keyspace (usually static)
        source_cls = self.create_keyspaced_version(keyspace)

        recs = self.modified_since(last_time)
        # these records are actually from the materialized view, not this table
        for dest_record in self.modified_since(last_time):
            # copy all the info in this particular record into the source model
            # instance
            source = source_cls(**dest_record)

            # now go searching the database for that record
            records_from_source = source.filter_by_partition()
            records_from_source = source.filter_by_clustering(
                query=records_from_source)
            # really, there should only ever be 1 or 0 records that come back
            assert records_from_source.count() < 2

            # the result contains the data from the SOURCE keyspace
            source_record = records_from_source.first()

            # the record was not found in the source table
            # so delete it from here
            if source_record is None:
                self.construct(dest_record).delete()
            else:
                # the record WAS found in the source table
                # copy that data into here
                dest = self.construct(source_record)
                # set the modified time back to what the source thinks it was
                # in most cases, this will be None
                dest.save(modified_time=source_record.modified)
                # this record should now look identical to the one in source

    def rewind_multiple(self, iterable, col_name, **kwargs):
        '''
        For use when you have to iterate over all possible values
        of a primary key in order to rewind all the values
        iterable: a list of values
        col_name: the name of the column
        e.g. col_name='year' and iterable = [2015, 2016]
        will run rewind for both years given.
        '''
        for item in iterable:
            this_model = self.construct()
            this_model[col_name] = item
            this_model.rewinder(**kwargs)

    # this gets overridden in other Models
    rewind = rewinder


class YearTSModel(ModifiedModel):
    year = C.Integer(primary_key=True)
    ts = C.DateTime(primary_key=True, clustering_order='DESC')
    __table_name_case_sensitive__ = True

    # the range of years that we can expect data in this table
    # really just used for rewinding
    YEARS_RANGE = tuple(range(2010, CT.this_year() + 1))

    _has_datetime_primary_key = True

    @classmethod
    def year_of_ts(cls, ts):
        try:
            return CT.pdt(ts).year
        except AttributeError:
            # None was passed in as the ts
            return cls.YEARS_RANGE[-1]

    def get(self, start, end=None):
        start = CT.pdt(start)
        if end is not None:
            end = CT.pdt(end)

        years = range(self.year_of_ts(start), self.year_of_ts(end) + 1)

        q = self.objects.filter(year__in=years)
        q = q.filter(Usage.ts >= start)
        if end is not None:
            q = q.filter(Usage.ts < end)

        q = q.limit(None)
        return q

    def get_df(self, start, end=None, freq=None, index=None):
        '''
        Queries the database and returns a dataframe with all the data
        from start to end.
        Reamples to "freq" if given.
        Uses the column in "index" if given.  Default is the clustering key
            if there is one and only one clustering key.
        '''
        q = self.get(start=start, end=end)
        df = self.convert_to_df(q, index=index)
        # reverse the order
        df = df.iloc[::-1]
        if freq:
            df = self.resamp(df, freq=freq)
        return df

    def latest(self, year=None):
        if year is None:
            year = self.YEARS_RANGE[-1]
        qq = None
        while (qq is None) and (year > self.YEARS_RANGE[0]):
            qq = self.objects.filter(Usage.year == year).first()
            # this covers the corner case of right after new years
            # the year will increment before new usage comes in
            year -= 1
        try:
            return dict(qq)
        except TypeError:
            # this means the table is empty
            # you made it all the way to MIN_YEAR without finding a record
            raise EmptyTableError('{0} has no data after the year {1}'.format(
                self, self.YEARS_RANGE[0]))

    def asof(self, lookup_time, max_lookback=300):
        '''
        Get the usage as a the lookup time.  Maxlookback is the longest
        ago you will consider usage to be "current"
        '''
        end = CT.pdt(lookup_time)
        start = end - PD.Timedelta(seconds=max_lookback)
        qry = self.get(start, end)
        val = qry.first()
        return dict(val)

    @staticmethod
    def format_df(df):
        # make a copy so you dont change the passed in df
        idf = df.iloc[:]
        if not df.empty:
            if 'ts' not in df.columns:
                idf.index.name = 'ts'
                idf = idf.reset_index()

            idf['year'] = PD.DatetimeIndex(idf.ts).year
            return idf
        else:
            return df

    def process_packet(self, row):
        try:
            row['year'] = self.year_of_ts(row['ts'])
        except KeyError:
            import pdb
            pdb.set_trace()
        self.insert_row(row)

    def rewind(self, years=None, **kwargs):
        years = years or self.YEARS_RANGE
        self.rewind_multiple(iterable=years, col_name='year', **kwargs)


class Usage(YearTSModel):
    usage = C.Float()
    total_demand = C.Float()
    soc = C.Float()
    output = C.Float()
    regd_ts = C.DateTime()

    access_name = 'usage'
    __table_name_case_sensitive__ = True

    def get_df(self, *args, **kwargs):
        df = super(Usage, self).get_df(*args, **kwargs)
        # could make this more general
        for col in ('output', 'soc'):
            try:
                df[col] = df[col].astype(float)
            except KeyError:
                # theres no data in the database about this column
                # it should still appear in the dataframe though
                df[col] = NP.NaN
        return df

if __name__ == '__main__':
    pass
