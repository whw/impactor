import pandas as pd

from cassandra.cqlengine.query import QueryException
from cassandra.cqlengine import columns as cCols

import tCass.models as models
import tCass.autoload as autoload
import tCass.tools as CT
import tCass.resources.VaTech.views as VTviews

res_name = 'VaTech'


class Usage(models.Usage):
    airhandler = cCols.Float()
    compressor = cCols.Float()
    lighting = cCols.Float()
    plug1 = cCols.Float()
    plug2 = cCols.Float()
    __table_name__ = models.resource_table_name(models.Usage, res_name)
    # gets called when packets come in to tpks and they need to enter the db

    def process_packet(self, res_pkt):
        # need to unify the terms in the pkt and the names of the columns
        dd = self.parse_packet_for_usage(res_pkt)

        if dd:
            use_data = res_pkt.get('other', {})
            circuit_usage = {}
            for circuit_name, circuit_info in use_data.iteritems():
                circuit_usage[circuit_name] = circuit_info['Power Sum']

            dd.update(circuit_usage)
            self.insert_row(dd)


class YrydayBaseModel(models.ModifiedModel):
    __abstract__ = True

    def insert_from(self, src, start, end, extra_days=0):
        start = pd.to_datetime(start, unit='s')
        start -= pd.Timedelta(days=extra_days)
        yrydays = CT.yryday_range(start, end)
        df = src.get_df(yrydays=yrydays)
        if df.shape[0] > 0:
            return self.insert_df(df)
        else:
            return []


class TrainingBase(YrydayBaseModel):
    hour = cCols.Integer(primary_key=True)
    yryday = cCols.Integer(primary_key=True, clustering_order='DESC')
    f_m7 = cCols.Float()
    f_m718 = cCols.Float()
    f_start2now = cCols.Float()
    f_now2end = cCols.Float()
    dow = cCols.Integer()
    kWh_now = cCols.Float()
    kWh_L = cCols.Float()
    kWh_A = cCols.Float()
    kWh_tot = cCols.Float()
    target = cCols.Float()

    HOURS_RANGE = range(0, 23)

    __table_name_case_sensitive__ = True
    # must specify this access name so client knows what to call it
    access_name = 'training_base'
    __table_name__ = models.resource_table_name(access_name, res_name)
    _has_datetime_primary_key = False

    def get(self, hour=None, yrydays=None):
        if hour is None:
            q = self.day_records_query(yrydays)
        else:
            q = self.objects.filter(hour=hour)
            if yrydays is not None:
                q = q.filter(yryday__in=yrydays)
        q.limit(None)
        return q

    def get_df(self, *args, **kwargs):
        q = self.get(*args, **kwargs)
        return self.convert_to_df(q)

    @classmethod
    def format_df(cls, df, hour=None):
        df.index.name = 'yryday'
        df = df.reset_index()
        if hour is not None:
            df['hour'] = hour
        return df

    def rewind(self, hours=None, **kwargs):
        hours = hours or self.HOURS_RANGE
        self.rewind_multiple(iterable=hours, col_name='hour',
                             **kwargs)

    def day_records_query(self, yrydays):
        q = self.objects.filter(hour__in=self.HOURS_RANGE)
        try:
            # accept a list of yrydays
            q = q.filter(yryday__in=yrydays)
        except QueryException:
            # or a single yryday
            q = q.filter(yryday=yrydays)
        return q

    def day_records(self, yrydays):
        q = self.day_records_query(yrydays)
        return self.convert_to_df(q, index='hour')


class Holidays(YrydayBaseModel):
    yryday = cCols.Integer(primary_key=True)

    # must specify this access name so client knows what to call it
    __table_name_case_sensitive__ = True
    access_name = 'holidays'
    __table_name__ = models.resource_table_name(access_name, res_name)

    def get(self, yrydays):
        q = self.filter(yryday__in=yrydays)
        q.limit(None)
        return q

    def get_df(self, *args, **kwargs):
        q = self.get(*args, **kwargs)
        return self.convert_to_df(q, index='yryday')

    @classmethod
    def insert_df(cls, df):
        df.index.name = 'yryday'
        df = df.reset_index()
        return cls.insert_concurrent(df)

    def rewind(self, **kwargs):
        # TODO PLACEHOLDER!  We need to rewind this table somehow
        pass


class Training(TrainingBase):
    # you automatically get all the same columns as the existing training table
    # because we inherited from VT.Training
    # in addition to those, lets define a new column
    day_type = cCols.Integer()
    target_predict = cCols.Float()
    kWh_tot_predict = cCols.Float()
    kWh_next_hr = cCols.Float()
    kWh_next_hr_predict = cCols.Float()

    # access_name is how the this table is stored in data_clients
    # this has nothing to do with cassandra, only the data_client
    access_name = 'training'
    # CQLEngine uses this syntax to define the table name in cassandra
    # I think it is a nice convention to use <res name>_<access_name>
    # as the actual table name in cassandra.
    # This is purely stylistic though and not enforced by anything
    __table_name__ = models.resource_table_name(access_name, res_name)

    day_type_view = VTviews.DayType

    def create_views(self):
        self.create_view(self.day_type_view)
        return super(Training, self).create_views()

    def get_by_day_type(self, hour, day_type, yrydays=None, limit=None):
        view_inst = self.attach_view(self.day_type_view)(**self)
        return view_inst.find_type(hour=hour, yrydays=yrydays,
                                   day_type=day_type,
                                   limit=limit)

    def get_df_by_day_type(self, *args, **kwargs):
        q = self.get_by_day_type(*args, **kwargs)
        return self.convert_to_df(q)

autoload.register(Usage, res_name)
autoload.register(Holidays, res_name)
autoload.register(Training, res_name)

if __name__ == '__main__':
    import tSrv.DataService.tCass.tools as CT
    CT.connect()
    tt = Training()
    print tt.get_df_by_day_type(hour=10, day_type=0, limit=5)
