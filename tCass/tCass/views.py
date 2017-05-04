from cassandra.cqlengine.connection import get_session
import tCass.tools as CT


class ViewAlreadyAttached(Exception):
    pass


class View(object):
    viewtag = None
    # must specify this to generate the viewname

    @classmethod
    def attach_to_model(cls, model):
        try:
            model._model_cls
            model._view_cls
            raise ViewAlreadyAttached('{mod} is already attached to {view}'.format(
                mod=model, view=model._view_cls))
        except AttributeError:
            pass

        if not isinstance(model, type):
            # model is an instance, but it needs to be a class
            model = model.__class__

        class AttachedView(model, cls):
            _model_cls = model
            _view_cls = cls
            __table_name__ = '{0}_{1}'.format(
                model.__table_name__, cls.viewtag)
            _table_name = __table_name__
        return AttachedView

    def drop(self):
        session = get_session()
        # session.get_keyspace(KS)
        cmd = """DROP MATERIALIZED VIEW {ks}."{viewname}";""".format(
            ks=self.active_keyspace(), viewname=self.viewname)
        session.execute(cmd)


class NewClustering(View):
    viewtag = "newclustering"
    # name of the new clustering column
    new_clustering = 'newclustering'

    def create_attached_view(self):
        '''
        This can only be run once the view is attached
        '''
        session = get_session()

        primary_keys = self._partition_keys.keys()
        primary_keys.append(self.new_clustering)
        primary_keys += self._clustering_keys.keys()

        where = 'WHERE '
        for col_name in primary_keys:
            where += '{0} IS NOT NULL AND '.format(col_name)
        # remove the final "AND "
        where = where[:-4]

        primary = primary_keys[:]
        primary = tuple(primary)
        primary = str(primary).replace("'", '"')

        cmd = """CREATE MATERIALIZED VIEW IF NOT EXISTS {ks}."{viewname}" AS
                          SELECT * FROM {ks}."{table}"
                          {where}
                          PRIMARY KEY {primary}
                          WITH CLUSTERING ORDER BY ({new_clustering} DESC);""".format(
            ks=self.active_keyspace(), viewname=self.__table_name__,
            table=self._model_cls.__table_name__,
            where=where,
            new_clustering=self.new_clustering,
            primary=primary)
        print cmd
        session.execute(cmd)


class Modified(NewClustering):
    viewtag = "modified"
    new_clustering = 'modified'

    def since(self, last_time, query=None, limit=None):
        # translation: clustering column > last time
        query = query or self.filter_by_partition()
        # find times greater than ("__gt") the passed in last_time
        filter_cond = {'{0}__gt'.format(self.new_clustering):
                       CT.pdt(last_time)}
        query = query.filter(**filter_cond)
        query = query.limit(limit)
        return query

if __name__ == '__main__':
    import tSrv.DataService.tCass.models as models
    import tSrv.DataService.tCass.resources.VaTech.models as VTmodels

    vd = VTmodels.Validdays
    vd = models.make_keyspaced_model(vd, 'dev_wtg')
    vd = vd(res_name='VaTech')

    CT.connect()

    qry = vd.modified_since('2016')

    print qry
    print qry[:]
