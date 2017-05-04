
import tCass.views

# view class


class DayType(tCass.views.NewClustering):
    viewtag = 'daytype'
    new_clustering = 'day_type'

    def find_type(self, hour, day_type, yrydays=None, limit=None):
        q = self.objects.filter(hour=hour)
        if yrydays:
            q = q.filter(yryday__in=yrydays)
        q = q.limit(limit)

        filter_cond = {'{0}__eq'.format(self.new_clustering):
                       day_type}
        return q.filter(**filter_cond)
