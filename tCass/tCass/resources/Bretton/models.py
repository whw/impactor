from cassandra.cqlengine import columns as cCols

import tCass.models as models
# import terms as terms
import tCass.autoload as autoload
import tCass.tools as CT

res_name = 'Bretton'


class Thermostat(models.YearTSModel):
    temp = cCols.Float()
    t_heat = cCols.Float()
    tmode = cCols.Integer()
    tstate = cCols.Integer()

    access_name = 'tstat'
    __table_name__ = models.resource_table_name(access_name, res_name)


class Switches(models.YearTSModel):
    # operation = cCols.Text()  #the name of the overall state.
    mensheater = cCols.Integer()  # 1 is on, 0 is off
    ladiesheater = cCols.Integer()
    wellpump = cCols.Integer()
    waterheater = cCols.Integer()

    access_name = 'switches'
    __table_name__ = models.resource_table_name(access_name, res_name)


class FX(models.YearTSModel):
    '''
    Data products from the FX inverter
    '''
    current = cCols.Integer()  # inverter current
    acoutv = cCols.Integer()  # ac output in volts
    opmode = cCols.Integer()  # FX operational mode
    error = cCols.Integer()  # FX errors
    batteryvolt = cCols.Float()  # battery voltage
    warning = cCols.Integer()

    access_name = 'inverter'
    __table_name__ = models.resource_table_name(access_name, res_name)


class Flexnet(models.YearTSModel):
    shuntamps_a = cCols.Float()
    shuntamps_b = cCols.Float()
    shuntamps_c = cCols.Float()
    extraid = cCols.Integer()
    extradata = cCols.Integer()
    batteryvolt = cCols.Float()
    soc = cCols.Float()

    # shunts enabled
    # 0 is enabled for some strange reason
    a_enable = cCols.Integer()
    b_enable = cCols.Integer()
    c_enable = cCols.Integer()

    batterytemp = cCols.Integer()
    statusflags = cCols.Integer()
    '''
    From the Flexnet DC documentation
    Status Flags: 00 to 63 This is an ASCII expression of an 8 bit byte, with each bit representing a
    different flag.  Relay state is 1 when relay is closed, 0 if relay is open.
    Relay mode is 0 if manual mode, 1 if relay control is in automatic mode.
    e.g. A returned 009 would be charge parms met and shunt 1 values are negative.

    BIT     #   Value Warning
    ----------------------------
    1       1   Charge parms met
    2       2   Relay mode:
    3       4   Relay state:
    4       8   Shunt A values are negative
    5       16  Shunt B values are negative
    6       32  Shunt C values are negative
    '''

    access_name = 'flexnet'
    __table_name__ = models.resource_table_name(access_name, res_name)


class Charger(models.YearTSModel):
    '''
    Data products from the MX charge controller
    '''
    address = cCols.Text(primary_key=True)
    current = cCols.Float()
    pvcurrent = cCols.Float()
    pvvolt = cCols.Float()
    daykwh = cCols.Float()
    aux = cCols.Integer()
    error = cCols.Integer()
    chargemode = cCols.Text()
    batteryvolt = cCols.Float()
    dayah = cCols.Float()

    access_name = 'charger'
    __table_name__ = models.resource_table_name(access_name, res_name)

    def get_df(self, start, end=None, freq=None):
        return super(Charger, self).get_df(start=start, end=end,
                                           freq=freq, index='ts')

    def get_day_kwh(self):
        q = self.objects.all()
        q = q.limit(2)  # should read how many to expect
        # TODO this is really ugly
        return sum(entry.daykwh for entry in q[:])


class Usage(models.Usage):
    pv_production = cCols.Float()

    access_name = 'usage'
    __table_name__ = models.resource_table_name(access_name, res_name)


autoload.register(FX, res_name)
autoload.register(Charger, res_name)
autoload.register(Usage, res_name)
autoload.register(Switches, res_name)
autoload.register(Thermostat, res_name)
autoload.register(Flexnet, res_name)
