import tCass.autoload as autoload

#autoload these general tables
import tCass.tMet.db_weather as dbw

autoload.register(dbw.Wunderground, autoload.DEFAULT_CATEGORY)
autoload.register(dbw.NOAA, autoload.DEFAULT_CATEGORY)
autoload.register(dbw.Mesowest, autoload.DEFAULT_CATEGORY)
