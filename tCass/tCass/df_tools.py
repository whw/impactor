def localize(item, tz=None):
    try:
        item = PD.to_datetime(item, unit='s')        
        item = item.tz_localize('UTC')
    except (TypeError, AttributeError):
        pass
    if tz:
        item = item.tz_convert(str(tz))
    return item

def localize_df(df, tz):
    '''
    Localize df to the same timezone as the timestamp ts
    '''
    if tz:
        df.index = localize(df.index, tz)
    return df
