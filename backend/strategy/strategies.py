def get_strategy(name='simple'):
    if name == 'simple':
        from simple_strategy import SimpleStrategy
        return SimpleStrategy()

    else:
        raise 'Only the simple strategy has been implemented. Not this: ' + name
