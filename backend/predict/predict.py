from simple import SimpleStrategy


def get_strategy(name='simple'):
    if name == 'simple':
        return SimpleStrategy()
    else:
        raise 'Only the simple strategy has been implemented. Not this: ' + name
