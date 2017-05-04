class Autoloader(object):
    def __init__(self):
        self.classes = {}

    def register(self, cls, category):
        try:
            self.classes[category].append(cls)
            self.classes[category] = list(set(self.classes[category]))
        except KeyError:
            self.classes[category] = [cls]

_AUTO = Autoloader()
DEFAULT_CATEGORY = 'general'

def register(cls, category=DEFAULT_CATEGORY):
    global _AUTO
    _AUTO.register(cls, category)
    return cls

def class_list(category=None):
    global _AUTO
    if category is None:
        return _AUTO.classes
    else:
        return _AUTO.classes.get(category, [])
