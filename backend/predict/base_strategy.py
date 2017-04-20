from abc import ABCMeta, abstractmethod


class BaseStrategy():
    __metaclass__ = ABCMeta

    @abstractmethod
    def predict(self, device_id, data):
        raise "Not implemented"
