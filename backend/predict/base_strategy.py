from abc import ABCMeta, abstractmethod


class BaseStrategy():
    __metaclass__ = ABCMeta

    @abstractmethod
    def predict(self, latest_datapoint):
        raise "Not implemented"
