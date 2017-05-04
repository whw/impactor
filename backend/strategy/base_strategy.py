from abc import ABCMeta, abstractmethod


class BaseStrategy():
    __metaclass__ = ABCMeta

    @abstractmethod
    def generate_command(self, resource_packet):
        raise "Not implemented"
