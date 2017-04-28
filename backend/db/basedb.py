from abc import ABCMeta, abstractmethod


class BaseDB():
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_table(self):
        raise "Not implemented"

    @abstractmethod
    def delete_table(self):
        raise "Not implemented"

    @abstractmethod
    def number_of_items_in_table(self):
        raise "Not implemented"

    @abstractmethod
    def scan_table(self):
        raise "Not implemented"

    @abstractmethod
    def write_item(self, data):
        raise "Not implemented"
