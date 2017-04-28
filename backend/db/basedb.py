from abc import ABCMeta, abstractmethod


class BaseDB():
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_table(self, table_name):
        raise "Not implemented"

    @abstractmethod
    def delete_table(self, table_name):
        raise "Not implemented"

    @abstractmethod
    def count_items(self, table_name):
        raise "Not implemented"

    @abstractmethod
    def scan_table(self, table_name):
        raise "Not implemented"

    @abstractmethod
    def write_item(self, data, table_name):
        raise "Not implemented"
