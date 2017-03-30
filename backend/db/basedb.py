from abc import ABCMeta, abstractmethod


class BaseDB():
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_config():
        raise "Not implemented"

    @abstractmethod
    def create_table():
        raise "Not implemented"

    @abstractmethod
    def number_of_items_in_table():
        raise "Not implemented"

    @abstractmethod
    def delete_table():
        raise "Not implemented"
