import json

from base_strategy import BaseStrategy


class SimpleStrategy(BaseStrategy):

    def generate_command(self, resource_packet):
        return json.dumps({
            "ts": resource_packet['ts'],
            "cmd": "output",
            "power": 1
        })
