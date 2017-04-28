import json

from base_strategy import BaseStrategy


class SimpleStrategy(BaseStrategy):

    def predict(self, latest_datapoint):
        return json.dumps({
            "ts": latest_datapoint['ts'],
            "cmd": "output",
            "power": latest_datapoint['output']
        })
