import json

from base_strategy import BaseStrategy


class SimpleStrategy(BaseStrategy):

    def predict(self, latest_datapoint):
        device_id = latest_datapoint.keys()[0]
        data = latest_datapoint[device_id]

        return json.dumps({
            "ts": data['ts'],
            "cmd": "output",
            "power": data['output']
        })
