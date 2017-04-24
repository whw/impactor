import json

from base_strategy import BaseStrategy


status_to_orders = {
    -1: 0,
    0: 1,
    1: -1,
}


class SimpleStrategy(BaseStrategy):

    def predict(self, latest_datapoint):
        device_id = latest_datapoint.keys()[0]
        data = latest_datapoint[device_id]

        return json.dumps({
            "ts": data['ts'],
            "cmd": "output",
            "power": status_to_orders[data['output']]
        })
