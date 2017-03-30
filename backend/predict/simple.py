import json

from base_strategy import BaseStrategy


status_to_orders = {
    -1: 0,
    0: 1,
    1: -1,
}


class SimpleStrategy(BaseStrategy):

    def predict(self, device_id, data):
        return json.dumps({
            "ts": data['ts'],
            "cmd": "output",
            "power": status_to_orders[data['output']]
        })
