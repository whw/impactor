import time


def _build_tumalow_packet(output, soc, ts):
    return [
        {
            "VaTech": {
                "soc": soc,
                "billing_demand": -1.0,
                "regd_ts": 1430496000.0,
                "battery": 0.0,
                "emergency_time": 1.333333,
                "ts": ts,
                "other": {
                    "plug1": {
                        "Power Sum": None
                    },
                    "compressor": {
                        "Power Sum": None
                    },
                    "airhandler": {
                        "Power Sum": None
                    },
                    "plug2": {
                        "Power Sum": None
                    },
                    "lighting": {
                        "Power Sum": None
                    }
                },
                "usage": 0.0,
                "output": output,
                "target_output": 1.0,
                "pv_production": 0.0
            }
        }
    ]
