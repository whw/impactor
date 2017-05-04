import time


def _build_tumalow_packet(output, soc, ts):
    return [
        {
            "device_id": "asdf",
            "location": "VaTech",
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

    ]


def _build_tumalow_packet_bretton(output, soc, ts):
    return [
        {
            'resource': 'Bretton',
            'ts': ts,
            'type': 'status',
            'version': 1,
            'data': [
                {
                    'usage': {
                        'total_demand': 0.0,
                        'output': output,
                        'pv_production': -0.0049,
                        'regd_ts': ts,
                        'soc': soc,
                        'ts': ts,
                        'usage': 1.2915
                    }
                },
                {
                    'inverter': {
                        'acoutv': 118,
                        'address': '1',
                        'batteryvolt': 24.8,
                        'current': 8,
                        'error': 0,
                        'opmode': 2,
                        'ts': ts,
                        'warning': 0
                    }
                },
                {
                    'charger': {
                        'address': 'C',
                        'aux': 3,
                        'batteryvolt': 24.4,
                        'chargemode': 'silent',
                        'current': 0.0,
                        'dayah': 38.0,
                        'daykwh': 0.9,
                        'error': 0,
                        'pvcurrent': 0.0,
                        'pvvolt': 19.0,
                        'ts': ts
                    }
                },
                {
                    'charger': {
                        'address': 'D',
                        'aux': 3,
                        'batteryvolt': 24.7,
                        'chargemode': 'silent',
                        'current': 0.0,
                        'dayah': 113.0,
                        'daykwh': 2.9,
                        'error': 0,
                        'pvcurrent': 0.0,
                        'pvvolt': 18.0,
                        'ts': ts
                    }
                },
                {
                    'flexnet': {
                        'a_enable': 0,
                        'address': 'd',
                        'b_enable': 0,
                        'batterytemp': 11,
                        'batteryvolt': 24.6,
                        'c_enable': 1,
                        'extradata': 436,
                        'extraid': 64,
                        'shuntamps_a': -52.5,
                        'shuntamps_b': -0.2,
                        'shuntamps_c': 0.0,
                        'soc': soc,
                        'status_flags': 24,
                        'ts': ts
                    }
                },
                {
                    'switches': {
                        'ladiesheater': 1,
                        'mensheater': 1,
                        'waterheater': 0,
                        'wellpump': 1,
                        'ts': ts
                    }
                },
                {
                    'tstat': {
                        'fmode': 0,
                        'fstate': 1,
                        'hold': 0,
                        'override': 0,
                        't_heat': 52.0,
                        't_type_post': 0,
                        'temp': 46.0,
                        'time': {'day': 4,
                                 'hour': 18,
                                 'minute': 59},
                        'tmode': 1,
                        'ts': ts,
                        'tstate': 1
                    }
                },
                {
                    'state': {
                        'state': 'normal',
                        'ts': ts
                    }
                },
                {
                    'errors': True
                }
            ]
        }
    ]
