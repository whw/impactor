import json


def handler(event, context):
    print("Received Status: \n" + json.dumps(event, indent=2))
    return json.dumps({'orders': 0})
