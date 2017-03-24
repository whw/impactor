import json

status_to_orders = {
    -1: 0,
    0: 1,
    1: -1,
}


def handler(event, context):
    # print("Received Status: \n" + json.dumps(event, indent=2))
    status = event['status']
    print("Received Status: " + str(status))
    return json.dumps({'orders': status_to_orders[status]})
