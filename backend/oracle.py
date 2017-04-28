from db import db
from predict import predict


def handler(datapoints, context):
    # datapoints is a native python data structure, not the raw json string
    # you expect. This conversion is handled by Lambda automatically.

    prediction_strategy = predict.get_strategy()
    orders = None

    for datapoint in datapoints:
        db.get_db().write_item(datapoint, 'usage')
        orders = prediction_strategy.predict(datapoint)

    return orders
