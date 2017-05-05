from db import db
from strategy import strategies


def handler(resource_packets, context):
    # resource_packets is a native python data structure, not the raw json string
    # you expect. This conversion is handled by Lambda automatically.

    # TODO: this connection should be moved outside of the handler function, and
    # only re-established here if the connection has been lost
    database = db.get_db()

    strategy = strategies.get_strategy()
    command = None

    for resource_packet in resource_packets:
        database.write(resource_packet)
        command = strategy.generate_command(resource_packet)

    return command
