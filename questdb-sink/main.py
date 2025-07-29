from questdb.ingress import Sender, TimestampNanos

conf = "http::addr=questdb:9000;"          # HTTP endpoint; no auth in local dev
with Sender.from_conf(conf) as sender:
    # Each `row()` call can create the table automatically if it’s new
    sender.row(
        "sensor_data",
        symbols={"id": "sensor-A"},
        columns={"value": 42.0},
        at=TimestampNanos.now()              # designated timestamp (ns)
    )
    sender.row(
        "sensor_data",
        symbols={"id": "sensor-B"},
        columns={"value": 17.3},
        at=TimestampNanos.now()
    )
    sender.flush()     