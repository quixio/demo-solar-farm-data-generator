import logging
from questdb.ingress import Sender, TimestampNanos

# ------------------------------------------------------------------------------
# 1. Configure logging (root logger + nicer format)
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,                                   # default verbosity
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("questdb_ingest")              # module-level logger

# ------------------------------------------------------------------------------
# 2. Ingest two dummy rows and flush
# ------------------------------------------------------------------------------
def ingest_dummy():
    logger.info("Building QuestDB Sender object")
    conf = "http::addr=questdb:9000;"                   # local QuestDB ILP/HTTP

    try:
        with Sender.from_conf(conf) as sender:            # managed connection
            logger.info("Sender created; queuing rows")

            for sensor, val in [("sensor-A", 42.0),
                                 ("sensor-B", 17.3)]:
                logger.debug("Adding row for %s (value=%s)", sensor, val)
                sender.row(
                    "sensor_data",
                    symbols={"id": sensor},
                    columns={"value": val},
                    at=TimestampNanos.now()
                )

            logger.info("Rows queued; initiating flush()")
            sender.flush()
            logger.info("Flush complete — ingestion finished 🎉")

    except Exception as exc:
        logger.exception("Ingestion failed: %s", exc)
        raise                                            # bubble up if needed

if __name__ == "__main__":
    ingest_dummy()
