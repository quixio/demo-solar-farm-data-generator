import os
import questdb.ingress as qi  # You must install the QuestDB Python client
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self.host = os.environ["QUESTDB_HOST"]
        self.port = int(os.environ.get("QUESTDB_PORT", 9009))
        self.token = os.environ["QUESTDB_TOKEN"]
        self.table = os.environ["QUESTDB_TABLE_NAME"]

    def setup(self):
        self.sender = qi.Sender(
            host=self.host,
            port=self.port,
            auth_token=self.token
        )

    def write(self, batch: SinkBatch):
        for item in batch:
            value = item.value
            # Build ILP line for QuestDB
            line = f"(self.table)"
            for k, v in value.items():
                line += f",{k}={v}"
            self.sender.send(line)

app = Application()
topic = app.topic(os.environ["INPUT_TOPIC"])
sdf = app.dataframe(topic)
questdb_sink = QuestDBSink()
sdf.sink(questdb_sink)
app.run(sdf)
