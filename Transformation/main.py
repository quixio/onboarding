import os
from quixstreams import Application, State
import uuid
from datetime import datetime

app = Application.Quix(str(uuid.uuid4()), auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"], value_deserializer='json')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)

sdf = sdf[["Timestamp", "Brake"]]
sdf = sdf[sdf["Brake"].notnull()]

sdf = sdf.apply(lambda row: row["Brake"]).hopping_window(1000, 200).mean().final()

sdf = sdf[sdf["value"] > 0.5]

sdf = sdf.apply(lambda row: {
    "Timestamp": row["start"],
    "Alert": {
        "Message": "Hard braking detected"
    }
})

#sdf = sdf.to_topic(output_topic)

sdf = sdf.update(lambda row: print(row))

if __name__ == "__main__":
    app.run(sdf)