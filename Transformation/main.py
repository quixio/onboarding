import os
from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer


app = Application.Quix("transformation-v4", auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"], value_deserializer='bytes')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)

# Here put transformation logic.

sdf = sdf.update(lambda row: print(row.decode()))

#sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)