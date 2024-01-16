import os
from quixstreams import Application, State

app = Application.Quix("transformation-v8", auto_offset_reset="latest")

input_topic = app.topic(os.environ["input"], value_deserializer='json')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)

#sdf = sdf.to_topic(output_topic)

sdf = sdf.update(lambda row: print(row))

if __name__ == "__main__":
    app.run(sdf)