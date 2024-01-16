import os
from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer


app = Application.Quix("transformation-v8", auto_offset_reset="latest")

input_topic = app.topic(os.environ["input"], value_deserializer='json')
output_topic = app.topic(os.environ["output"], value_serializer='json')

sdf = app.dataframe(input_topic)
sdf = sdf[sdf.contains("Speed")]

def reduce_speed(state: dict, val: float) -> dict:
    print(state)
    print(val)
    state["last"] = val
    state["min"] = min(state["min"], val) 
    state["max"] = max(state["max"], val) 

    return state

def init_reduce_speed(val: float) -> dict:
    
    return {
        "last" : val,
        "first": val,
        "max": val,
        "min": val
    }

sdf = sdf.apply(lambda val: val["Speed"]).tumbling_window(10, 0).reduce(reduce_speed, init_reduce_speed).final()

sdf = sdf.update(lambda row: print(row))

#sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)