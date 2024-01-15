import time

from quixstreams.kafka import Producer
from quixstreams.platforms.quix import QuixKafkaConfigsBuilder, TopicCreationConfigs
import os
import json
cfg_builder = QuixKafkaConfigsBuilder()
cfgs, topics, _ = cfg_builder.get_confluent_client_configs([os.environ["Topic"]])
topic = topics[0]
cfg_builder.create_topics([TopicCreationConfigs(name=topic)])

import pandas as pd
import datetime


with Producer(
    broker_address=cfgs.pop("bootstrap.servers"), extra_config=cfgs
) as producer:
  
  
    published_rows = 0


    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv("demo-data.csv")


    print("File loaded.")

    df = df.rename(columns={"Timestamp": "original_timestamp"})

    headers = df.columns.tolist()

    # Iterate over the rows and send them to the API
    for index, row in df.iterrows():

        # Create a dictionary that includes both column headers and row values
        row_data = {header: row[header] for header in headers}

        row_data["Timestamp"] = time.time()

        producer.produce(
            topic=topic,
            key="f1-data",
            value=json.dumps(row_data))

        print(json.dumps(row_data))

        producer.flush()

        print(str(datetime.datetime.fromtimestamp(row_data["Timestamp"])) + " - row sent")


        if index + 1 < len(df):
            current_timestamp = pd.to_datetime(row['original_timestamp'])
            next_timestamp = pd.to_datetime(df.at[index + 1, 'original_timestamp'])
            time_difference = next_timestamp - current_timestamp
            delay_seconds = time_difference.total_seconds()

            # Cater for negative sleep values
            if delay_seconds < 0:
                delay_seconds = 0
                
            # For this demo, if the delay is greater than 1 second, just delay for 1 second
            if delay_seconds > 1:
                # Uncomment this line if you want to know when a delay is shortened
                # print(f"Skipping long delay of {delay_seconds} between timestamps.")
                delay_seconds = 1

            time.sleep(delay_seconds)


