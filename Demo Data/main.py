import time
import uuid
from random import randint, random, choice
from time import sleep

from dotenv import load_dotenv

from quixstreams.kafka import Producer
from quixstreams.models.serializers import (
    QuixTimeseriesSerializer,
    SerializationContext,
)
from quixstreams.platforms.quix import QuixKafkaConfigsBuilder, TopicCreationConfigs
import os
import json
cfg_builder = QuixKafkaConfigsBuilder()
cfgs, topics, _ = cfg_builder.get_confluent_client_configs([os.environ["output"]])
topic = topics[0]
cfg_builder.create_topics([TopicCreationConfigs(name=topic)])

with Producer(
    broker_address=cfgs.pop("bootstrap.servers"), extra_config=cfgs
) as producer:
  
  
    published_rows = 0


    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    print("File loaded.")

    headers = df.columns.tolist()

    total_rows = len(df) * iterations
    published_rows = 0
    n_percent = float(total_rows / 100) * update_pct

    print(f"Publishing {total_rows} rows. (Expect an update every {update_pct}% ({int(n_percent)} rows).")

    if keep_timing:
        print("note: Delays greater than 1 second will be reduced to 1 second for this demo.")
    else:
        print("note: Timing of the original data is being ignored.")


    # Iterate over the rows and send them to the API
    for index, row in df.iterrows():

        # If shutdown has been requested, exit the loop.
        if shutting_down:
            break

        # Create a dictionary that includes both column headers and row values
        row_data = {header: row[header] for header in headers}

        producer.produce(
            topic=topic,
            headers=headers,
            key=account_id,
            value=json.dumps(row_data)

        # Increment the number of published rows
        published_rows += 1
        if int(published_rows % n_percent) == 0:
            print(f"{int(100 * float(published_rows) / float(total_rows))}% published")

        # Delay sending the next row if it exists
        # The delay is calculated using the original timestamps and ensure the data 
        # is published at a rate similar to the original data rates
        if keep_timing and index + 1 < len(df):
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



# Run this method before shutting down.
# In this case we set a flag to tell the loops to exit gracefully.
def before_shutdown():
    global shutting_down
    print("Shutting down")

    # set the flag to True to stop the loops as soon as possible.
    shutting_down = True

# keep the app running and handle termination signals.
qx.App.run(before_shutdown = before_shutdown)