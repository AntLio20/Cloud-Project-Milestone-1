import csv
import glob
import json
import os
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub

# Set up Google Pub/Sub
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
project_id = "NULL"
topic_name = "labels-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing messages to {topic_path}...")

# Read and publish messages from Labels.csv
with open("Labels.csv", mode="r") as csv_file:
    file_reader = csv.DictReader(csv_file)  
    for row in file_reader:
        # Serialize each row to JSON
        message_data = json.dumps(row).encode("utf-8")
        try:
            # Publish the message
            future = publisher.publish(topic_path, message_data)
            future.result()  
            print(f"Published: {row}")
        except Exception as e:
            print(f"Failed to publish message: {row}, Error: {e}")
