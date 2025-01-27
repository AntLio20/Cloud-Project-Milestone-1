import json
import glob
import os
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub

# Set up Google Pub/Sub
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
project_id = "NULL"
subscription_id = "labels-topic-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")

# Callback to process received messages
def callback(message):
    try:
        # Deserialize message data
        message_data = json.loads(message.data.decode("utf-8"))
        print(f"Consumed record: {message_data}")
        message.ack()  # Report To Google Pub/Sub a successful process of the received message
    except Exception as e:
        print(f"Error processing message: {e}")

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()  # Block indefinitely to receive messages
    except KeyboardInterrupt:
        streaming_pull_future.cancel()  # Stop the listener