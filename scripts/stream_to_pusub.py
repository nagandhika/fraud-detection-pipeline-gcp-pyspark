import time
import json
import pandas as pd
from google.cloud import pubsub_v1

PROJECT_ID = "vaulted-sector-461110-b7"
TOPIC_ID = "fraud-transactions"
CSV_FILE = "onlinefraud.csv"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

df = pd.read_csv(CSV_FILE)
print(f"📦 Loaded {len(df)} transactions from {CSV_FILE}")

for index, row in df.iterrows():
    message = row.to_dict()
    message_json = json.dumps(message).encode("utf-8")
    publisher.publish(topic_path, message_json)
    print(f"✅ Published row {index + 1}")
    time.sleep(0.5)

print("🎉 All data published to Pub/Sub!")
