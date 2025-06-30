import pandas as pd
import json
import time
import uuid
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
from fastavro import writer, parse_schema
from io import BytesIO

# Load Avro schema
import json
with open("feedback_schema.avsc", "r") as f:
    schema = parse_schema(json.load(f))

# Load data
df = pd.read_csv("iphone.csv")

# Initialize Event Hub
CONNECTION_STR = "add connection string"
EVENT_HUB_NAME = "FeedbackHub"
producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)

# Function to convert dict to Avro bytes
def to_avro_bytes(record, schema):
    buffer = BytesIO()
    writer(buffer, schema, [record])
    return buffer.getvalue()

# Send messages
for _, row in df.iterrows():
    feedback = {
        "review_id": str(uuid.uuid4()),
        "product_asin": str(row.get("productAsin", "")),
        "variant_asin": str(row.get("variantAsin", "")),
        "country": str(row.get("country", "")),
        "review_title": str(row.get("reviewTitle", "")),
        "review_description": str(row.get("reviewDescription", "")),
        "rating_score": int(row.get("ratingScore", 0)),
        "is_verified": str(row.get("isVerified", "")).lower() == "true",
        "review_date": str(row.get("date", "")),
        "timestamp": datetime.utcnow().isoformat()
    }


    avro_data = to_avro_bytes(feedback, schema)
    event_data = EventData(body=avro_data)

    with producer:
        producer.send_batch([event_data])
    
    print(f"Sent review_id: {feedback['review_id']}")
    time.sleep(1)  # Simulate real-time feed (1 message per second)
