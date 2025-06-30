Step 1: Set Up Azure Event Hub

🔹1.1. Create a Resource Group
az group create --name CustomerFeedbackRG --location eastus
az group list --output table
az group show --resource-group CustomerFeedbackRG --output table

🔹1.2. Create an Event Hubs Namespace
az eventhubs namespace create --name FeedbackNamespace --resource-group CustomerFeedbackRG --location eastus --sku Standard

🔹1.3. Create an Event Hub
az eventhubs eventhub create --name FeedbackHub --resource-group CustomerFeedbackRG --namespace-name FeedbackNamespace

🔹1.4. Get Connection String (for RootManageSharedAccessKey)
az eventhubs namespace authorization-rule keys list \
  --resource-group CustomerFeedbackRG \
  --namespace-name FeedbackNamespace \
  --name RootManageSharedAccessKey

👉 Copy the connection string and save it somewhere (we'll use it in the Python script). 
    -- Keeping here itself :)


{
  "keyName": "RootManageSharedAccessKey",
  "primaryConnectionString": "Endpoint=sb://feedbacknamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=QgUpS19pIkJ6ORjAIlPSEAuwmrU4VCANp+AEhDR4X3Y=",
  "primaryKey": "QgUpS19pIkJ6ORjAIlPSEAuwmrU4VCANp+AEhDR4X3Y=",
  "secondaryConnectionString": "Endpoint=sb://feedbacknamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=B9u+vOtTdaHzSEPxzB04TvLV8RrKlhuTa+AEhMS5rI8=",
  "secondaryKey": "B9u+vOtTdaHzSEPxzB04TvLV8RrKlhuTa+AEhMS5rI8="
}

Step 2: Prepare the Avro Schema
Create a file called iphone_feedback_schema.avsc:

{
  "type": "record",
  "name": "IphoneFeedback",
  "fields": [
    { "name": "review_id", "type": "string" },
    { "name": "product_asin", "type": "string" },
    { "name": "variant_asin", "type": "string" },
    { "name": "country", "type": "string" },
    { "name": "review_title", "type": "string" },
    { "name": "review_description", "type": "string" },
    { "name": "rating_score", "type": "int" },
    { "name": "is_verified", "type": "boolean" },
    { "name": "review_date", "type": "string" },
    { "name": "timestamp", "type": "string" }
  ]
}


Step 3: Install Required Python Libraries
pip install pandas azure-eventhub fastavro


Step 4: Create Python Script – generate_feedback.py
import pandas as pd
import json
import uuid
import random
from datetime import datetime
from time import sleep
from azure.eventhub import EventHubProducerClient, EventData
from fastavro import writer, parse_schema
import io

# Load Avro Schema
with open('iphone_feedback_schema.avsc', 'r') as f:
    schema = json.load(f)
parsed_schema = parse_schema(schema)

# Load dataset
df = pd.read_csv('iphone.csv')
df = df.dropna(subset=['reviewDescription', 'ratingScore'])
df = df.sample(frac=1).reset_index(drop=True)  # Shuffle

# Setup Event Hub
CONNECTION_STR = '<<PASTE-YOUR-CONNECTION-STRING>>'
EVENTHUB_NAME = 'FeedbackHub'
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

# Stream data
for _, row in df.iterrows():
    review = {
        "review_id": str(uuid.uuid4()),
        "product_asin": row['productAsin'],
        "variant_asin": row['variantAsin'],
        "country": row['country'],
        "review_title": row['reviewTitle'],
        "review_description": row['reviewDescription'],
        "rating_score": int(row['ratingScore']),
        "is_verified": bool(row['isVerified']),
        "review_date": row['date'],
        "timestamp": datetime.utcnow().isoformat()
    }

    # Avro encode
    bytes_writer = io.BytesIO()
    writer(bytes_writer, parsed_schema, [review])
    avro_bytes = bytes_writer.getvalue()

    # Send to Event Hub
    event_data = EventData(body=avro_bytes)
    producer.send_batch([event_data])
    print(f"Sent review: {review['review_id']}")
    sleep(1)  # 1 second delay to simulate real-time

producer.close()

Replace this line:
CONNECTION_STR = '<<PASTE-YOUR-CONNECTION-STRING>>'


Step 5: Run the Script
python generate_feedback.py

✅ EXPECTED OUTCOME
You will see reviews being sent to Event Hub every second.

az monitor metrics list --resource CustomerFeedbackRG \
  --resource-type "Microsoft.EventHub/namespaces" \
  --name FeedbackNamespace --metric "IncomingMessages"
