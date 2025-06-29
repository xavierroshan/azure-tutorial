import fastavro 
import pandas
import pyarrow
from azure.eventhub import EventHubConsumerClient  # Example for Event Hubs
import azure.functions as func  # For Azure Functions
from azure.storage.blob import BlobServiceClient  # For Blob Storage
import cloudpickle
import flask
import apache_beam 
import airflow 








order_data = {"order_id": "123", "customer_email": "user@example.com", "amount": 99.99}
required_fields = ["order_id", "customer_email", "amount"]

# Normal way of writting code
missing_field = []

for x in required_fields:
    if x in order_data:
        pass
    else:
        missing_field.append(x)

if missing_field:
    print("missing field")
else:
    print("no missing field")


# Pythonic way of writing code
if not all (field in order_data for field in required_fields):
    print("missing field")
else:
    print ("no missing field")