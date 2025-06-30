# 1. Note that when pip install -r requirement.txt doesnt work install from cli

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




# 2. sample for the comprehension: if not all (field in order_data for field in required_fields):



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



# 3. using JMESPath: use [] for quering inside a dictionary inside a list 
# az eventhubs namespace list --query "[].geoDataReplication.locations[].locationName"
    [
  {
    "createdAt": "2025-06-29T15:47:57.0278467Z",
    "disableLocalAuth": false,
    "geoDataReplication": {
      "locations": [
        {
          "locationName": "eastus",
          "roleType": "Primary"
        }
      ],
      "maxReplicationLagDurationInSeconds": 0
    },
    "id": "/subscriptions/e5651ef5-919c-471f-80c6-d949378cbe80/resourceGroups/CustomerFeedbackRG/providers/Microsoft.EventHub/namespaces/FeedbackNamespace",
    "isAutoInflateEnabled": false,
    "kafkaEnabled": true,
    "location": "eastus",
    "maximumThroughputUnits": 0,
    "metricId": "e5651ef5-919c-471f-80c6-d949378cbe80:feedbacknamespace",
    "minimumTlsVersion": "1.2",
    "name": "FeedbackNamespace",
    "provisioningState": "Succeeded",
    "publicNetworkAccess": "Enabled",
    "resourceGroup": "CustomerFeedbackRG",
    "serviceBusEndpoint": "https://FeedbackNamespace.servicebus.windows.net:443/",
    "sku": {
      "capacity": 1,
      "name": "Basic",
      "tier": "Basic"
    },
    "status": "Active",
    "tags": {},
    "type": "Microsoft.EventHub/Namespaces",
    "updatedAt": "2025-06-29T15:48:22Z",
    "zoneRedundant": true
  }
]