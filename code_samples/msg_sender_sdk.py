```python
import json
import os
from azure.storage.queue import QueueClient, QueueMessage
from azure.core.exceptions import AzureError

# Get the connection string from environment variable
connection_string = os.environ.get("AzureWebJobsStorage")
queue_name = "order-queue"

try:
    # Initialize the QueueClient
    queue_client = QueueClient.from_connection_string(connection_string, queue_name)

    # Create the queue if it doesn't exist
    queue_client.create_queue()

    # Define the order data (simulating an order)
    order_data = {
        "order_id": "123",
        "customer_email": "user@example.com",
        "amount": 99.99
    }

    # Convert order data to JSON string
    message = json.dumps(order_data)

    # Send the message to the queue
    queue_client.send_message(message)
    print(f"Successfully sent message to queue: {message}")

except AzureError as e:
    print(f"Error interacting with Azure Storage Queue: {str(e)}")
except Exception as e:
    print(f"Unexpected error: {str(e)}")
```