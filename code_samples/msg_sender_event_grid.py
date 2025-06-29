"""Step-by-Step: Add Event Grid Subscription on Blob Upload
🔹 1. Prerequisites
A Function App with a deployed function using @event_grid_trigger
A Storage Account with a container (e.g., input)
Function App should be publicly accessible (not behind a VNET)
Azure CLI installed and logged in

🔹 2. Create Event Grid Subscription via Azure CLI
Replace the placeholders with your values:

# Variables
STORAGE_ACCOUNT="<your-storage-account-name>"
RESOURCE_GROUP="<your-resource-group>"
SUBSCRIPTION_NAME="blob-event-subscription"
CONTAINER_NAME="input"
FUNCTION_APP_NAME="<your-function-app-name>"
FUNCTION_NAME="process_blob_event"  # Must match @function_name in code
REGION="eastus"  # or your region

# Get the blob service endpoint for Event Grid
STORAGE_ID=$(az storage account show \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query "id" -o tsv)

# Get Function App callback URL
FUNCTION_ENDPOINT=$(az functionapp function show \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --function-name $FUNCTION_NAME \
    --query "invokeUrlTemplate" -o tsv)

# Create the Event Grid subscription
az eventgrid event-subscription create \
    --name $SUBSCRIPTION_NAME \
    --source-resource-id $STORAGE_ID \
    --endpoint-type azurefunction \
    --endpoint $FUNCTION_ENDPOINT \
    --included-event-types Microsoft.Storage.BlobCreated \
    --subject-begins-with "/blobServices/default/containers/$CONTAINER_NAME/" \
    --subject-ends-with ".csv" \
    --event-delivery-schema eventgridschema
📂 What This Does
Subscribes to BlobCreated events (only .csv files in input/ container).
Sends those events to your Azure Function named process_blob_event.

✅ Final Order of Setup
Deploy the function with the code (provided below)
Create the Event Grid subscription using the above command
Upload a .csv file to input/ container
Azure triggers the function, which sends blob info to the queue"""


import azure.functions as func
import json
import logging

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="event")
@app.queue_output(arg_name="outputQueue", queue_name="order-queue", connection="AzureWebJobsStorage")
def process_blob_event(event: func.EventGridEvent, outputQueue: func.Out[str]):
    try:
        event_data = event.get_json()

        if event.event_type == "Microsoft.Storage.BlobCreated":
            blob_url = event_data.get("url")
            subject = event.subject

            message = json.dumps({
                "blob_url": blob_url,
                "event_id": event.id,
                "event_type": event.event_type,
                "subject": subject
            })

            outputQueue.set(message)
            logging.info(f"[✓] Blob Created Event - Sent message to queue: {message}")
        else:
            logging.info(f"[i] Skipped event type: {event.event_type}")

    except Exception as e:
        logging.error(f"[✗] Failed to process Event Grid event: {str(e)}")


