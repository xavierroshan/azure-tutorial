import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import os
import logging

# Define the app instance for the Azure Functions Python V2 programming model
app = func.FunctionApp()

# --- Configuration Constants (fetched from environment variables for flexibility) ---
# These variables define the names of your storage containers and the timestamp blob.
# They are retrieved from environment variables (e.g., in local.settings.json or Azure portal app settings).
SOURCE_CONTAINER_NAME = os.environ.get("SOURCE_CONTAINER_NAME", "input") # Default to 'input' if not set
TARGET_CONTAINER_NAME = os.environ.get("TARGET_CONTAINER_NAME", "output") # Default to 'output' if not set
LAST_SCAN_TIMESTAMP_BLOB_NAME = os.environ.get("LAST_SCAN_TIMESTAMP_BLOB_NAME", "last_scan_timestamp.txt") # Default filename for the timestamp blob
METADATA_CONTAINER_NAME = os.environ.get("METADATA_CONTAINER_NAME", "function-metadata") # Container dedicated to storing metadata like the timestamp blob

# Register the function with the app instance and define its trigger
@app.function_name(name="BlobScannerFunction") # Logical name for this function within the app
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer") # Timer trigger: "0 * * * * *" runs every 1 minute
def blob_scanner_function(myTimer: func.TimerRequest):
    """
    This Azure Function scans a specified source container for blobs
    that have been uploaded or modified since the last scan and copies them
    to a target container.
    """
    
    # Capture the current UTC time when the function starts.
    # This will be used as the new 'last scan time' after this run.
    utc_now = datetime.utcnow()
    logging.info(f"BlobScannerFunction triggered at {utc_now.isoformat()}")

    # Retrieve the Azure Storage connection string from environment variables.
    # This is typically 'AzureWebJobsStorage' for Azure Functions.
    connection_string = os.environ["AzureWebJobsStorage"]
    
    # Initialize the BlobServiceClient to interact with the Azure Blob Storage account.
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Initialize last_scan_time to the minimum possible datetime.
    # This ensures that on the very first run (when no timestamp blob exists),
    # all existing blobs in the source container will be considered 'new' and copied.
    last_scan_time = datetime.min 
    
    # --- 1. Retrieve the Last Scan Timestamp from Storage ---
    # This block tries to read the timestamp of the previous successful scan
    # from a dedicated blob in the metadata container.
    try:
        # Get a client for the metadata container.
        metadata_container_client = blob_service_client.get_container_client(METADATA_CONTAINER_NAME)
        
        # Attempt to create the metadata container. If it already exists,
        # a 'ContainerAlreadyExists' error will occur, which we can safely ignore.
        try:
            metadata_container_client.create_container()
            logging.info(f"Created metadata container: {METADATA_CONTAINER_NAME}")
        except Exception as e:
            # Check if the error is due to the container already existing
            if "ContainerAlreadyExists" not in str(e): 
                logging.warning(f"Failed to create metadata container (might already exist): {e}")

        # Get a client for the specific blob that stores the timestamp.
        timestamp_blob_client = metadata_container_client.get_blob_client(LAST_SCAN_TIMESTAMP_BLOB_NAME)

        # Check if the timestamp blob exists.
        if timestamp_blob_client.exists():
            # If it exists, download its content (which is the timestamp string)
            timestamp_content = timestamp_blob_client.download_blob().readall().decode('utf-8')
            try:
                # Attempt to parse the timestamp string back into a datetime object.
                last_scan_time = datetime.fromisoformat(timestamp_content)
                logging.info(f"Last scan time retrieved: {last_scan_time.isoformat()}")
            except ValueError:
                # If parsing fails (e.g., malformed timestamp), log a warning
                # and proceed with datetime.min, causing a full scan.
                logging.warning(f"Invalid timestamp format in '{LAST_SCAN_TIMESTAMP_BLOB_NAME}': {timestamp_content}. Starting from minimum time.")
        else:
            # If the timestamp blob does not exist (first run), log it.
            logging.info(f"'{LAST_SCAN_TIMESTAMP_BLOB_NAME}' not found. Starting scan from minimum time.")

    except Exception as e:
        # Catch any other errors during timestamp retrieval and log them.
        # The function will still proceed, scanning from datetime.min.
        logging.error(f"Error retrieving last scan timestamp: {e}")
        last_scan_time = datetime.min # Fallback to min time to ensure all files are scanned

    # --- 2. Get Container Clients for Source and Target ---
    # Get clients for the source and target blob containers.
    source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER_NAME)
    target_container_client = blob_service_client.get_container_client(TARGET_CONTAINER_NAME)

    # Attempt to create the target container. This ensures it exists before copying.
    try:
        target_container_client.create_container()
        logging.info(f"Created target container: {TARGET_CONTAINER_NAME}")
    except Exception as e:
        if "ContainerAlreadyExists" not in str(e): 
            logging.warning(f"Failed to create target container (might already exist): {e}")

    copied_count = 0 # Initialize a counter for successfully copied files

    # --- 3. List Blobs in Source Container and Copy New/Modified Files ---
    # This block iterates through all blobs in the source container
    try:
        for blob_item in source_container_client.list_blobs():
            # Check if the blob's last modified time is newer than the last scan time.
            # We use 'last_modified' as it reflects uploads and updates.
            if blob_item.last_modified and blob_item.last_modified > last_scan_time:
                # Get blob clients for both the source and target blobs.
                # blob_item.name gives the full blob path (e.g., 'folder/file.txt').
                source_blob_client = source_container_client.get_blob_client(blob_item.name)
                target_blob_client = target_container_client.get_blob_client(blob_item.name)

                # Generate a Shared Access Signature (SAS) token for the source blob.
                # This SAS token grants temporary read access to the source blob's URL,
                # which is required by the 'start_copy_from_url' operation.
                # IMPORTANT: This assumes the BlobServiceClient was initialized with an account key.
                # For enhanced security in production, consider Azure Managed Identities.
                source_blob_sas_url = source_blob_client.url + "?" + generate_blob_sas(
                    account_name=blob_service_client.account_name,
                    container_name=source_container_client.container_name,
                    blob_name=blob_item.name,
                    account_key=blob_service_client.credential.account_key, # Access the account key from the client's credential
                    permission=BlobSasPermissions(read=True), # Grant read permission
                    expiry=utc_now + timedelta(hours=1) # SAS token valid for 1 hour from now
                )

                logging.info(f"Copying '{blob_item.name}' (Last Modified: {blob_item.last_modified.isoformat()})...")
                
                # Initiate the asynchronous copy operation from the source blob's SAS URL to the target blob.
                copy_result = target_blob_client.start_copy_from_url(source_blob_sas_url)
                
                # In a more robust application, you might add logic here to poll
                # copy_result.status or copy_result.id to ensure the copy completes successfully.
                copied_count += 1
            else:
                logging.info(f"Skipping '{blob_item.name}' (Last Modified: {blob_item.last_modified.isoformat()}) - not newer than last scan time.")

    except Exception as e:
        # Catch and log any errors that occur during the listing or copying process.
        logging.error(f"Error listing or copying blobs: {e}")

    # --- 4. Update the Last Scan Timestamp in Storage ---
    # This block updates the timestamp blob with the current time (utc_now),
    # so the next function execution knows from when to start scanning.
    try:
        new_scan_time = utc_now 
        # Upload the current UTC time (in ISO format) to the timestamp blob, overwriting previous value.
        timestamp_blob_client.upload_blob(new_scan_time.isoformat(), overwrite=True)
        logging.info(f"Updated last scan time to: {new_scan_time.isoformat()}. Copied {copied_count} new files.")
    except Exception as e:
        # Catch and log any errors during the timestamp update.
        logging.error(f"Error updating last scan timestamp: {e}")

