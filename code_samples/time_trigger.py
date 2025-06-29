import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.data.tables import TableServiceClient # Import for Azure Table Storage
from datetime import datetime, timedelta
import os
import logging
import json # Import for JSON serialization of custom metadata

# Define the app instance for the Azure Functions Python V2 programming model
app = func.FunctionApp()

# --- Configuration Constants (fetched from environment variables for flexibility) ---
# These variables define the names of your storage containers and the timestamp blob.
# They are retrieved from environment variables (e.g., in local.settings.json or Azure portal app settings).
SOURCE_CONTAINER_NAME = os.environ.get("SOURCE_CONTAINER_NAME", "input") # Default to 'input' if not set
TARGET_CONTAINER_NAME = os.environ.get("TARGET_CONTAINER_NAME", "output") # Default to 'output' if not set
LAST_SCAN_TIMESTAMP_BLOB_NAME = os.environ.get("LAST_SCAN_TIMESTAMP_BLOB_NAME", "last_scan_timestamp.txt") # Default filename for the timestamp blob
METADATA_CONTAINER_NAME = os.environ.get("METADATA_CONTAINER_NAME", "function-metadata") # Container dedicated to storing timestamp blob
TABLE_NAME = os.environ.get("TABLE_NAME", "BlobMetadataTable") # Name of the Azure Table to store metadata

# Register the function with the app instance and define its trigger
@app.function_name(name="BlobScannerFunction") # Logical name for this function within the app
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer") # Timer trigger: "0 * * * * *" runs every 1 minute
def blob_scanner_function(myTimer: func.TimerRequest):
    """
    This Azure Function scans a specified source container for blobs
    that have been uploaded or modified since the last scan and copies them
    to a target container. It also extracts metadata from new/modified files
    and appends it to an Azure Table Storage.
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
    
    # Initialize the TableServiceClient to interact with Azure Table Storage.
    table_service_client = TableServiceClient.from_connection_string(connection_string)
    table_client = None # Initialize table_client to None

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

    # --- 2. Get Container Clients for Source and Target & Initialize Table Client ---
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
            
    # Get client for the metadata table and attempt to create it
    try:
        table_client = table_service_client.get_table_client(table_name=TABLE_NAME)
        table_client.create_table()
        logging.info(f"Created Azure Table: {TABLE_NAME}")
    except Exception as e:
        if "TableAlreadyExists" not in str(e): # Ignore if it already exists
            logging.warning(f"Failed to create Azure Table (might already exist): {e}")
        else:
            logging.info(f"Azure Table '{TABLE_NAME}' already exists.")


    copied_count = 0 # Initialize a counter for successfully copied files
    metadata_processed_count = 0 # Initialize a counter for processed metadata entries

    # --- 3. List Blobs in Source Container, Copy New/Modified Files, and Store Metadata ---
    # This block iterates through all blobs in the source container
    try:
        for blob_item in source_container_client.list_blobs():
            # Check if the blob's last modified time is newer than the last scan time.
            # We use 'last_modified' as it reflects uploads and updates.
            if blob_item.last_modified and blob_item.last_modified > last_scan_time:
                # --- Copy Blob (Existing Logic) ---
                source_blob_client = source_container_client.get_blob_client(blob_item.name)
                target_blob_client = target_container_client.get_blob_client(blob_item.name)

                source_blob_sas_url = source_blob_client.url + "?" + generate_blob_sas(
                    account_name=blob_service_client.account_name,
                    container_name=source_container_client.container_name,
                    blob_name=blob_item.name,
                    account_key=blob_service_client.credential.account_key, 
                    permission=BlobSasPermissions(read=True), 
                    expiry=utc_now + timedelta(hours=1) 
                )

                logging.info(f"Copying '{blob_item.name}' (Last Modified: {blob_item.last_modified.isoformat()})...")
                copy_result = target_blob_client.start_copy_from_url(source_blob_sas_url)
                copied_count += 1

                # --- Extract and Store Metadata ---
                if table_client: # Ensure table_client was successfully initialized
                    try:
                        # Construct the entity for Azure Table Storage
                        # PartitionKey and RowKey are mandatory
                        # RowKey should be unique within a PartitionKey
                        entity = {
                            "PartitionKey": SOURCE_CONTAINER_NAME, # Using source container name as PartitionKey
                            "RowKey": blob_item.name.replace("/", "---"), # Blob name as RowKey, replace '/' for validity
                            "BlobName": blob_item.name,
                            "BlobSize": blob_item.size,
                            "LastModified": blob_item.last_modified.isoformat() if blob_item.last_modified else None,
                            "CreationTime": blob_item.creation_time.isoformat() if blob_item.creation_time else None,
                            "ETag": blob_item.etag,
                            "BlobType": str(blob_item.blob_type), # Convert enum to string
                            "ContentType": blob_item.content_settings.content_type if blob_item.content_settings else None,
                            "ContentMD5": blob_item.content_settings.content_md5 if blob_item.content_settings else None,
                            "ContentEncoding": blob_item.content_settings.content_encoding if blob_item.content_settings else None,
                            "ContentDisposition": blob_item.content_settings.content_disposition if blob_item.content_settings else None,
                            "ContentLanguage": blob_item.content_settings.content_language if blob_item.content_settings else None,
                            "CacheControl": blob_item.content_settings.cache_control if blob_item.content_settings else None,
                            "CustomMetadata": json.dumps(blob_item.metadata) if blob_item.metadata else "{}" # Serialize custom metadata dictionary to JSON string
                        }
                        
                        # Upsert the entity: inserts if not exists, updates if exists.
                        table_client.upsert_entity(entity=entity)
                        metadata_processed_count += 1
                        logging.info(f"Metadata for '{blob_item.name}' upserted to Azure Table '{TABLE_NAME}'.")

                    except Exception as table_e:
                        logging.error(f"Error processing metadata for '{blob_item.name}': {table_e}")
                else:
                    logging.warning(f"Table client not initialized. Skipping metadata storage for '{blob_item.name}'.")
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
        logging.info(f"Updated last scan time to: {new_scan_time.isoformat()}. Copied {copied_count} new files. Processed {metadata_processed_count} metadata entries.")
    except Exception as e:
        # Catch and log any errors during the timestamp update.
        logging.error(f"Error updating last scan timestamp: {e}")

