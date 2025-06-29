import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import logging

# define the app instance
app = func.FunctionApp() # This line defines the app instance

# registers the function with the app
@app.function_name(name="process_file") 
# defines the trigger
@app.blob_trigger(arg_name="myblob", path="input/{name}", connection="AzureWebJobsStorage")
# defines the function that gets triggered. myblob is passes as func.InputStream
def process_file(myblob: func.InputStream):
    # reads the metadata and content of the blob
    logging.info(f"Blob name: {myblob.name}")
    logging.info(f"Blob length: {myblob.length}")
    
    # Extract only the filename from the full blob path
    # For example, if myblob.name is "input/my_file.txt", this will get "my_file.txt"
    file_name_only = os.path.basename(myblob.name)
    logging.info(f"Processing file: {file_name_only}")

    file_ext = os.path.splitext(file_name_only)[1].lower() # Use file_name_only for extension check
    
    #initialize the blob service clinet using the connection string from settings file
    blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    #Define source and target container 
    source_container_name = "input" # Not directly used for copying, but good for context
    target_container_name = "output"
   
    try:
        # Get the client for the target container
        target_container_client = blob_service_client.get_container_client(target_container_name)
        
        # Get the blob client for the target file, using only the filename
        target_blob_client = target_container_client.get_blob_client(file_name_only)
        
        # Read content based on file type and upload
        if file_ext in ['.txt', '.csv', '.json']:
            data = myblob.read().decode('utf-8')
            logging.info(f"Content preview: {data[:100]}")
            target_blob_client.upload_blob(data.encode('UTF-8'), overwrite=True)
            logging.info(f"Successfully uploaded text blob '{file_name_only}' to '{target_container_name}' container.")
        else:
            data = myblob.read()
            target_blob_client.upload_blob(data, overwrite=True)
            logging.info(f"Successfully uploaded binary blob '{file_name_only}' to '{target_container_name}' container.")

    except Exception as e:
        logging.error(f"Error copying blob: {e}")
