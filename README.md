Study Plan

Storage Account
SQL Database
CosmosDB
Data Factory
Synapse
Databricks
K8s
Container
Functions
Logic Apps
Service bus
Event Hub
Event Grid





Storage Account

Use case 1:
1. Create two conatiners in ADLS
2. Load csv files to one of the containers
3. Use python and convert files to avro and store it in a second container

Use case 2:
1. Create two conatiners in ADLS
2. Load parquet files to one of the containers
3. Read parquet from ADLS using Pandas

Use case 3:
1. Create two containers in Blob storage
2. Load avro files to one of the containers
3. Read avro from the blob storage

Use case 4:
1. Inserting CSV with config details into storage tables
2. Access storage table to retrive this data in run time

Use case 5:
1. insert a auto generated output into storage queue
2. retrive the storage queue

# How to get the storage account key as a varaiable and how to use it to upload file
# Get the storage account key
account_key=$(az storage account keys list \
  --resource-group rx \
  --account-name rxstorageac1 \
  --query '[0].value' -o tsv)

# Upload the CSV
az storage blob upload \
  --account-name rxstorageac \
  --account-key "$account_key" \
  --container-name input \
  --file /path/to/your.csv \
  --name your.csv

# Get username of the account
 az account show --query "user.name" --output tsv

Use case 6: 
1. load a file to blob container
2. This triggers a azure function
3. Function takes the bloab and processes it
4. Is loaded to another container


# detailed steps for use case 6
# 1. Create the containers:
az storage container create --account-name rxstorageac1 --name input
az storage container create --account-name rxstorageac1 --name output

# 2. Install Azure function core tools so that commands like "func" work

# 3. Create a Python Azure Function Project
func init file-processor-func --python

# 4. Go to the Azure function project folder named "file-processor-func" and create a function triggered by blob
func new --name process_file --template "Azure Blob Storage trigger" --authlevel "function"