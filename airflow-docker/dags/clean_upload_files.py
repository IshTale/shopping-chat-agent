from datetime import datetime, timedelta
import os
import glob
import json
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
import shutil
import boto3
import botocore
from airflow import DAG
from airflow.decorators import dag, task
import gzip
import csv
from azure.data.tables import TableServiceClient

s3_bucket_name = 'amazon-berkeley-objects'
s3 = boto3.client('s3', config=boto3.session.Config(signature_version=botocore.UNSIGNED))

# Azure Storage settings
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storefile1;AccountKey=k4B6SuLg7Vnd2QDYU/NeU9plKOLg5IAHB34YQxwVsZa7l/fbr9jhDXV94EJTItFpXnBjWa1sQXTJ+AStKf/M9g==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "cleaned-object-data"

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clean_upload_files',
    default_args=default_args,
    description='Download and process Amazon Berkeley Objects dataset',
    schedule="@daily",  
    start_date=datetime(2025, 8, 14),
    catchup=False,
)

@task(dag=dag)
def download_json_files():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    print("Starting the download and clean process...")
    logger.info("Starting the download and clean process...")
    
    # Test connection to the public bucket
    try:
        response = s3.list_objects_v2(
            Bucket=s3_bucket_name,
            Prefix='images/metadata/',
            MaxKeys=1
        )
        logger.info(f"Successfully listed bucket contents: {response}")
    except Exception as e:
        logger.error(f"Error accessing S3: {str(e)}")
        raise
    
    # Create temporary directory for downloads
    temp_dir = '/tmp/abo_data'
    os.makedirs(temp_dir, exist_ok=True)
    
    # Download metadata file
    metadata_key = 'images/metadata/images.csv.gz'
    local_metadata_path = os.path.join(temp_dir, 'images.csv.gz')
    
    try:
        print(f"Attempting to download metadata file from {metadata_key}")
        s3.download_file(s3_bucket_name, metadata_key, local_metadata_path)
        print("Successfully downloaded metadata file")
    except Exception as e:
        print(f"Error downloading metadata: {e}")
        return

    # Create blob client
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    blob_client = container_client.get_blob_client("metadata/metadata.csv.gz")
    with open(local_metadata_path, "rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/gzip")
        )

    # Download and process each JSON file
    try:
        response = s3.list_objects_v2(
            Bucket=s3_bucket_name,
            Prefix='listings/metadata/',
            MaxKeys=1
        )
        logger.info(f"Successfully listed bucket contents: {response}")
    except Exception as e:
        logger.error(f"Error accessing S3: {str(e)}")
        raise

    try:
        response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix='listings/metadata/')
        if 'Contents' not in response:
            logger.warning("No JSON files found in the specified S3 bucket.")
            return
        for obj in response['Contents']:
            print(f"Processing file: {obj['Key']}")
            if obj['Key'].endswith('.gz'):
                local_json_path = os.path.join(temp_dir, os.path.basename(obj['Key']))
                s3.download_file(s3_bucket_name, obj['Key'], local_json_path)
                # Upload to Azure Blob Storage
                blob_client = container_client.get_blob_client(f"listings/{os.path.basename(obj['Key'])}")
                with open(local_json_path, "rb") as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        content_settings=ContentSettings(content_type="application/gzip")
                    )
                logger.info(f"Uploaded {os.path.basename(obj['Key'])} to Azure Blob Storage")
    except Exception as e:
        logger.error(f"Error processing JSON files: {str(e)}")
        raise
    finally:    
        shutil.rmtree(temp_dir, ignore_errors=True)

@task(dag=dag)
def unzip_files():
    table_service_client = TableServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    # download metatadata file in temp directory
    metadata_blob = container_client.get_blob_client("metadata/metadata.csv.gz")
    with open('/tmp/metadata.csv.gz', "wb") as download_file:
        download_file.write(metadata_blob.download_blob().readall())
    gzip_file_path = '/tmp/metadata.csv.gz'
    with gzip.open(gzip_file_path, 'rb') as f_in:
        with open('/tmp/metadata.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("Metadata file downloaded and decompressed successfully.")
    # Add metadata csv to Azure Table Storage
    with open('/tmp/metadata.csv', 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    table_name = 'product_metadata'
    try:
        table_service_client.create_table(table_name)
    except Exception as e:
        print(f"Table {table_name} already exists or could not be created: {e}")
    table_client = table_service_client.get_table_client(table_name)
    # Insert rows into Azure Table Storage
    i = 0
    for row in rows:
        entity = {
            'PartitionKey': row['image_id'],  # Adjust based on your CSV structure
            'RowKey': i,
            'height': row['height'],  # Adjust based on your CSV structure
            'width': row['width'],  # Adjust based on your CSV structure
            'path' : row['path'],  # Adjust based on your CSV structure
            # Add more columns as needed
        }
        i += 1
        try:
            table_client.insert_entity(entity)
        except:
            continue
    # download json files in temp directory
    json_blobs = container_client.list_blobs(name_starts_with="listings/")
    os.makedirs('/tmp/listings', exist_ok=True)
    for blob in json_blobs:
        if blob.name.endswith('.json.gz'):
            json_blob = container_client.get_blob_client(blob)
            with open(f"/tmp/listings/{os.path.basename(blob.name)}", "wb") as download_file:
                download_file.write(json_blob.download_blob().readall())
    # Unzip json files
    json_files = glob.glob('/tmp/listings/*.json.gz')
    for json_file in json_files:
        with gzip.open(json_file, 'rb') as f_in:
            with open(json_file[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    print("JSON files downloaded and decompressed successfully.")
    # Upload unzipped files back to Azure Blob Storage
    for file in os.listdir('/tmp/listings'):
        if file.endswith('.json'):
            blob_client = container_client.get_blob_client(f"listings/{file}")
            with open(os.path.join('/tmp/listings', file), "rb") as data:
                blob_client.upload_blob(
                    data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type="application/json")
                )
            print(f"Uploaded {file} to Azure Blob Storage")
    shutil.rmtree('/tmp', ignore_errors=True)

@task(dag=dag)
def clean_json_files():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # Create temporary directory for downloads
    temp_dir = '/tmp/listings'
    os.makedirs(temp_dir, exist_ok=True)

    # Download JSON files from Azure Blob Storage
    json_blobs = container_client.list_blobs(name_starts_with="listings/")
    for blob in json_blobs:
        if blob.name.endswith('.json'):
            json_blob = container_client.get_blob_client(blob)
            with open(os.path.join(temp_dir, os.path.basename(blob.name)), "wb") as download_file:
                download_file.write(json_blob.download_blob().readall())

    # Clean JSON files
    json_files = glob.glob('/tmp/listings/*.json')
    for json_file in json_files:
        with open(json_file, 'r') as f:
            file_data = []
            try:
                # Try to load multiple JSON objects
                for line in f:
                    if line.strip():  # Skip empty lines
                        file_data.append(json.loads(line))
            except json.JSONDecodeError:
                # If the above fails, try loading as a single JSON
                f.seek(0)  # Reset file pointer to beginning
                file_data = [json.load(f)]
        
        # Process each JSON object in the file
        cleaned_objects = []
        for data in file_data:
            # Initialize cleaned data with required fields
            cleaned_data = {
                'main_image_id': data.get('main_image_id', ''),
                'item_id': data.get('item_id', ''),
                'domain_name': data.get('domain_name', '')
            }

            # Initialize combined text for this object
            combined_text = []

            # Fields to extract text from
            text_fields = {
                "bullet_point": "value",
                "color": "value",
                "fabric_type": "value",
                "finish_type": "value",
                "item_name": "value",
                "item_shape": "value",
                "material": "value",
                "model_name": "value",
                "model_number": "value",
                "product_description": "value",
                "pattern": "value",
                "style": "value"
            }

            # Extract text from each field
            for field, value_key in text_fields.items():
                if field in data:
                    if isinstance(data[field], list):
                        for item in data[field]:
                            if isinstance(item, dict) and value_key in item:
                                text = str(item[value_key])
                                # Clean the text
                                text = text.replace("\n", " ").replace("\r", "").replace("\t", " ")
                                text = text.replace("<p>", "").replace("</p>", "").replace('\\', '')
                                if text.strip():
                                    combined_text.append(text)

            # Add item dimensions if available
            if 'item_dimensions' in data:
                dims = data['item_dimensions']
                if isinstance(dims, dict):
                    dim_text = []
                    for dim_type in ['height', 'width', 'length']:
                        if dim_type in dims:
                            try:
                                value = dims[dim_type].get('value', '')
                                unit = dims[dim_type].get('unit', '')
                                if value and unit:
                                    dim_text.append(f"{dim_type}: {value} {unit}")
                            except (KeyError, AttributeError):
                                continue
                    if dim_text:
                        combined_text.append("Dimensions: " + ", ".join(dim_text))

            # Add item weight if available
            if 'item_weight' in data:
                weights = data['item_weight']
                if isinstance(weights, list):
                    for weight in weights:
                        try:
                            value = weight.get('value', '')
                            unit = weight.get('unit', '')
                            if value and unit:
                                combined_text.append(f"Weight: {value} {unit}")
                                break  # Only take the first valid weight
                        except (KeyError, AttributeError):
                            continue

            # Join all text pieces with periods
            cleaned_data['text'] = ". ".join(combined_text)
            cleaned_objects.append(cleaned_data)

        # Save all cleaned data objects back to file
        with open(json_file, 'w') as f:
            if len(cleaned_objects) == 1:
                # If there was only one object, store as a single JSON
                json.dump(cleaned_objects[0], f)
            else:
                # If there were multiple objects, store as JSONL
                for obj in cleaned_objects:
                    f.write(json.dumps(obj) + '\n')
        
        # Upload cleaned file back to Azure Blob Storage
        blob_client = container_client.get_blob_client(f"listings/cleaned/{os.path.basename(json_file)}")
        with open(json_file, "rb") as data:
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json")
            )
        print(f"Uploaded cleaned {os.path.basename(json_file)} to Azure Blob Storage")

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)

download_json_files() >> unzip_files() >> clean_json_files()
