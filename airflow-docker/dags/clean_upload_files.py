from datetime import datetime, timedelta
import os
import glob
import json
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
import shutil
import boto3
import botocore
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM 
import torch
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
    'retry_delay': timedelta(minutes=2),
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
    logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    logger.setLevel(logging.WARNING)
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
    logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    logger.setLevel(logging.WARNING)

    table_service_client = TableServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    # # download metatadata file in temp directory
    # metadata_blob = container_client.get_blob_client("metadata/metadata.csv.gz")
    # with open('/tmp/metadata.csv.gz', "wb") as download_file:
    #     download_file.write(metadata_blob.download_blob().readall())
    # gzip_file_path = '/tmp/metadata.csv.gz'
    # with gzip.open(gzip_file_path, 'rb') as f_in:
    #     with open('/tmp/metadata.csv', 'wb') as f_out:
    #         shutil.copyfileobj(f_in, f_out)
    # print("Metadata file downloaded and decompressed successfully.")
    # # Add metadata csv to Azure Table Storage
    # with open('/tmp/metadata.csv', 'r') as f:
    #     reader = csv.DictReader(f)
    #     rows = list(reader)
    
    # table_name = 'ProductMetadata'
    # try:
    #     table_service_client.create_table(table_name)
    # except Exception as e:
    #     print(f"Table {table_name} already exists or could not be created: {e}")
    # table_client = table_service_client.get_table_client(table_name)
    # # Insert rows into Azure Table Storage
    # i = 0
    # for row in rows:
    #     entity = {
    #         'PartitionKey': row['image_id'],  # Adjust based on your CSV structure
    #         'RowKey': "0",
    #         'height': row['height'],  # Adjust based on your CSV structure
    #         'width': row['width'],  # Adjust based on your CSV structure
    #         'path' : row['path'],  # Adjust based on your CSV structure
    #         # Add more columns as needed
    #     }
    #     try:
    #         table_client.create_entity(entity)
    #     except:
    #         continue
    #     finally:
    #         i += 1
    #         if i % 5000 == 0:
    #             print(f"Inserted {i} entities into the table...")
    # download json files in temp directory
    print("Downloading JSON files from Azure Blob Storage...")
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
    logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    logger.setLevel(logging.WARNING)

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # Create temporary directory for downloads
    temp_dir = '/tmp/listings'
    os.makedirs(temp_dir, exist_ok=True)
    print("Downloading JSON files from Azure Blob Storage for cleaning...")
    # Download JSON files from Azure Blob Storage
    json_blobs = container_client.list_blobs(name_starts_with="listings/")
    for blob in json_blobs:
        if blob.name.endswith('.json'):
            json_blob = container_client.get_blob_client(blob)
            with open(os.path.join(temp_dir, os.path.basename(blob.name)), "wb") as download_file:
                download_file.write(json_blob.download_blob().readall())
                print(f"Downloaded {blob.name} for cleaning.")

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
        print(file_data[0])
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
                                if isinstance(text, list):
                                    text = " ".join(map(str, text))  # Join list elements into a single string

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
            # Ensure `text` field is not empty
            if combined_text:
                cleaned_data['text'] = ". ".join(combined_text)
            else:
                # Add debugging logs to identify why `combined_text` is empty
                logger.warning(f"No text extracted for item_id: {data.get('item_id', 'unknown')}")
                # Fallback: Use other fields to populate `text`
                fallback_text = []
                for key in ['main_image_id', 'item_id', 'domain_name']:
                    if data.get(key):
                        fallback_text.append(f"{key}: {data[key]}")
                cleaned_data['text'] = ". ".join(fallback_text) if fallback_text else "No description available"
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
        blob_client = container_client.get_blob_client(f"listings_cleaned/{os.path.basename(json_file)}")
        with open(json_file, "rb") as data:
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json")
            )
        print(f"Uploaded cleaned {os.path.basename(json_file)} to Azure Blob Storage")

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)

@task(dag=dag)
def add_image_descriptions():
    table_service_client = TableServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    table_client = table_service_client.get_table_client('ProductMetadata')
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    logger.setLevel(logging.WARNING)


    print("Loading Florence-2 model and processor...")
    device = "cpu"
    model = AutoModelForCausalLM.from_pretrained("microsoft/Florence-2-large", trust_remote_code=True).to(device)
    processor = AutoProcessor.from_pretrained("microsoft/Florence-2-large", trust_remote_code=True)
    print("Model and processor loaded successfully.")

    # download JSON files in temp directory
    print("Downloading cleaned JSON files from Azure Blob Storage...")
    json_blobs = container_client.list_blobs(name_starts_with="listings_cleaned/")
    os.makedirs('/tmp/cleaned_listings', exist_ok=True)
    for blob in json_blobs:
        if blob.name.endswith('.json'):
            json_blob = container_client.get_blob_client(blob)
            with open(f"/tmp/cleaned_listings/{os.path.basename(blob.name)}", "wb") as download_file:
                download_file.write(json_blob.download_blob().readall())
    # Add image descriptions to cleaned json files
    print("Adding image descriptions to cleaned JSON files...")
    json_files = glob.glob('/tmp/cleaned_listings/*.json')
    print(f"Found {len(json_files)} cleaned JSON files to process for image descriptions.")
    i = 0
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
                f.seek(0)
                file_data = [json.load(f)]
        # Process each JSON object in the file
        for data in file_data:  
            image_id = data.get('main_image_id', '')
            image_path = ''
            if not image_id:
                continue
            # Fetch image path from Azure Table Storage
            try:
                entity = table_client.get_entity(partition_key=image_id, row_key="0")
                image_path = entity.get('path', '')
                image_path = "images/original/" + image_path
            except Exception as e:
                print(f"Error fetching metadata for image_id {image_id}: {e}")
                continue
            if not image_path:
                continue

            # Download image from S3
            local_image_path = f"/tmp/{image_id}.jpg"
            try:
                s3.download_file(s3_bucket_name, image_path, local_image_path)
            except Exception as e:
                print(f"Error downloading image {image_path} from S3: {e}")
                continue

            # Generate image description using Florence-2 model
            image = Image.open(local_image_path).convert("RGB")
            inputs = processor(images=image, return_tensors="pt", text="<MORE_DETAILED_CAPTION>").to(device)
            outputs = model.generate(**inputs, max_new_tokens=2048)
            generated_text = processor.batch_decode(outputs, skip_special_tokens=True)[0]
            parsed_answer = processor.post_process_generation(generated_text, task="<MORE_DETAILED_CAPTION>", image_size=(image.width, image.height))
            description = parsed_answer.get('<MORE_DETAILED_CAPTION>', '').strip()
            data['text'] += f". Image description: {description}"
            i += 1
            if i % 10 == 0:
                print(f"Processed {i} images for descriptions...")
            if os.path.exists(local_image_path):
                os.remove(local_image_path)

        # Save all updated data objects back to file
        with open(json_file, 'w') as f:
            if len(file_data) == 1:
                json.dump(file_data[0], f)
            else:
                for obj in file_data:
                    f.write(json.dumps(obj) + '\n')
        print(f"Updated file {os.path.basename(json_file)} with image descriptions.")
    
    # Upload updated files back to Azure Blob Storage
        for file in os.listdir('/tmp/cleaned_listings'):
            if file.endswith('.json'):
                blob_client = container_client.get_blob_client(f"listings_cleaned/{file}")
                with open(os.path.join('/tmp/cleaned_listings', file), "rb") as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        content_settings=ContentSettings(content_type="application/json")
                    )
                print(f"Uploaded {file} with descriptions to Azure Blob Storage")
    shutil.rmtree('/tmp/cleaned_listings', ignore_errors=True)


download_json_files() >> unzip_files() >> clean_json_files() >> add_image_descriptions()
