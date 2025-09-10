from sentence_transformers import SentenceTransformer
import airflow
from airflow import DAG
from airflow.decorators import dag, task
from azure.storage.blob import BlobServiceClient, ContentSettings
import json
import chromadb

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
    'encode_embeddings',
    default_args=default_args,
    description='Download and process Amazon Berkeley Objects dataset',
    schedule="@daily",  
    start_date=datetime(2025, 8, 14),
    catchup=False,
)

@dag.task
def setup_vector_db():
    client = chromadb.Client()
    collection = client.create_collection(name="amazon_berkeley_objects")
    return collection

@dag.task
def encode_clean_data():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=storefile1;AccountKey=k4B6SuLg7Vnd2QDYU/NeU9plKOLg5IAHB34YQxwVsZa7l/fbr9jhDXV94EJTItFpXnBjWa1sQXTJ+AStKf/M9g==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("cleaned-object-data")
    json_blobs = container_client.list_blobs(name_starts_with="listings_cleaned/")
    os.makedirs("/tmp/cleaned_data", exist_ok=True)
    print("Downloading JSON files from Azure Blob Storage...")
    for blob in json_blobs:
        if blob.name.endswith('.json'):
            blob_client = container_client.get_blob_client(blob)
            download_file_path = os.path.join("/tmp/cleaned_data", os.path.basename(blob.name))
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
    
    # Load and process JSON files
    data = []
    for filename in os.listdir("/tmp/cleaned_data"):
        if filename.endswith('.json'):
            with open(os.path.join("/tmp/cleaned_data", filename), 'r') as f:
                data.extend(json.load(f))
    print(f"Loaded {len(data)} records from JSON files.")
    texts = [item['description'] for item in data if 'description' in item]
    print("Encoding texts...")
    embeddings = model.encode(texts, convert_to_tensor=True, show_progress_bar=True, device=device)
    # Save processed data and embeddings in chromadb using xcom pull
    collection = xcom_pull(task_ids='setup_vector_db')
    collection.add(documents=texts, embeddings=embeddings)
    print("Data encoded and stored in vector database.")
    return 
    