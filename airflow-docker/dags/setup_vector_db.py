from datetime import datetime, timedelta
import os
import json
from airflow import DAG
from airflow.decorators import dag, task
from azure.storage.blob import BlobServiceClient, ContentSettings
import chromadb
from chromadb.utils import embedding_functions
from chromadb.config import Settings

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
    'setup_vector_db',
    default_args=default_args,
    description='Set up and populate Chroma vector database',
    schedule="@once",  # Run once
    start_date=datetime(2025, 8, 16),
    catchup=False,
)

@task(dag=dag)
def setup_chroma():
    # Initialize ChromaDB with persistence
    chroma_client = chromadb.Client(Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory="/opt/airflow/chroma_db"
    ))
    
    # Create a collection for product data
    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )
    
    # Create or get collection
    collection = chroma_client.create_collection(
        name="product_descriptions",
        embedding_function=embedding_function,
        metadata={"description": "Product descriptions and metadata from Amazon Berkeley Objects dataset"}
    )
    
    return "Collection created successfully"

@task(dag=dag)
def load_data_to_chroma():
    # Initialize ChromaDB client
    chroma_client = chromadb.Client(Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory="/opt/airflow/chroma_db"
    ))
    
    collection = chroma_client.get_collection(
        name="product_descriptions",
        embedding_function=embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )
    )
    
    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # Download and process cleaned JSON files
    blobs = container_client.list_blobs(name_starts_with="listings/cleaned/")
    
    documents = []
    metadatas = []
    ids = []
    
    for blob in blobs:
        if not blob.name.endswith('.json'):
            continue
            
        # Download JSON content
        blob_client = container_client.get_blob_client(blob.name)
        json_content = blob_client.download_blob().readall()
        product_data = json.loads(json_content)
        
        # Create combined text for embedding
        combined_text = ""
        if 'item_name' in product_data:
            combined_text += product_data['item_name'] + ". "
        if 'product_description' in product_data:
            combined_text += product_data['product_description'] + ". "
        
        if combined_text:
            documents.append(combined_text)
            metadatas.append(product_data)
            ids.append(blob.name)
        
        # Batch insert every 100 documents
        if len(documents) >= 100:
            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            documents = []
            metadatas = []
            ids = []
    
    # Insert any remaining documents
    if documents:
        collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )
    
    return f"Loaded {collection.count()} documents into ChromaDB"

# Define task dependencies
setup_chroma() >> load_data_to_chroma()
