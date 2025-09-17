from sentence_transformers import SentenceTransformer
from azure.storage.blob import BlobServiceClient, ContentSettings
import json
import chromadb
from chromadb.config import Settings
import torch
import os
import numpy as np # Import numpy
from torch.utils.data import Dataset, DataLoader

def setup_vector_db():
    client = chromadb.PersistentClient(path="./chromadb_data")
    try:
        collection = client.create_collection(name="amazon_berkeley_objects")
    except Exception as e: # Catch the exception if the collection already exists
        if "already exists" in str(e):
            collection = client.get_collection(name="amazon_berkeley_objects")
        else:
            raise e # Re-raise other exceptions
    return collection

class TextDataset(Dataset):
    def __init__(self, texts):
        self.texts = texts

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        return self.texts[idx]


device = "mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {device}")
blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=storefile1;AccountKey=k4B6SuLg7Vnd2QDYU/NeU9plKOLg5IAHB34YQxwVsZa7l/fbr9jhDXV94EJTItFpXnBjWa1sQXTJ+AStKf/M9g==;EndpointSuffix=core.windows.net")
container_client = blob_service_client.get_container_client("cleaned-object-data")
json_blobs = container_client.list_blobs(name_starts_with="listings_cleaned/")
model = SentenceTransformer('IshTale/MultiEccomerceEmbeddingModel')
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
            for line in f:  # Read line by line
                try:
                    data.append(json.loads(line))  # Load each line as a JSON object
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line in {filename}: {line.strip()}") # Handle potential errors
print(f"Loaded {len(data)} records from JSON files.")

text_dict = {item["main_image_id"]: item["text"] for item in data if "text" in item and "main_image_id" in item and item["text"].strip() and item["main_image_id"].strip()}

texts = list(text_dict.values())
ids = list(text_dict.keys())

if not texts: # Check if texts is empty
    print("No 'texts' found in the loaded data. Cannot proceed with encoding.")
else:
    print("Encoding texts...")
    embeddings = model.encode(
        texts,
        batch_size=128,
        convert_to_numpy=True,
        show_progress_bar=True,
        device=device,
    )

    # Save processed data and embeddings in chromadb using xcom pull
    collection = setup_vector_db()
    max_allowed_batch_size = 1000  # Define a maximum batch size to avoid memory issues
    for i in range(0, len(embeddings), max_allowed_batch_size):
        if i + max_allowed_batch_size > len(embeddings):
            max_allowed_batch_size = len(embeddings) - i
        batch_texts = texts[i:i + max_allowed_batch_size]
        batch_ids = ids[i:i + max_allowed_batch_size]
        batch_embeddings = embeddings[i:i + max_allowed_batch_size]
        collection.add(
            documents=batch_texts,
            embeddings=batch_embeddings.tolist(),  # Convert to list for JSON serialization
            ids=batch_ids
        )
    
    print("Data encoded and stored in vector database.")