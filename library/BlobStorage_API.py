from azure.storage.blob import BlobServiceClient, BlobClient
# from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO
import pandas as pd
import json
import os
import logging

logging.getLogger('azure').setLevel(logging.ERROR)

def get_env():
    # Carrega as variáveis do .env
    from dotenv import load_dotenv

    # Tentar carregar .env da raiz do projeto
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_root, 'settings.ENV')
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        print('carregado .env da raiz do projeto')
    else:
        raise FileNotFoundError(f".env file not found at {env_path}")

class AzureBlobStorage:
    def __init__(self):
        get_env()
        # 
        RPA_SAS_URL = os.getenv("RPA_SAS_URL")
        print("RPA_SAS_URL: ", RPA_SAS_URL)
        if not RPA_SAS_URL:
            raise ValueError("RPA_SAS_URL environment variable is not set")
        
        try:
            # Extract the account URL and container name from the SAS URL
            if "?" in RPA_SAS_URL:
                base_url, sas_token = RPA_SAS_URL.split("?", 1)
                # Remove the container name from the base URL to get the account URL
                account_url = base_url.rsplit("/", 1)[0]
                self.container_name = base_url.split("/")[-1]
                self.sas_token = sas_token
                self.blob_service_client = BlobServiceClient(account_url=account_url, credential=self.sas_token)
                print(f"Successfully connected to Azure Blob Storage. Container: {self.container_name}")
            else:
                raise ValueError("Invalid SAS URL format")
        except Exception as e:
            print(f"Error connecting to Azure Blob Storage: {str(e)}")
            raise

    def add_file(self, buffer:BytesIO, blob_name:str):
        """
        Adds a file to the Azure Blob Storage container.
        
        Args:
            buffer (BytesIO): The file data to upload
            blob_name (str): The name of the blob (including folder path)
        """
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            data=buffer,
            overwrite=True
        )

    def upload_azure(self, name:str, data:pd.DataFrame|dict, extension='.parquet'):
        """
        Uploads data to Azure Blob Storage.
        
        Args:
            name (str): The name of the file (including folder path)
            data (pd.DataFrame|dict): The data to upload
            extension (str): The file extension ('.parquet' or '.json')
        """
        print("AzureBlobStorage.upload_azure: ", name)
        
        # Prepare the data based on extension
        if extension == ".parquet":
            buffer = BytesIO()
            data.to_parquet(
                path=buffer,
                engine='pyarrow',
                compression='gzip'
            )
            buffer = buffer.getvalue()
        elif extension == ".json":
            buffer = json.dumps(data)
        else:
            raise ValueError(f"Unsupported extension: {extension}")

        self.add_file(
            buffer=buffer,
            blob_name=name
        )

    def download_azure(self, blob_path: str) -> bytes:
        """
        Downloads a file from Azure Blob Storage and returns its bytes.
        
        Args:
            blob_path (str): The path to the blob in the format 'folder/filename'
            
        Returns:
            bytes: The contents of the downloaded file
            
        Raises:
            Exception: If the blob is not found or if there are other Azure storage errors
        """
        try:
            if not blob_path:
                raise ValueError("blob_path cannot be empty")
            
            # Since we're using a container-level SAS, we don't need the container name in the path
            blob_name = blob_path
            
            print(f"Attempting to download blob: {blob_name}")
            print(f"From container: {self.container_name}")
            
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            try:
                download_stream = blob_client.download_blob()
                return download_stream.readall()
            except Exception as e:
                if "BlobNotFound" in str(e):
                    raise Exception(f"Blob '{blob_name}' does not exist in container '{self.container_name}'")
                raise
            
        except Exception as e:
            print(f"Error downloading blob: {str(e)}")
            print(f"Full blob path attempted: {blob_path}")
            raise