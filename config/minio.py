import os
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

client = Minio(
  endpoint=os.getenv("MINIO_ENDPOINT"),
  access_key=os.getenv("MINIO_ROOT_USER"),
  secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
  secure=False
)

BUCKET_NAME = "dados-marque-facil"


def conectar_minio():
    return client