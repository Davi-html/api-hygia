import os
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

client = Minio(
  os.getenv("MINIO_ENDPOINT"),
  access_key=os.getenv("MINIO_ACCESS_KEY"),
  secret_key=os.getenv("MINIO_SECRET_KEY"),
  secure=False
)

BUCKET_NAME = "dados-marqueFacil"


def conectar_minio():
    return client