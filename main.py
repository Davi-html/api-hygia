import os
from dotenv import load_dotenv
import json
import requests
import math
import time
import pandas as pd
from config.minio import conectar_minio, BUCKET_NAME


load_dotenv()

session = requests.Session()
session.cookies.update({"PHPSESSID": os.getenv("PHPSESSID")})

competencia = "dt_inicial=20/01/2026&dt_final=19/02/2026"

url = "https://cisbaf.hygiahub.com.br/agendamento_procedimento?status=VALIDADO&{competencia}".format(competencia=competencia)

def extract(url):
  resultado = []
  
  response = session.get(url)
  response.raise_for_status()
 
  data = response.json()

  totalPagina = math.ceil(data["total"] / 500)

  for pagina in range(1, totalPagina + 1):
    data = session.get(url, params={"page": pagina, "limit": 500})
    data = data.json()
    resultado.extend(data["data"])
    time.sleep(1)


  return resultado


def load():
  try:
    data = extract(url)
    
    df = pd.DataFrame(data)

    from io import BytesIO
    parquet_buffer = BytesIO()

    df.to_parquet(
      parquet_buffer,
      engine="pyarrow",
      index=False
    )

    parquet_buffer.seek(0)
    parquet_size = parquet_buffer.getbuffer().nbytes

    client = conectar_minio()

    if not client.bucket_exists(BUCKET_NAME):
      client.make_bucket(BUCKET_NAME)


    client.put_object(
      bucket_name=BUCKET_NAME,
      object_name="teste.parquet",
      data=parquet_buffer,
      length=parquet_size,
      content_type="application/octet-stream"
    )

  except Exception as e:
    print(f"Erro ao carregar os dados: {e}")


load()

def transform(data):
  return data
    