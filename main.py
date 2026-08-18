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

url = "https://cisbaf.hygiahub.com.br/agendamento_procedimento"

def extract(url):
    resultado = []

    LENGTH = 500

    params_base = {
      "status": "VALIDADO",
      "dt_inicial": "20/01/2026",
      "dt_final": "19/02/2026",
      "draw": 10,
      "length": LENGTH
    }

    response = session.get(
      url,
      params={
        **params_base,
        "start": 0
      }
    )

    response.raise_for_status()

    data = response.json()

    total_registros = data["total"]

    for start in range(0, total_registros, LENGTH):
      response = session.get(
        url,
        params={
          **params_base,
          "start": start
        }
      )

      response.raise_for_status()

      data = response.json()

      registros = data["data"]

      resultado.extend(registros)

      time.sleep(0.2)

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
    