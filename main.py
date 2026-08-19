import os
from dotenv import load_dotenv
import requests
import time
from config.minio import BUCKET_NAME
from config.pyspark import spark

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


def load(data):
  try:

    for row in data:
      row.pop("anexos", None)

    df = spark.createDataFrame(data)

    (
      df.write
      .mode("overwrite")
      .parquet(f"s3a://{BUCKET_NAME}/teste.parquet")
    ) 

  except Exception as e:
    print(f"Erro ao carregar os dados: {e}")

def transform():
  df = spark.read.parquet(f"s3a://{BUCKET_NAME}/teste.parquet")

  return print(df)


extract_data = extract(url=url)
load(data=extract_data)
transform()
    