import os
from dotenv import load_dotenv
import requests
import math
import time

load_dotenv()

session = requests.Session()
session.cookies.update({"PHPSESSID": os.getenv("PHPSESSID")})

competencia = "dt_inicial=20/01/2026&dt_final=19/02/2026",


def extract(url):
  resultado = []
  
  data = session.get(url)
  data = data.json()

  totalPagina = math.ceil(data["total"] / 500)

  for pagina in range(1, totalPagina + 1):
    data = session.get(url, params={"page": pagina, "limit": 500})
    data = data.json()
    resultado.extend(data["data"])
    time.sleep(1)


  return (resultado)


def load():
  return

def transform(data):
  return data
    
extract("https://cisbaf.hygiahub.com.br/agendamento_procedimento?status=VALIDADO&{competencia}".format(competencia=competencia))