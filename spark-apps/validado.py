import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

hoje = datetime.today()

data_inicial = hoje - relativedelta(months=1)
data_inicial = data_inicial.replace(day=20)

data_final = hoje.replace(day=19)

data_inicial = data_inicial.strftime("%d/%m/%Y")
data_final = data_final.strftime("%d/%m/%Y")

data_inicial = "20/07/2026"
data_final = "19/08/2026"

print(f"Data inicial: {data_inicial}")
print(f"Data final:   {data_final}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
 StructType, StructField, StringType, IntegerType 
)

spark = (
 SparkSession
 .builder
 .appName("Agendamentos")
 .config("spark.sql.shuffle.partitions", "4")
 .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
 .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
 .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
 .config("spark.hadoop.fs.s3a.path.style.access", "true")
 .getOrCreate()
)

import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import (
 StructType,
 StructField,
 StringType
)

def get_data():
 session = requests.Session()
 base_url = "https://cisbaf.hygiahub.com.br/agendamento_procedimento"

 all_data = []
 start = 0
 length = 500

 while True:
  response = session.get(
   base_url,
   params={
    "status": "TODOS",
    "dt_inicial": data_inicial,
    "dt_final": data_final,
    "start": start,
    "length": length,
    "draw": 1
   },
   cookies={"PHPSESSID": os.getenv("PHPSESSID")}
  )
  
  response.raise_for_status()
  resultado = response.json()
  
  page_data = resultado.get("data", [])
  if not page_data:
   break
  
  all_data.extend(page_data)
  start += length

 return all_data


def load(data):

 # Remove anexos
 for registro in data:
  registro.pop("anexos", None)

 # Descobre todas as colunas
 colunas = set()

 for registro in data:
  colunas.update(registro.keys())

 colunas = sorted(colunas)

 # Converte todos os valores para string
 dados = []

 for registro in data:
  linha = []

  for coluna in colunas:
   valor = registro.get(coluna)

   if valor is None:
    linha.append(None)
   else:
    linha.append(str(valor))

  dados.append(tuple(linha))

 schema = StructType([
  StructField(coluna, StringType(), True)
  for coluna in colunas
 ])

 # Cria DataFrame
 df = spark.createDataFrame(
  dados,
  schema=schema
 )

 data_inicial_path = data_inicial.replace("/", "-")
 data_final_path = data_final.replace("/", "-")
 
 output_path = (
  f"s3a://agendamentos/raw/"
  f"agendamentos_{data_inicial_path}_{data_final_path}"
 )

 df.write.mode("overwrite").parquet(output_path)

 print(f"Dados salvos em: {output_path}")

 return output_path

def silver(data_raw):
 df = spark.read.parquet(data_raw)

 df = (
  df
  .withColumnRenamed("fornecedor", "prestador")
  .withColumnRenamed("nome", "paciente")
 )

 df = (
  df
  .withColumn("agendamento_id", col("agendamento_id").cast("int"))
  .withColumn("celular", col("celular").cast("string"))
  .withColumn("cidade", col("cidade").cast("string"))
  .withColumn("codigo_sus", col("codigo_sus").cast("string"))
  .withColumn("contraste", col("contraste").cast("int"))
  .withColumn("data_hora", col("data_hora").cast("string"))
  .withColumn("data_nascimento", col("data_nascimento").cast("string"))
  .withColumn("dt_realizado", col("dt_realizado").cast("string"))
  .withColumn("exames_laboratoriais", col("exames_laboratoriais").cast("int"))
  .withColumn("fila_espera_id", col("fila_espera_id").cast("int"))
  .withColumn("prestador", col("prestador").cast("string"))
  .withColumn("id", col("id").cast("int"))
  .withColumn("paciente", col("paciente").cast("string"))
  .withColumn("paciente_foto", col("paciente_foto").cast("boolean"))
  .withColumn("paciente_municipio_id", col("paciente_municipio_id").cast("int"))
  .withColumn("procedimento", col("procedimento").cast("string"))
  .withColumn("profissional", col("profissional").cast("string"))
  .withColumn("sedacao", col("sedacao").cast("boolean"))
  .withColumn("situacao", col("situacao").cast("string"))
  .withColumn("solicitacao_medica", col("solicitacao_medica").cast("boolean"))
  .withColumn("ubs", col("ubs").cast("string"))
 )

 data_inicial_path = data_inicial.replace("/", "-")
 data_final_path = data_final.replace("/", "-")

 output_path = (
  f"s3a://agendamentos/silver/"
  f"agendamentos_{data_inicial_path}_{data_final_path}"
 )

 df.write.mode("overwrite").parquet(output_path)

 print(f"Dados salvos em: {output_path}")
 return output_path

def gold(data_silver):
 df = spark.read.parquet(data_silver)
 from pyspark.sql.functions import concat, lit

 data_inicial_dia_mes = data_inicial
 data_final_dia_mes = data_final

 df = df.withColumn("ano", lit(data_inicial[6:]))
 df = df.withColumn("ano2", lit(data_inicial[8:]))

 df = df.withColumn(
  "competencia",
  concat(
   lit("Competencia "),
   lit(data_inicial[:5]),
   lit(" a "),
   lit(data_final[:5]),
  )
 )

 df = df.withColumn(
  "comp",
  concat(
   lit(data_inicial[:5]),
   lit(" a "),
   lit(data_final[:5]),
  )
 )

 data_inicial_path = data_inicial.replace("/", "-")
 data_final_path = data_final.replace("/", "-")

 output_path = (
  f"s3a://agendamentos/gold/"
  f"agendamentos_{data_inicial_path}_{data_final_path}"
 )

 spark.display(df)

 df.write.mode("overwrite").parquet(output_path)

 print(f"Dados salvos em: {output_path}")
    


data = get_data()
data_raw = load(data)
data_silver = silver(data_raw)
data_gold = gold(data_silver)

