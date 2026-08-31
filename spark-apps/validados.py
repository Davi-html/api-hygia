#!/usr/bin/env python3

import os
import dotenv
import requests
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

dotenv.load_dotenv()

SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("agendamento_id", IntegerType(), True),
    StructField("codigo_sus", StringType(), True),
    StructField("procedimento", StringType(), True),
    StructField("data_hora", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("celular", StringType(), True),
    StructField("paciente_municipio_id", IntegerType(), True),
    StructField("situacao", StringType(), True),
    StructField("fornecedor", StringType(), True),
    StructField("ubs", StringType(), True),
    StructField("profissional", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("data_nascimento", StringType(), True),
    StructField("exames_laboratoriais", IntegerType(), True),
    StructField("contraste", IntegerType(), True),
    StructField("fila_espera_id", IntegerType(), True),
    StructField("sedacao", IntegerType(), True),
    StructField("dt_realizado", StringType(), True),
    StructField("solicitacao_medica", IntegerType(), True),
    StructField("paciente_foto", IntegerType(), True),
    StructField("anexo_flag", IntegerType(), True),
    StructField("anexo_qtd_documentos", IntegerType(), True),
])


def create_spark_session(app_name="ETL_Job_Example"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract():
    session = requests.Session()
    response = session.get(
        'https://cisbaf.hygiahub.com.br/agendamento_procedimento?status=VALIDADO&dt_inicial=20/07/2026&dt_final=19/08/2026',
        cookies={"PHPSESSID": "pnbue0a3i66qa8lmbk3r5nsqni"}
    )
    response.raise_for_status()
    resultado = response.json()

    return resultado["data"]


def load(data):
    if not data:
        print("Nenhum registro retornado pela API. Encerrando sem escrever no MinIO.")
        return None

    for registro in data:
        registro.pop("anexos", None)

    registros_limpos = [
        {campo.name: registro.get(campo.name) for campo in SCHEMA.fields}
        for registro in data
    ]

    spark = create_spark_session()
    df = spark.createDataFrame(registros_limpos, schema=SCHEMA)
    df.printSchema()

    timestamp = datetime.now().strftime("%Y%m%d_%H")
    output_path = f"s3a://raw/processed_data_{timestamp}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Dados salvos em: {output_path}")

    spark.stop()
    return output_path


def transform(raw_path):
    if not raw_path:
        print("Sem path de origem, pulando transform().")
        return

    spark = create_spark_session()
    df = spark.read.parquet(raw_path)

    timestamp = datetime.now().strftime("%Y%m%d_%H")
    output_path = f"s3a://silver/processed_data_{timestamp}"


    df.write.mode("overwrite").parquet(output_path)
    print(f"Dados salvos em: {output_path}")

    spark.stop()


def main():
    data = extract()
    raw_path = load(data)
    transform(raw_path)


if __name__ == "__main__":
    main()