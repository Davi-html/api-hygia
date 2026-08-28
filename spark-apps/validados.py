
#!/usr/bin/env python3

import os
import sys
import requests
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

def create_spark_session(app_name="ETL_Job_Example"):

    spark = (
        SparkSession.builder
        .appName("ETL-MinIO")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])   
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def extract():
    session = requests.Session()
    response  = session.get('https://cisbaf.hygiahub.com.br/agendamento_procedimento', cookies={"PHPSESSID": os.getenv("PHPSESSID")})
    response.raise_for_status()

    return response.json()


def main():
    dados = extract()
    print(f"Registros extraídos: {len(dados)}")

    spark = create_spark_session()

    # ajusta conforme a estrutura real do JSON retornado
    df = spark.createDataFrame(dados)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3a://validado/processed_data_{timestamp}"

    df.write.mode("overwrite").parquet(output_path)
    print(f"Dados salvos em: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
