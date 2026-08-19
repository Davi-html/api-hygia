from pyspark.sql import SparkSession
import os

spark = (
 SparkSession
 .builder
 .appName("minio-app")
 .config("fs.s3a.endpoint", "http://localhost:9222")
 .config("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
 .config("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
 .config("fs.s3a.path.style.access", "true")

 .getOrCreate
)


