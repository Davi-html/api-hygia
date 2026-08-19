from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("minio-app").getOrCreate()

spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9222")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")

