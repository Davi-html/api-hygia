FROM apache/spark:4.0.1-python3

USER root

RUN pip3 install --no-cache-dir \
    "pandas<3" \
    pyarrow \
    requests \
    python-dotenv \
    minio

# Hadoop S3A
RUN curl -fL \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar

# AWS SDK v2
RUN curl -fL \
    -o /opt/spark/jars/bundle-2.29.52.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

# Disponibiliza o PySpark para o Python
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

USER spark

WORKDIR /opt/app