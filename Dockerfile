
# Aproveita a distribuição Spark já existente
FROM apache/spark:3.5.0 AS spark

FROM apache/airflow:3.3.1-python3.11

USER root

# Dependências necessárias
RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Copia o Spark da imagem oficial
COPY --from=spark /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Configuração Python do Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER airflow

# Instala somente as dependências do Airflow e da aplicação
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.11.txt" \
    apache-airflow-providers-apache-spark \
    papermill \
    pandas \
    requests \
    jupyter \
    nbconvert \
    python-dotenv

# Verifica o Spark
RUN spark-submit --version 2>&1 | head -3
