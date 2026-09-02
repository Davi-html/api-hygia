# Airflow com Spark para desenvolvimento

FROM apache/airflow:3.3.1

USER root

# Instalar dependências e Java
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        wget \
        netcat-openbsd \
        build-essential \
        iputils-ping \
        telnet \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instalar Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN cd /tmp \
    && wget --no-verbose \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xvzf \
        "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
        -C /opt/ \
    && mv \
        "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" \
        "${SPARK_HOME}" \
    && rm \
        "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Configurar PATH
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Configurar Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# IMPORTANTE:
# O pip do Airflow deve ser executado como airflow
USER airflow

# Instalar provider Spark e PySpark
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    "pyspark==3.5.0" \
    papermill \
    pandas \
    requests \
    jupyter \
    nbconvert

# Verificar instalação
RUN spark-submit --version 2>&1 | head -3 || echo "Spark instalado"