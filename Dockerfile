FROM apache/airflow:3.3.1-python3.11

USER root

RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        openjdk-17-jre-headless \
        curl wget netcat-openbsd build-essential iputils-ping telnet \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    "pyspark==3.5.0" \
    papermill pandas requests jupyter nbconvert

RUN spark-submit --version 2>&1 | head -3