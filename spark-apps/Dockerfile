FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER spark
