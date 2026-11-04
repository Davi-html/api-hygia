FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget build-essential zlib1g-dev libssl-dev libffi-dev libbz2-dev \
    libreadline-dev libsqlite3-dev liblzma-dev libncurses5-dev tk-dev uuid-dev \
    && rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.11.9
RUN cd /tmp \
    && wget --no-verbose https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
    && tar -xzf Python-${PYTHON_VERSION}.tgz \
    && cd Python-${PYTHON_VERSION} \
    && ./configure --prefix=/usr/local \
    && make -j$(nproc) \
    && make altinstall \
    && cd / && rm -rf /tmp/Python-*

RUN ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3 \
    && python3 -m ensurepip --upgrade \
    && python3 -m pip install --no-cache-dir py4j==0.10.9.7

ENV PYSPARK_PYTHON=/usr/local/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark