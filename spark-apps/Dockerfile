
FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    zlib1g-dev \
    libssl-dev \
    libffi-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    liblzma-dev \
    libncurses5-dev \
    tk-dev \
    uuid-dev \
    && rm -rf /var/lib/apt/lists/*

# Instala Python 3.13
RUN cd /tmp \
    && wget --no-verbose https://www.python.org/ftp/python/3.13.0/Python-3.13.0.tgz \
    && tar -xzf Python-3.13.0.tgz \
    && cd Python-3.13.0 \
    && ./configure --enable-optimizations --prefix=/usr/local \
    && make -j$(nproc) \
    && make altinstall \
    && cd / \
    && rm -rf /tmp/Python-3.13.0*

# Garante os comandos do Python
RUN ln -sf /usr/local/bin/python3.13 /usr/local/bin/python3 \
    && ln -sf /usr/local/bin/python3.13 /usr/bin/python3.13

# Instala pip + Py4J compatível com Spark 3.5
RUN python3.13 -m ensurepip --upgrade \
    && python3.13 -m pip install --no-cache-dir py4j==0.10.9.7

# Configura Python utilizado pelo Spark
ENV PYSPARK_PYTHON=/usr/local/bin/python3.13
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.13

# Permite que o Python encontre o PySpark que já vem com o Spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark

