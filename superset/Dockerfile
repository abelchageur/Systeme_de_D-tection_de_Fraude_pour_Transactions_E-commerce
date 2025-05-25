FROM apache/superset:latest

# Install system dependencies for sasl and other packages
USER root
RUN apt-get update && apt-get install -y \
    libsasl2-dev \
    build-essential \
    g++ \
    gcc \
    libkrb5-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for Superset
RUN pip install --no-cache-dir \
    Pillow==10.3.0 \
    pyhive[hive]==0.7.0 \
    thrift==0.16.0