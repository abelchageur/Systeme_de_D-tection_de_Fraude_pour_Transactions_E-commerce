# streaming/Dockerfile
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        ca-certificates \
        procps \
        net-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /opt/spark/conf

# Set Java environment variables
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin
ENV PYSPARK_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON python3

# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .
RUN pip install kafka-python cassandra-driver
# Command to run the application
CMD ["python", "fraud_consumer.py"]