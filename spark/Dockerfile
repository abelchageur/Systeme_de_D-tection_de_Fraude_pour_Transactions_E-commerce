FROM bitnami/spark:latest

# Install Jupyter and dependencies
RUN pip install jupyter pandas py4j 

# Fix missing apt directories and install supervisor
USER root
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y --no-install-recommends supervisor && \
    rm -rf /var/lib/apt/lists/*

# Copy configuration files
WORKDIR /opt/spark/work-dir
COPY core-site.xml /opt/spark/conf/core-site.xml
COPY hdfs-site.xml /opt/spark/conf/hdfs-site.xml
RUN pip install matplotlib
RUN pip install seaborn
# Expose ports for Spark and Jupyter
EXPOSE 7077 8080 8888

# Configure supervisord
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Start supervisord
CMD ["/usr/bin/supervisord"]