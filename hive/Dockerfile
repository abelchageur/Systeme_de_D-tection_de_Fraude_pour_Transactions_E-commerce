FROM bde2020/hive:2.3.2-postgresql-metastore
USER root
# Set working directory
WORKDIR /home

# Update sources.list to use Debian archive for Jessie
RUN echo "deb http://archive.debian.org/debian jessie main" > /etc/apt/sources.list 
RUN echo "deb http://archive.debian.org/debian-security jessie/updates main" >> /etc/apt/sources.list  
RUN echo "Acquire::Check-Valid-Until false;" > /etc/apt/apt.conf.d/99no-check-valid-until

# Install basic utilities
RUN apt-get update
RUN apt-get install -y --force-yes wget
RUN wget https://downloads.apache.org/tez/0.9.2/apache-tez-0.9.2-bin.tar.gz
RUN tar -xzf apache-tez-0.9.2-bin.tar.gz
RUN ls -l apache-tez-0.9.2-bin
RUN mv apache-tez-0.9.2-bin /opt/tez
RUN export TEZ_HOME=/opt/tez
RUN export HADOOP_CLASSPATH=${TEZ_HOME}/*:${TEZ_HOME}/lib/*
RUN export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/tez/*:/opt/tez/lib/*