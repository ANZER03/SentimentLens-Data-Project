FROM apache/airflow:latest

# Install Spark provider
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

RUN pip install --no-cache-dir boto3

USER root

# Install OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow