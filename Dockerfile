FROM bitnami/spark:latest

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get install -y wget && \
    pip3 install pyspark psycopg2-binary

# Define environment variables for Spark (optional)
#ENV SPARK_HOME=/opt/bitnami/spark

# Download the Spark-Kafka connector
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -P /opt/bitnami/spark/jars \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar

#USER 1001  # Replace with the appropriate user if different
