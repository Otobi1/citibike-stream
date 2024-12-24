#
#FROM bitnami/spark:3.5.3
#
## Switch to root to install additional packages
#USER root

## Install wget and unzip
#RUN apt-get update && \
#    apt-get install -y wget unzip && \
#    rm -rf /var/lib/apt/lists/*
#
## Create the jars directory
#RUN mkdir -p /opt/bitnami/spark/jars
#
## Download the Spark-Kafka connectors
#RUN wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
#    wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.3/spark-streaming-kafka-0-10_2.12-3.5.3.jar
#
## Download the Kafka client library with the correct version
#RUN wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
#
## Download the Scala library
#RUN wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.15/scala-library-2.12.15.jar
#
## Download additional dependencies
#RUN wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar && \
#    wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.30/slf4j-log4j12-1.7.30.jar && \
#    wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.13.3/jackson-databind-2.13.3.jar && \
#    wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.13.3/jackson-core-2.13.3.jar && \
#    wget -P /opt/bitnami/spark/jars \
#    https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.13.3/jackson-annotations-2.13.3.jar
#
## Install Python dependencies
#RUN pip install pyspark psycopg2-binary
#
## Clean up to reduce image size
#RUN rm -rf /tmp/*
#
## Switch back to the non-root user if required by the base image
##USER 1001  # Replace with the appropriate user ID if different
