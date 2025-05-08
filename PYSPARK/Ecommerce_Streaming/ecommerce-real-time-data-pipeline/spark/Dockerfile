# Dockerfile for Spark
FROM bitnami/spark:3.5.0

# Install PostgreSQL JDBC driver
USER root
RUN curl -o /opt/bitnami/spark/jars/postgresql-jdbc.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY spark_streaming_to_postgres.py /opt/bitnami/spark/app/

# Run Spark job
CMD ["spark-submit", "--master", "local[*]", "--jars", "/opt/bitnami/spark/jars/postgresql-jdbc.jar", "/opt/bitnami/spark/app/spark_streaming_to_postgres.py"]