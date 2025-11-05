# Spark Jars

This directory contains the JAR files required for Spark to interact with various services.

- `iceberg-spark-runtime-3.x_2.12-x.x.x.jar`: Apache Iceberg runtime for Spark. Used for Iceberg table format support.
- `spark-sql-kafka-0-10_2.12-3.x.x.jar`: Spark-Kafka connector. Enables Spark Structured Streaming to read from and write to Kafka.
- `hadoop-aws-x.x.x.jar`: Hadoop AWS module. Provides S3A filesystem client for connecting to S3-compatible storage like MinIO.
- `aws-java-sdk-bundle-x.x.x.jar`: AWS Java SDK bundle. A dependency for `hadoop-aws`.
