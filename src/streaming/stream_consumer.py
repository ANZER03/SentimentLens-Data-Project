import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils.spark_factory import create_spark_session

from pyspark.sql.functions import col, split

def start_rating_streaming_job():
    """
    Starts the Spark streaming job to process ratings from Kafka and store them in Iceberg.
    """
    spark = create_spark_session(app_name="RatingStreamingConsumer")

    # Create database and table if they don't exist
    # spark.sql("CREATE DATABASE IF NOT EXISTS silver_layer")
    # spark.sql("""
    #     CREATE TABLE IF NOT EXISTS silver_layer.rating (
    #         user_id INT,
    #         movie_id INT,
    #         rating INT,
    #         `timestamp` LONG
    #     ) USING iceberg
    # """)

    # Read from Kafka topic
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "rating-topic")
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "rating-consumer-group")
        .load()
    )

    # The value from Kafka is a binary, cast it to string
    ratings_df = kafka_df.select(col("value").cast("string"))

    # Split the 'value' column into multiple columns
    split_col = split(ratings_df['value'], '::')
    processed_df = ratings_df.withColumn('user_id', split_col.getItem(0).cast("integer")) \
                               .withColumn('movie_id', split_col.getItem(1).cast("integer")) \
                               .withColumn('rating', split_col.getItem(2).cast("integer")) \
                               .withColumn('timestamp', split_col.getItem(3).cast("long")) \
                               .select("user_id", "movie_id", "rating", "timestamp")

    # Write to Iceberg table
    query = (
        processed_df.writeStream
        .outputMode("append")
        .format("iceberg")
        # .option("checkpointLocation", "/tmp/spark-checkpoints/rating-streaming-consumer")
        .toTable("silver_layer.rating")
    )

    query.awaitTermination()

if __name__ == "__main__":
    start_rating_streaming_job()
