from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, from_unixtime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import spark_factory

def create_gold_layer(spark: SparkSession):
    """
    Creates the gold layer by building a star schema with fact and dimension tables.
    """
    # Read data from the silver layer
    movies_df = spark.read.table("iceberg.silver_layer.movies")
    users_df = spark.read.table("iceberg.silver_layer.users")
    ratings_df = spark.read.table("iceberg.silver_layer.rating")

    # Create dim_movies
    dim_movies = movies_df.select("movie_id", "title", "genres")
    dim_movies.write.mode("overwrite").saveAsTable("iceberg.gold_layer.dim_movies")

    # Create dim_users
    dim_users = users_df.select("user_id", "gender", "age", "occupation", "zip_code")
    dim_users.write.mode("overwrite").saveAsTable("iceberg.gold_layer.dim_users")

    # Create dim_time
    time_df = ratings_df.select(col("timestamp")).distinct()
    dim_time = time_df.withColumn("time_id", col("timestamp")) \
        .withColumn("datetime", from_unixtime(col("timestamp")))
        
    dim_time = dim_time.withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime"))) \
        .withColumn("day_of_week", dayofweek(col("datetime"))) \
        .select("time_id", "timestamp", "year", "month", "day", "day_of_week")
        
    dim_time.write.mode("overwrite").saveAsTable("iceberg.gold_layer.dim_time")

    # Create fact_ratings
    fact_ratings = ratings_df.alias("r") \
        .join(dim_movies, "movie_id", "inner") \
        .join(dim_users, "user_id", "inner") \
        .join(dim_time, col("r.timestamp") == dim_time.time_id, "inner") \
        .select(
            col("r.user_id"),
            col("r.movie_id"),
            col("r.rating"),
            col("r.timestamp")
        )
    fact_ratings.write.mode("overwrite").saveAsTable("iceberg.gold_layer.fact_ratings")

if __name__ == "__main__":
    spark = spark_factory.create_spark_session("create_gold_layer")
    create_gold_layer(spark)
    spark.stop()
