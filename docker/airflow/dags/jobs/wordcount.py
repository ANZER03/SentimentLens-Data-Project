from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("WordCountTest") \
        .getOrCreate()

    # Sample data
    data = ["hello world", "hello airflow", "hello spark"]

    # Create DataFrame
    df = spark.createDataFrame(data, "string").toDF("line")

    # Split into words and count
    counts = df.selectExpr("explode(split(line, ' ')) as word") \
               .groupBy("word").count()

    # Show results
    counts.show()

    spark.stop()
