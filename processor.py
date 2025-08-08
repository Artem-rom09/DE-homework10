from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("WikiStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    schema = StructType([
        StructField("meta", StructType([
            StructField("domain", StringType(), True)
        ]), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_is_bot", BooleanType(), True),
        StructField("page_title", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "input") \
        .load()

    processed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                     .select("data.*") \
                     .select(col("meta.domain").alias("domain"), "user_id", "user_is_bot", "page_title", col("timestamp").alias("created_at"))

    # Фільтрація даних
    filtered_df = processed_df.filter(
        (col("domain").isin(["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"])) &
        (col("user_is_bot") == False)
    )

    # Запис у топік 'processed'
    query = filtered_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "processed") \
        .option("checkpointLocation", "/tmp/checkpoints/processor") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
