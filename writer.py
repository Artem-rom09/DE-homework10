from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("CassandraStreamWriter") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()

    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("domain", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("page_title", StringType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "processed") \
        .load()

    cassandra_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                     .select("data.*")

    # Запис в Cassandra
    query = cassandra_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia") \
        .option("table", "page_creations") \
        .option("checkpointLocation", "/tmp/checkpoints/writer") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
